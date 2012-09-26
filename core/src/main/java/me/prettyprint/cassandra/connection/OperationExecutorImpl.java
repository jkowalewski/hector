package me.prettyprint.cassandra.connection;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.prettyprint.cassandra.connection.client.HClient;
import me.prettyprint.cassandra.service.CassandraClientMonitor;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.ExceptionsTranslator;
import me.prettyprint.cassandra.service.ExceptionsTranslatorImpl;
import me.prettyprint.cassandra.service.JmxMonitor;
import me.prettyprint.cassandra.service.Operation;
import me.prettyprint.cassandra.service.CassandraClientMonitor.Counter;
import me.prettyprint.hector.api.exceptions.HCassandraInternalException;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.exceptions.HPoolRecoverableException;
import me.prettyprint.hector.api.exceptions.HTimedOutException;
import me.prettyprint.hector.api.exceptions.HUnavailableException;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.exceptions.HectorTransportException;

public class OperationExecutorImpl implements OperationExecutor {

  private static final Logger log = LoggerFactory.getLogger(OperationExecutorImpl.class);  
  private final HConnectionManager connectionManager; 
  private final LoadBalancingPolicy loadBalancingPolicy; 
  private final CassandraClientMonitor monitor; 
  private final ExceptionsTranslator exceptionsTranslator; 
  private final Collection<HClientPool> activeHostPools; 
  
  public OperationExecutorImpl(LoadBalancingPolicy policy, HConnectionManager manager) {
    
    this.connectionManager = manager; 
    this.loadBalancingPolicy = policy; 
    this.activeHostPools = connectionManager.getActivePools(); 
    this.monitor = JmxMonitor.getInstance().getCassandraMonitor(connectionManager); 
    this.exceptionsTranslator = new ExceptionsTranslatorImpl();
  }
  
  @Override
  public void executeOperation(Operation<?> op, HClientPool pool) {
    HOpTimer timer = connectionManager.getTimer(); 
    final Object timerToken = timer.start();
    int retries = Math.min(op.failoverPolicy.numRetries, activeHostPools.size());
    HClient client = null;
    boolean success = false;
    boolean retryable = false;
    int numberOfAttempts = 0; 
    Set<CassandraHost> excludeHosts = new HashSet<CassandraHost>(); // HLT.getExcludedHosts() (will be empty most times)
    // TODO start timer for limiting retry time spent
    while ( !success ) {
      try {
        // TODO how to 'timeout' on this op when underlying pool is exhausted
        numberOfAttempts += 1; 
        
        //If we have failed at all, need to go back to the LBP for another pool. 
        if(numberOfAttempts > 1) { 
          pool = getClientFromLBPolicy(excludeHosts, op);
        }
        client = pool.borrowClient();
        Cassandra.Client c = client.getCassandra(op.keyspaceName);
        // Keyspace can be null for some system_* api calls
        if ( op.credentials != null && !op.credentials.isEmpty() && !client.isAlreadyAuthenticated(op.credentials)) {
          c.login(new AuthenticationRequest(op.credentials));
          client.setAuthenticated(op.credentials);
        }

        op.executeAndSetResult(c, pool.getCassandraHost());
        success = true;
        timer.stop(timerToken, op.stopWatchTagName, true);
        break;

      } catch (Exception ex) {
        HectorException he = exceptionsTranslator.translate(ex);
        if ( he instanceof HUnavailableException) {
          // break out on HUnavailableException as well since we can no longer satisfy the CL
          throw he;
        } else if (he instanceof HInvalidRequestException || he instanceof HCassandraInternalException) {
          connectionManager.closeClient(client);
          throw he;
        } else if (he instanceof HectorTransportException) {
          connectionManager.closeClient(client);
          connectionManager.markHostAsDown(pool.getCassandraHost());
          excludeHosts.add(pool.getCassandraHost());
          retryable = op.failoverPolicy.shouldRetryFor(HectorTransportException.class);

          monitor.incCounter(Counter.RECOVERABLE_TRANSPORT_EXCEPTIONS);

        } else if (he instanceof HTimedOutException ) {
          // DO NOT drecrement retries, we will be keep retrying on timeouts until it comes back
          // if HLT.checkTimeout(cassandraHost): suspendHost(cassandraHost);
          connectionManager.doTimeoutCheck(pool.getCassandraHost());

          retryable = op.failoverPolicy.shouldRetryFor(HTimedOutException.class);

          monitor.incCounter(Counter.RECOVERABLE_TIMED_OUT_EXCEPTIONS);
          client.close();
          // TODO timecheck on how long we've been waiting on timeouts here
          // suggestion per user moores on hector-users
        } else if ( he instanceof HPoolRecoverableException ) {
          retryable = op.failoverPolicy.shouldRetryFor(HPoolRecoverableException.class);;
          if ( activeHostPools.size() == 1 ) {
            throw he;
          }
          monitor.incCounter(Counter.POOL_EXHAUSTED);
          excludeHosts.add(pool.getCassandraHost());
        } else {
          // something strange happened. Added here as suggested by sbridges.
          // I think this gives a sane way to future-proof against any API additions
          // that we don't add in time.
          retryable = false;
        }
        if ( retries <= 0 || retryable == false)
          throw he;

        log.warn("Could not fullfill request on this host {}", client);
        log.warn("Exception: ", he);
        monitor.incCounter(Counter.SKIP_HOST_SUCCESS);
        connectionManager.sleepBetweenHostSkips(op.failoverPolicy);
      } finally {
        --retries;
        if ( !success ) {
          monitor.incCounter(op.failCounter);
          timer.stop(timerToken, op.stopWatchTagName, false);
        }
        connectionManager.releaseClient(client);
        client = null;
      }
    }
  }
  
  private HClientPool getClientFromLBPolicy(Set<CassandraHost> excludeHosts, Operation<?> operation) {
    if (activeHostPools.isEmpty()) {
      throw new HectorException("All host pools marked down. Retry burden pushed out to client.");
    }
    
    HClientPool pool = loadBalancingPolicy.getPool(activeHostPools, excludeHosts, operation); 
    
    return pool; 
  }
}
