package me.prettyprint.cassandra.connection;

import me.prettyprint.cassandra.connection.client.HClient;
import me.prettyprint.cassandra.connection.factory.HClientFactory;
import me.prettyprint.cassandra.connection.factory.HClientFactoryProvider;
import me.prettyprint.cassandra.service.CassandraClientMonitor;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ExceptionsTranslator;
import me.prettyprint.cassandra.service.ExceptionsTranslatorImpl;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.cassandra.service.JmxMonitor;
import me.prettyprint.cassandra.service.Operation;
import me.prettyprint.hector.api.ClockResolution;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.exceptions.HectorTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class HConnectionManager {

  private static final Logger log = LoggerFactory.getLogger(HConnectionManager.class);

  private final ConcurrentMap<CassandraHost,HClientPool> hostPools;
  private final ConcurrentMap<CassandraHost,HClientPool> suspendedHostPools;
  private final Collection<HClientPool> hostPoolValues;
  private final String clusterName;
  private CassandraHostRetryService cassandraHostRetryService;
  private NodeAutoDiscoverService nodeAutoDiscoverService;
  private final LoadBalancingPolicy loadBalancingPolicy;
  private final CassandraHostConfigurator cassandraHostConfigurator;
  private final HClientFactory clientFactory;
  private HostTimeoutTracker hostTimeoutTracker;
  private final ClockResolution clock;
  private final OperationExecutor opExecutor; 

  final ExceptionsTranslator exceptionsTranslator;
  private final CassandraClientMonitor monitor;
  private HOpTimer timer;
  private ConnectionManagerListenersHandler listenerHandler = new ConnectionManagerListenersHandler();

  public HConnectionManager(String clusterName, CassandraHostConfigurator cassandraHostConfigurator) {

    clientFactory = HClientFactoryProvider.createFactory(cassandraHostConfigurator);

    loadBalancingPolicy = cassandraHostConfigurator.getLoadBalancingPolicy();
    opExecutor = new OperationExecutorImpl(loadBalancingPolicy, this); 
    clock = cassandraHostConfigurator.getClockResolution();
    hostPools = new ConcurrentHashMap<CassandraHost, HClientPool>();
    suspendedHostPools = new ConcurrentHashMap<CassandraHost, HClientPool>();
    this.clusterName = clusterName;
    if ( cassandraHostConfigurator.getRetryDownedHosts() ) {
      cassandraHostRetryService = new CassandraHostRetryService(this, clientFactory, cassandraHostConfigurator, listenerHandler);
    }
    for ( CassandraHost host : cassandraHostConfigurator.buildCassandraHosts()) {
      try {
        HClientPool hcp = loadBalancingPolicy.createConnection(clientFactory, host);
        hostPools.put(host,hcp);
      } catch (HectorTransportException hte) {
        log.error("Could not start connection pool for host {}", host);
        listenerHandler.fireOnHostDown(host);
        if ( cassandraHostRetryService != null ) {
          cassandraHostRetryService.add(host);
        }
      }
    }

    if ( cassandraHostConfigurator.getUseHostTimeoutTracker() ) {
      hostTimeoutTracker = new HostTimeoutTracker(this, cassandraHostConfigurator);
    }
    monitor = JmxMonitor.getInstance().getCassandraMonitor(this);
    exceptionsTranslator = new ExceptionsTranslatorImpl();
    this.cassandraHostConfigurator = cassandraHostConfigurator;
    hostPoolValues = hostPools.values();
    if ( cassandraHostConfigurator.getAutoDiscoverHosts() ) {
      nodeAutoDiscoverService = new NodeAutoDiscoverService(this, cassandraHostConfigurator);
    }

    timer = cassandraHostConfigurator.getOpTimer();
  }

  public void doAddNodes() {
    if (nodeAutoDiscoverService != null) {
      nodeAutoDiscoverService.doAddNodes();
    } else {
      log.warn("unable to add nodes, nodeAutoDiscoverService was null.  " +
              "CassandraHostConfigurator.autoDiscoverHosts is {}", cassandraHostConfigurator.getAutoDiscoverHosts());
    }
  }

  /**
   * Returns true if the host was successfully added. In any sort of failure exceptions are
   * caught and logged, returning false.
   * @param cassandraHost
   * @return
   */
  public boolean addCassandraHost(CassandraHost cassandraHost) {
    if ( !getHosts().contains(cassandraHost) ) {
      HClientPool pool = null;
      try {
        cassandraHostConfigurator.applyConfig(cassandraHost);
        pool = cassandraHostConfigurator.getLoadBalancingPolicy().createConnection(clientFactory, cassandraHost);
        hostPools.putIfAbsent(cassandraHost, pool);
        log.info("Added host {} to pool", cassandraHost.getName());
        listenerHandler.fireOnAddHost(cassandraHost, true, null, null);
        return true;
      } catch (HectorTransportException hte) {
        String errorMessage = "Transport exception host to HConnectionManager: " + cassandraHost;
        log.error(errorMessage, hte);
        listenerHandler.fireOnAddHost(cassandraHost, false, errorMessage, hte);
      } catch (Exception ex) {
        String errorMessage = "General exception host to HConnectionManager: " + cassandraHost;
        log.error(errorMessage, ex);
        listenerHandler.fireOnAddHost(cassandraHost, false, errorMessage, ex);
      }
    } else {
      String message = "Host already existed for pool " + cassandraHost.getName();
      log.info(message);
      listenerHandler.fireOnAddHost(cassandraHost, false, message, null);
    }
    return false;
  }

  /**
   * Remove the {@link CassandraHost} from the pool, bypassing retry service. This
   * would be called on a host that is known to be going away. Gracefully shuts down
   * the underlying connections via {@link HClientPool#shutdown()}. This method
   * will also:
   * <ul>
   * <li>shutdown pools in the suspended state, removing them from the underlying
   * suspended map.</li>
   * <li>remove hosts from {@link CassandraHostRetryService} if contained therein</li></ul>
   *
   * @param cassandraHost
   */
  public boolean removeCassandraHost(CassandraHost cassandraHost) {
    boolean removed = getHosts().contains(cassandraHost);
    String message;
    if ( removed ) {
    	HClientPool pool = hostPools.remove(cassandraHost);
        message = "Removed from hostPools";
      if ( pool == null ) {
        log.info("removeCassandraHost looking for host {} in suspendedHostPools", cassandraHost);
        pool = suspendedHostPools.remove(cassandraHost);
        message = "Removed from suspendedHostPools";
      }
      if ( pool != null ) {
        pool.shutdown();
      } else {
        removed = false;
        message = "Removed by another thread";
        log.info("removeCassandraHost attempt miss for CassandraHost {} May have been beaten by another thread?", cassandraHost);
      }
    } else if ( cassandraHostRetryService != null && cassandraHostRetryService.contains(cassandraHost)) {
        log.info("Host {} not in active pools, but found in retry service.", cassandraHost);
        removed = cassandraHostRetryService.remove(cassandraHost);
        message = "Removed from Downed hosts";
    } else {
        message = "Host not found";
        log.info("Remove requested on a host that was not found in active or disabled pools: {}", cassandraHost);
    }
    log.info("Remove status for CassandraHost pool {} was {}", cassandraHost, removed);
    listenerHandler.fireOnRemoveHost(cassandraHost, removed, message);
    return removed;
  }

  /**
   * Remove the {@link HClientPool} referenced by the {@link CassandraHost} from
   * the active host pools. This does not shut down the pool, only removes it as a candidate from
   * future operations.
   * @param cassandraHost
   * @return true if the operation was successful.
   */
  public boolean suspendCassandraHost(CassandraHost cassandraHost) {
    HClientPool pool = hostPools.remove(cassandraHost);
    boolean removed = pool != null;
    if ( removed ) {
      suspendedHostPools.put(cassandraHost, pool);
    }
    listenerHandler.fireOnSuspendHost(cassandraHost, removed);
    log.info("Suspend operation status was {} for CassandraHost {}", removed, cassandraHost);
    return removed;
  }

  /**
   * The opposite of suspendCassandraHost, places the pool back into selection
   * @param cassandraHost
   * @return true if this operation was successful. A no-op returning false
   * if there was no such host in the underlying suspendedHostPool map.
   */
  public boolean unsuspendCassandraHost(CassandraHost cassandraHost) {
    HClientPool pool = suspendedHostPools.remove(cassandraHost);
    boolean readded = pool != null;
    if ( readded ) {
      boolean alreadyThere = hostPools.putIfAbsent(cassandraHost, pool) != null;
      if ( alreadyThere ) {
        log.error("Unsuspend called on a pool that was already active for CassandraHost {}", cassandraHost);
        pool.shutdown();
      }
    }
    listenerHandler.fireOnUnSuspendHost(cassandraHost, readded);
    log.info("UN-Suspend operation status was {} for CassandraHost {}", readded, cassandraHost);
    return readded;
  }

  /**
   * Returns a Set of {@link CassandraHost} which are in the suspended status
   * @return
   */
  public Set<CassandraHost> getSuspendedCassandraHosts() {
    return suspendedHostPools.keySet();
  }

  public Set<CassandraHost> getHosts() {
    return Collections.unmodifiableSet(hostPools.keySet());
  }

  public List<String> getStatusPerPool() {
    List<String> stats = new ArrayList<String>();
    for (HClientPool clientPool : hostPools.values()) {
        stats.add(clientPool.getStatusAsString());
    }
    return stats;
  }


  public void operateWithFailover(Operation<?> op) throws HectorException {
    opExecutor.executeWithFailover(op); 
  }

  void closeClient(HClient client) {
    if ( client != null ) {
      client.close();
      client.clearAuthentication();
    }
  }

  public HOpTimer getTimer() {
    return timer;
  }

  public void setTimer(HOpTimer timer) {
    this.timer = timer;
  }

  /**
   * adds a listener in order to execute some operations when a status of a host changes
   * @param listenerName - a name identifier for the listener
   * @param listener - a {@link me.prettyprint.cassandra.connection.ConnectionManagerListener} listener
   */
  public void addListener(String listenerName, ConnectionManagerListener listener){
      listenerHandler.put(listenerName, listener);
  }

  /**
   * removes a listener from the connectionManager
   * @param listenerName - the name of the listener to remove
   */
  public void removeListener(String listenerName){
      listenerHandler.remove(listenerName);
  }

  /**
   * removes all listeners from the connectionManager
   */
  public void removeAllListeners(){
      listenerHandler.clear();
  }

  /**
   * Use the HostTimeoutCheck and initiate a suspend if and only if
   * we are configured for such AND there is more than one operating host pool
   * @param cassandraHost
   */
  void doTimeoutCheck(CassandraHost cassandraHost) {
    if ( hostTimeoutTracker != null && hostPools.size() > 1) {
      if (hostTimeoutTracker.checkTimeout(cassandraHost) ) {
        suspendCassandraHost(cassandraHost);
      }
    }
  }

  /**
  * Sleeps for the specified time as determined by sleepBetweenHostsMilli.
  * In many cases failing over to other hosts is done b/c the cluster is too busy, so the sleep b/w
  * hosts may help reduce load on the cluster.
  */
  void sleepBetweenHostSkips(FailoverPolicy<?> failoverPolicy) {
    if (failoverPolicy.sleepBetweenHostsMilli > 0) {
      if ( log.isDebugEnabled() ) {
        log.debug("Will sleep for {} millisec", failoverPolicy.sleepBetweenHostsMilli);
      }
      try {
        Thread.sleep(failoverPolicy.sleepBetweenHostsMilli);
      } catch (InterruptedException e) {
        log.warn("Sleep between hosts interrupted", e);
      }
    }
  }

  private HClientPool getClientFromLBPolicy(Set<CassandraHost> excludeHosts) {
    if ( hostPools.isEmpty() ) {
      throw new HectorException("All host pools marked down. Retry burden pushed out to client.");
    }
    return loadBalancingPolicy.getPool(hostPoolValues, excludeHosts, null);
  }

  void releaseClient(HClient client) {
    if ( client == null ) return;
    HClientPool pool = hostPools.get(client.getCassandraHost());
    if ( pool == null ) {
      pool = suspendedHostPools.get(client.getCassandraHost());
    }
    if ( pool != null ) {
      pool.releaseClient(client);
    } else {
      log.info("Client {} released to inactive or dead pool. Closing.", client);
      client.close();
    }
  }

  HClient borrowClient() {
    HClientPool pool = getClientFromLBPolicy(null);
    if ( pool != null ) {
      return pool.borrowClient();
    }
    return null;
  }

  void markHostAsDown(CassandraHost cassandraHost) {
    log.error("MARK HOST AS DOWN TRIGGERED for host {}", cassandraHost.getName());
    listenerHandler.fireOnHostDown(cassandraHost);
    HClientPool pool = hostPools.remove(cassandraHost);
    if ( pool != null ) {
      log.error("Pool state on shutdown: {}", pool.getStatusAsString());
      pool.shutdown();
      if ( cassandraHostRetryService != null )
        cassandraHostRetryService.add(cassandraHost);
    }
  }

  public Set<CassandraHost> getDownedHosts() {
    return cassandraHostRetryService.getDownedHosts();
  }

  public Collection<HClientPool> getActivePools() {
    return Collections.unmodifiableCollection(hostPools.values());
  }

  public long createClock() {
    return this.clock.createClock();
  }

  public String getClusterName() {
    return clusterName;
  }

  public void shutdown() {
    log.info("Shutdown called on HConnectionManager");
    if ( cassandraHostRetryService != null )
      cassandraHostRetryService.shutdown();
    if ( nodeAutoDiscoverService != null )
      nodeAutoDiscoverService.shutdown();
    if ( hostTimeoutTracker != null )
      hostTimeoutTracker.shutdown();

    for (HClientPool pool : hostPools.values()) {
      try {
        pool.shutdown();
      } catch (IllegalArgumentException iae) {
        log.error("Out of order in HConnectionManager shutdown()?: {}", iae.getMessage());
      }
    }
  }

  public void setCassandraHostRetryDelay(int retryDelay) {
    cassandraHostRetryService.setRetryDelayInSeconds(retryDelay);
    cassandraHostRetryService.applyRetryDelay();
  }

}
