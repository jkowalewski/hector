package me.prettyprint.cassandra.connection.loadbalancing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TException;

import com.google.common.collect.Iterables;

import me.prettyprint.cassandra.connection.ConcurrentHClientPool;
import me.prettyprint.cassandra.connection.HClientPool;
import me.prettyprint.cassandra.connection.HConnectionManager;
import me.prettyprint.cassandra.connection.LoadBalancingPolicy;
import me.prettyprint.cassandra.connection.OperationExecutor;
import me.prettyprint.cassandra.connection.client.HClient;
import me.prettyprint.cassandra.connection.factory.HClientFactory;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.Operation;
import me.prettyprint.cassandra.service.OperationFactory.BatchMutateOperation;
import me.prettyprint.cassandra.service.OperationFactory.RangeSliceOperation;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;

/**
 * @author jkowalewski
 *
 */
public class TokenAwareLoadBalancingPolicy implements LoadBalancingPolicy {
  
  /**
   * 
   */
  private static final long serialVersionUID = 4995036804416677551L;
  private OperationExecutor operationExecutor; 
  private IPartitioner<Token<?>> partitioner; 
  private HConnectionManager connectionManager; 
  private Map<String,List<TokenRangeAndHosts>> keyspaceToTokenRangeAndHostMap; 
  private Map<CassandraHost,HClientPool> activeHostsAndClientPools; 
  private Map<Class<?>, TokenAwareOperationCommand> operationClassToExecutionCommandMap; 
  
  public TokenAwareLoadBalancingPolicy() {
    keyspaceToTokenRangeAndHostMap = new HashMap<String, List<TokenRangeAndHosts>>(); 
    operationClassToExecutionCommandMap = new HashMap<Class<?>, TokenAwareOperationCommand>();
  }
  
  //TOOD: consider not having this be an init method, but just a setter? 
  public void init(HConnectionManager connectionManager) { 
    this.connectionManager = connectionManager; 
    activeHostsAndClientPools = connectionManager.getActiveHostToPoolMapping(); 
  }
  

  @Override
  public void operateWithFailover(Operation<?> op) {
    //if we havent seen the keyspace before, we need to populate the internal mappings. It would
    //be pretty awesome if we could know the keyspaces ahead of time, so we could do this when the client starts up
    //instead of needing to check per request. 
    if(!keyspaceToTokenRangeAndHostMap.containsKey(op.keyspaceName)) {
      determineTokenRange(op.keyspaceName); 
    }
    
    if(!operationClassToExecutionCommandMap.containsKey(op.getClass())) { 
      //TODO: this might not even happen - i'm not sure of a case where there would be an operation that wouldnt use this policy, but i have it here for now. 
      throw new HInvalidRequestException("The operation is not allowed for this load balancing policy."); 
    }
    
    TokenAwareOperationCommand command = operationClassToExecutionCommandMap.get(op.getClass()); 
    
    command.execute(operationExecutor, op); 
  }
  
  Operation<?> decorateOpIfRequired(Operation<?> op) { 
    Operation<?> opToReturn = op; 
    
    if(op instanceof BatchMutateOperation) { 
      opToReturn = new TokenAwareBatchMutateOperation(op); 
    }
    
    return opToReturn;  
  }

  @SuppressWarnings("unchecked")
  void determineTokenRange(String keyspaceName) {
    //TODO: this is a very rough way of getting a client. Probably want to actually use the cluster describe_ring here? 
    Collection<HClientPool> activePools = connectionManager.getActivePools();
    HClientPool clientPool = Iterables.getFirst(activePools, null); 
    //yes yes, this can be null, and we arent checking if the pool is empty. 
    HClient client = clientPool.borrowClient(); 

    Cassandra.Client cassandra = client.getCassandra();
    
    try {
      
      if(partitioner == null) { 
        String partitionerName = cassandra.describe_partitioner();
        try {
          partitioner = FBUtilities.newPartitioner(partitionerName);
        } catch (ConfigurationException e) {
          //TOOD: what to do here? 
        } 
      }
      
      List<TokenRange> tokenRanges = cassandra.describe_ring(keyspaceName);
      List<TokenRangeAndHosts> tokenRangeAndHosts = new ArrayList<TokenRangeAndHosts>(); 
      
      for(TokenRange tr : tokenRanges) { 
        Token<?> startToken = partitioner.getTokenFactory().fromString(tr.start_token);
        Token<?> endToken = partitioner.getTokenFactory().fromString(tr.end_token); 
        
        Range<Token<?>> range = new Range<Token<?>>(startToken, endToken, partitioner);

        List<CassandraHost> hosts = new ArrayList<CassandraHost>(); 
        for(EndpointDetails endpoint : tr.endpoint_details) {
          //TODO: where are the port details? 
          hosts.add(new CassandraHost(endpoint.host)); 
        }
        
        tokenRangeAndHosts.add(new TokenRangeAndHosts(range, hosts)); 
      }
      
      keyspaceToTokenRangeAndHostMap.put(keyspaceName, tokenRangeAndHosts); 
    } catch (InvalidRequestException e) {
      //TODO: what to do here? 
    } catch (TException e) {
      //TODO: what to do here?
    }  
  }
  

  @Override
  public HClientPool getPool(Collection<HClientPool> pools,
      Set<CassandraHost> excludeHosts, Operation<?> operation) {
    TokenAwareOperationCommand command = operationClassToExecutionCommandMap.get(operation.getClass()); 
    
    //TODO: making an assumption here that exclude hosts may not be necessary as the underlying map of host pools should be updated if a host is marked down, right? 
    return command.getPool(operation); 
  }


  @Override
  public HClientPool createConnection(HClientFactory clientFactory,
      CassandraHost host) {
    return new ConcurrentHClientPool(clientFactory, host); 
  }


  @Override
  public void setOperationExecutor(OperationExecutor operationExecutor) {
    TokenAwareBatchMutateCommand batchMutateCommand = new TokenAwareBatchMutateCommand(keyspaceToTokenRangeAndHostMap, partitioner, activeHostsAndClientPools); 
    operationClassToExecutionCommandMap.put(TokenAwareBatchMutateOperation.class,batchMutateCommand); 
    operationClassToExecutionCommandMap.put(BatchMutateOperation.class, batchMutateCommand); 
    operationClassToExecutionCommandMap.put(RangeSliceOperation.class, new TokenAwareRangeSliceCommand(keyspaceToTokenRangeAndHostMap, partitioner, activeHostsAndClientPools));
  }
  
}
