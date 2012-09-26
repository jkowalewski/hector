package me.prettyprint.cassandra.connection;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.BigIntegerToken;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TException;

import com.google.common.collect.Iterables;

import me.prettyprint.cassandra.connection.client.HClient;
import me.prettyprint.cassandra.connection.factory.HClientFactory;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.Operation;
import me.prettyprint.cassandra.service.OperationFactory.BatchMutateOperation;
import me.prettyprint.cassandra.service.OperationFactory.RangeSliceOperation;
import me.prettyprint.hector.api.exceptions.HUnavailableException;

/**
 * @author jkowalewski
 *
 */
public class TokenAwareLoadBalancingPolicy implements LoadBalancingPolicy {

  private class TokenRangeAndHosts
  {
    public TokenRangeAndHosts(Range<Token<?>> tokenRange, List<CassandraHost> hosts) { 
      this.tokenRange = tokenRange; 
      this.hosts = hosts; 
    }
    
    public Range<Token<?>> tokenRange; 
    public List<CassandraHost> hosts; 
  }
  
  /**
   * 
   */
  private static final long serialVersionUID = 4995036804416677551L;
  private OperationExecutor operationExecutor; 
  private IPartitioner<Token<?>> partitioner; 
  private HConnectionManager connectionManager; 
  private Map<String,List<TokenRangeAndHosts>> keyspaceToTokenRangeAndHostMap; 
  private Map<CassandraHost,HClientPool> hostToClientPoolMapping; 
  
  public TokenAwareLoadBalancingPolicy() {
    keyspaceToTokenRangeAndHostMap = new HashMap<String, List<TokenRangeAndHosts>>(); 
  }
  
  public void init(HConnectionManager connectionManager) { 
    this.connectionManager = connectionManager; 
    hostToClientPoolMapping = connectionManager.getActiveHostToPoolMapping(); 
  }
  

  @Override
  public void operateWithFailover(Operation<?> op) {
    //if we havent seen the keyspace before, we need to populate the internal mappings. It would
    //be pretty awesome if we could know the keyspaces ahead of time, so we could do this when the client starts up
    //instead of needing to check per request. 
    if(!keyspaceToTokenRangeAndHostMap.containsKey(op.keyspaceName)) {
      determineTokenRange(op.keyspaceName); 
    }
    
    //get the initial pool that the operation executor will use.
    HClientPool pool = null; 
    
    if(pool instanceof RangeSliceOperation) {
      //TODO: for range slices, we can do multiple gets for however many replicas fall across the token range for the keys, 
      //or we can just use the first key range and ignore the rest. 
      pool = getPoolFor((RangeSliceOperation)op);
    }
    
    //TODO: pass the pool onto the operation executor to actually perform the op. failover will request another pool from this policy, where we will RR and return the next replica.
    
  }

  private HClientPool getPoolFor(RangeSliceOperation op) {
    HClientPool selectedPool = null; 
    List<TokenRangeAndHosts> tokenRangeAndHosts = keyspaceToTokenRangeAndHostMap.get(op.keyspaceName); 
    
    KeyRange keyRange = ((RangeSliceOperation)op).getKeyRange();
    
    Token<?> token = partitioner.getToken(keyRange.start_key);
    
    for(TokenRangeAndHosts tr : tokenRangeAndHosts) { 
      if(tr.tokenRange.contains(token)) { 
        List<CassandraHost> hosts = tr.hosts;  
        
        //TODO: round robin getting the host here.     
        selectedPool = hostToClientPoolMapping.get(hosts.get(0));
        
        if(selectedPool == null) { 
          throw new HUnavailableException("Unable to get an alive host for the desired token."); 
        }        
      }
    }

    return selectedPool; 
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
  
  private class TokenAwareBatchMutateDecorator { 
    
  }

  @Override
  public HClientPool getPool(Collection<HClientPool> pools,
      Set<CassandraHost> excludeHosts, Operation<?> operation) {
    
    HClientPool selectedPool = null; 
    
    if(operation instanceof RangeSliceOperation) {
      selectedPool = getPoolFor((RangeSliceOperation)operation); 
    }
    else if(operation instanceof BatchMutateOperation<?>) { 
      
    }

    return selectedPool; 
  }


  @Override
  public HClientPool createConnection(HClientFactory clientFactory,
      CassandraHost host) {
    return new ConcurrentHClientPool(clientFactory, host); 
  }


  @Override
  public void setOperationExecutor(OperationExecutor operationExecutor) {
    // TODO Auto-generated method stub
    
  }
}
