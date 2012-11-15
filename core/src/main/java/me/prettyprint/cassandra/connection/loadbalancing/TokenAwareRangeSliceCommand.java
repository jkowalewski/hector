package me.prettyprint.cassandra.connection.loadbalancing;

import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.connection.HClientPool;
import me.prettyprint.cassandra.connection.OperationExecutor;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.Operation;
import me.prettyprint.cassandra.service.OperationFactory.RangeSliceOperation;
import me.prettyprint.hector.api.exceptions.HUnavailableException;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.KeyRange;

import com.google.common.collect.Iterables;

public class TokenAwareRangeSliceCommand extends TokenAwareOperationCommand {

  public TokenAwareRangeSliceCommand(Map<String, List<TokenRangeAndHosts>> ksToTokenRangeAndHostMap,
      IPartitioner<?> partitioner, Map<CassandraHost,HClientPool> hostToClientPoolMapping) {
    super(ksToTokenRangeAndHostMap, partitioner, hostToClientPoolMapping);
  }

  @Override
  public void execute(OperationExecutor executor, Operation<?> op) {
    HClientPool initialPool = getPool(op); 
    operationExecutor.executeOperation(op, initialPool); 
  }

  @Override
  public HClientPool getPool(Operation<?> op) {
    HClientPool selectedPool = null; 
    List<TokenRangeAndHosts> tokenRangeAndHosts = ksToTokenRangeAndHostMap.get(op.keyspaceName); 
    
    KeyRange keyRange = ((RangeSliceOperation)op).getKeyRange();
    
    //TODO: should we change this into multiple pools if the start key / end key span a large range? That will affect the contract with the caller. 
    Token<?> token = partitioner.getToken(keyRange.start_key);
    
    for(TokenRangeAndHosts tr : tokenRangeAndHosts) { 
      if(tr.tokenRange.contains(token)) { 
        List<CassandraHost> hosts = tr.hosts;  
        
        //TODO: to make this work, both operations below need to be atomic (i think) - my assumption here is that the contention is the same as with RR LBP. 
        synchronized(this) { 
          CassandraHost selectedHost = Iterables.get(hosts, getAndIncrement(tr, hosts.size())); 
          selectedPool = hostToClientPoolMapping.get(selectedHost);
        }
        
        if(selectedPool == null) { 
          throw new HUnavailableException("Unable to get an alive host for the desired token."); 
        }        
      }
    }
    
    return selectedPool; 
  }
  
  private int getAndIncrement(TokenRangeAndHosts tr, int size) {
    int counterToReturn;

      if (tr.counter >= 16384) {
        tr.counter = 0; 
      }

    counterToReturn = tr.counter++;
    return counterToReturn % size;
  }

}
