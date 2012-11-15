package me.prettyprint.cassandra.connection.loadbalancing;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.connection.HClientPool;
import me.prettyprint.cassandra.connection.OperationExecutor;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.Operation;
import me.prettyprint.cassandra.service.OperationFactory.BatchMutateOperation;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.Mutation;

public class TokenAwareBatchMutateCommand extends TokenAwareOperationCommand {

  public TokenAwareBatchMutateCommand(
      Map<String, List<TokenRangeAndHosts>> ksToTokenRangeAndHostMap,IPartitioner<?> partitioner, Map<CassandraHost, HClientPool> hostToClientPoolMapping) {
    super(ksToTokenRangeAndHostMap, partitioner, hostToClientPoolMapping);
    // TODO Auto-generated constructor stub
  }

  @Override
  public void execute(OperationExecutor executor, Operation<?> op) {
    
    if(!isDecorated(op)) { 
      //The operation was not broken into a batch mutate operation, so we have to build mutation map and segregate for this operation.
      BatchMutateOperation<?> batchMutateOp = (BatchMutateOperation<?>)op; 
      
      Map<ByteBuffer,Map<String,List<Mutation>>> mutationMap = batchMutateOp.getMutations().getMutationMap(); 
      List<TokenRangeAndHosts> tokenRangeAndHosts = ksToTokenRangeAndHostMap.get(op.keyspaceName); 
      
      for(ByteBuffer key : mutationMap.keySet()) { 
        Token<?> token = partitioner.getToken(key);
        
        for(TokenRangeAndHosts tr : tokenRangeAndHosts) { 
          if(tr.tokenRange.contains(token)) { 
            Map<String,List<Mutation>> mutationsForKey = mutationMap.get(key);
            
            //create new batch mutate operations for each input operation and issue them! 
            
          }
        }
      }
    }
    else { 
      //the operation was decorated - its already been broken up and is likely coming back here because of a failure. RR and grab the next available host for the mutations. 
    }
  }
  
  private boolean isDecorated(Operation<?> op) { 
    return (op instanceof TokenAwareBatchMutateOperation); 
  }

  @Override
  public HClientPool getPool(Operation<?> op) {
    // TODO Auto-generated method stub
    return null;
  }

}
