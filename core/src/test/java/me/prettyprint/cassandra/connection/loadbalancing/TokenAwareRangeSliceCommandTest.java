package me.prettyprint.cassandra.connection.loadbalancing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.connection.HClientPool;
import me.prettyprint.cassandra.connection.OperationExecutor;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.Operation;
import me.prettyprint.cassandra.service.OperationFactory.RangeSliceOperation;

import org.apache.cassandra.dht.BigIntegerToken;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.TokenRange;
import org.junit.Test;
import static org.mockito.Mockito.*;

public class TokenAwareRangeSliceCommandTest {
  
  @Test
  public void testGetPoolForSingleTokenRange() { 
    OperationExecutor mockExecutor = mock(OperationExecutor.class); 
    Map<String,List<TokenRangeAndHosts>> ksToTokenRangeAndHostMap = new HashMap<String, List<TokenRangeAndHosts>>();
    
    Range<Token<?>> range = new Range(null, null);  
   
    List<CassandraHost> hosts = new ArrayList<CassandraHost>(); 
    CassandraHost host1 = new CassandraHost("host1"); 
    hosts.add(host1); 
    
    TokenRangeAndHosts tr1 = new TokenRangeAndHosts(range, hosts); 
    
    List<TokenRangeAndHosts> trAndHosts = new ArrayList<TokenRangeAndHosts>(); 
    
    
    
    ksToTokenRangeAndHostMap.put("testKeyspace", new ArrayList<TokenRangeAndHosts>()); 
    IPartitioner<?> partitioner = mock(IPartitioner.class); 
    Map<CassandraHost,HClientPool> hostToClientPoolMapping = new HashMap<CassandraHost, HClientPool>(); 
    
    RangeSliceOperation mockOp = mock(RangeSliceOperation.class); 
   
    KeyRange keyRange = new KeyRange(); 
    keyRange.start_token = "0";
    keyRange.end_token= "10"; 
    
    mockOp.keyspaceName = "testKeyspace"; 
    
    when(mockOp.getKeyRange()).thenReturn(keyRange); 
    
    TokenAwareRangeSliceCommand command = new TokenAwareRangeSliceCommand(ksToTokenRangeAndHostMap, partitioner, hostToClientPoolMapping); 
    
    command.getPool(mockOp); 
  }
}
