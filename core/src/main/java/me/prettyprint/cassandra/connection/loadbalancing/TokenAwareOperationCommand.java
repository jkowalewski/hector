package me.prettyprint.cassandra.connection.loadbalancing;

import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.connection.HClientPool;
import me.prettyprint.cassandra.connection.OperationExecutor;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.Operation;

import org.apache.cassandra.dht.IPartitioner;

public abstract class TokenAwareOperationCommand {
  protected OperationExecutor operationExecutor;
  protected Map<String, List<TokenRangeAndHosts>> ksToTokenRangeAndHostMap;
  protected IPartitioner<?> partitioner;
  protected Map<CassandraHost, HClientPool> hostToClientPoolMapping;

  public TokenAwareOperationCommand(Map<String, List<TokenRangeAndHosts>> ksToTokenRangeAndHostMap,
      IPartitioner<?> partitioner, Map<CassandraHost, HClientPool> hostToClientPoolMapping) {
    this.partitioner = partitioner; 
    this.ksToTokenRangeAndHostMap = ksToTokenRangeAndHostMap; 
    this.hostToClientPoolMapping = hostToClientPoolMapping; 
  }

  public abstract void execute(OperationExecutor executor, Operation<?> op);
  public abstract HClientPool getPool(Operation<?> op);
}
