package me.prettyprint.cassandra.connection.loadbalancing;

import java.util.Map;

import org.apache.cassandra.thrift.Cassandra.Client;

import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.cassandra.service.Operation;
import me.prettyprint.cassandra.service.OperationType;

public class TokenAwareBatchMutateOperation extends Operation<Void> {
  private Operation<?> operation;

  public TokenAwareBatchMutateOperation(OperationType operationType, FailoverPolicy<?> failoverPolicy, String keyspaceName,
      Map<String, String> credentials) {
    super(operationType, failoverPolicy, keyspaceName, credentials);
  }
  
  public TokenAwareBatchMutateOperation(Operation<?> op) { 
    this(op.operationType, op.failoverPolicy, op.keyspaceName, op.credentials); 
    this.operation = op; 
  }

  @Override
  public Void execute(Client cassandra) throws Exception {
    return (Void)this.operation.execute(cassandra); 
  }
  
}
