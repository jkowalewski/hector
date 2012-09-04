package me.prettyprint.cassandra.connection;

import me.prettyprint.cassandra.service.Operation;

public interface OperationExecutor {
  
  void executeWithFailover(Operation<?> op); 
}
