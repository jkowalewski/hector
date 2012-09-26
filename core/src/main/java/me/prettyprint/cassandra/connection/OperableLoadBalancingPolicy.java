package me.prettyprint.cassandra.connection;

import me.prettyprint.cassandra.service.Operation;

public interface OperableLoadBalancingPolicy {
  void operateWithFailover(Operation<?> op); 
}
