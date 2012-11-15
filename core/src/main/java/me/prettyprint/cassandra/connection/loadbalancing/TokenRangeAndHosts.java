package me.prettyprint.cassandra.connection.loadbalancing;

import java.util.List;

import me.prettyprint.cassandra.service.CassandraHost;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

/**
 * @author jkowalewski
 *
 */
public class TokenRangeAndHosts
{
  public TokenRangeAndHosts(Range<Token<?>> tokenRange, List<CassandraHost> hosts) { 
    this.tokenRange = tokenRange; 
    this.hosts = hosts; 
    this.counter = 0; 
  }
  
  public Range<Token<?>> tokenRange; 
  public List<CassandraHost> hosts;
  public int counter; 
}
