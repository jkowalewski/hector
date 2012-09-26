package me.prettyprint.cassandra.service;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.model.thrift.ThriftConverter;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.TokenRange;

public class OperationFactory {
  
  private ConsistencyLevelPolicy consistencyLevelPolicy;
  private ExceptionsTranslator exceptionsTranslator;  
  
  public OperationFactory(ConsistencyLevelPolicy consistencyLevelPolicy) {    
    this.consistencyLevelPolicy = consistencyLevelPolicy;
    this.exceptionsTranslator = new ExceptionsTranslatorImpl();
    ThriftSupport.consistencyLevelPolicy = consistencyLevelPolicy;  
  }
  
  ConsistencyLevel getThriftCl(OperationType operationType) {
    return ThriftConverter.consistencyLevel(consistencyLevelPolicy.get(operationType));
  }
 
  private static class ThriftSupport
  {
    public static ConsistencyLevelPolicy consistencyLevelPolicy; 
    
    static ConsistencyLevel getThriftCl(OperationType operationType) {
      return ThriftConverter.consistencyLevel(consistencyLevelPolicy.get(operationType));
    }
    
    static List<Column> getColumnList(List<ColumnOrSuperColumn> columns) {
      ArrayList<Column> list = new ArrayList<Column>(columns.size());
      for (ColumnOrSuperColumn col : columns) {
        list.add(col.getColumn());
      }
      return list;
    }
  }
  
  public Operation<?> buildRangeSliceOperation(ColumnParent columnParent, SlicePredicate predicate, KeyRange keyRange) {
    RangeSliceOperation op = new RangeSliceOperation(columnParent, predicate, keyRange); 
    op.consistencyLevelPolicy = consistencyLevelPolicy; 
    op.exceptionsTranslator = exceptionsTranslator; 
    
    return op; 
  }
  
  public Operation<?> buildMultiGetSliceOperation(List<ByteBuffer> keys, ColumnParent columnParent, SlicePredicate predicate) {
    MultiGetSliceOperation op = new MultiGetSliceOperation(keys, columnParent, predicate); 
    op.consistencyLevelPolicy = consistencyLevelPolicy; 
    op.exceptionsTranslator = exceptionsTranslator; 
    
    return op; 
  }
  
  public <K> Operation<?> buildBatchMutateOperation(BatchMutation<K> batchMutation) { 
    BatchMutateOperation<K> op = new BatchMutateOperation<K>(batchMutation); 
    op.consistencyLevelPolicy = consistencyLevelPolicy; 
    op.exceptionsTranslator = exceptionsTranslator; 
    
    return op; 
  }
  
  public Operation<?> buildDescribeRingOperation(String keyspace) { 
    DescribeRingOperation op = new DescribeRingOperation(keyspace);
    
    op.exceptionsTranslator = exceptionsTranslator; 
    return op; 
  }
 
  
  public static class RangeSliceOperation extends Operation<Map<ByteBuffer, List<Column>>> {  
    
    private ColumnParent columnParent; 
    private SlicePredicate predicate; 
    private KeyRange keyRange; 
    
    public ExceptionsTranslator exceptionsTranslator; 
    
    public RangeSliceOperation(ColumnParent columnParent, SlicePredicate predicate, KeyRange keyRange) {
      super(OperationType.READ); 
      
      this.columnParent = columnParent; 
      this.predicate = predicate; 
      this.keyRange = keyRange; 
    }
   
    @Override
    public Map<ByteBuffer, List<Column>> execute(Client cassandra) throws Exception {
      try {
        List<KeySlice> keySlices = cassandra.get_range_slices(columnParent,
            predicate, keyRange, ThriftSupport.getThriftCl(operationType));
        if (keySlices == null || keySlices.isEmpty()) {
          return new LinkedHashMap<ByteBuffer, List<Column>>(0);
        }
        LinkedHashMap<ByteBuffer, List<Column>> ret = new LinkedHashMap<ByteBuffer, List<Column>>(
            keySlices.size());
        for (KeySlice keySlice : keySlices) {
          ret.put(ByteBuffer.wrap(keySlice.getKey()), ThriftSupport.getColumnList(keySlice.getColumns()));
        }
        return ret;
      } catch (Exception e) {
        throw exceptionsTranslator.translate(e);
      }
    }
    
    public KeyRange getKeyRange() { 
      return keyRange; 
    }
  }
  
  public static class MultiGetSliceOperation extends Operation<Map<ByteBuffer,List<Column>>> {
    
    private List<ByteBuffer> keys; 
    private ColumnParent columnParent; 
    private SlicePredicate predicate; 
    
    public ExceptionsTranslator exceptionsTranslator; 
    
    public List<ByteBuffer> getKeys() { 
      return keys; 
    } 
    
    public MultiGetSliceOperation(List<ByteBuffer> keys, ColumnParent columnParent, SlicePredicate predicate) {
      super(OperationType.READ);
      
      this.columnParent = columnParent; 
      this.predicate = predicate; 
      this.keys = keys; 
    }

    @Override
    public Map<ByteBuffer, List<Column>> execute(Client cassandra) throws Exception {
      try {
        Map<ByteBuffer, List<ColumnOrSuperColumn>> cfmap = cassandra.multiget_slice(
            keys, columnParent, predicate, ThriftSupport.getThriftCl(operationType));

        Map<ByteBuffer, List<Column>> result = new HashMap<ByteBuffer, List<Column>>();
        for (Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> entry : cfmap.entrySet()) {
          result.put(entry.getKey(), ThriftSupport.getColumnList(entry.getValue()));
        }
        return result;
      } catch (Exception e) {
          throw exceptionsTranslator.translate(e);
      }
    }
  }
  
  public static class BatchMutateOperation<K> extends Operation<Void> {

    private BatchMutation<K> mutations;
    public ExceptionsTranslator exceptionsTranslator; 
    
    public BatchMutateOperation(BatchMutation<K> batchMutation) {
      super(OperationType.WRITE);
      
      this.mutations = batchMutation; 
    }

    @Override
    public Void execute(Client cassandra) throws Exception {
      try { 
        cassandra.batch_mutate(mutations.getMutationMap(), ThriftSupport.getThriftCl(operationType)); 
      } catch(Exception e) {
          throw exceptionsTranslator.translate(e); 
      }
      
      return null;
    }
    
    public BatchMutation<K> getMutations() { 
      return mutations; 
    }
  }
  
  public static class DescribeRingOperation extends Operation<List<TokenRange>> {
    
    private String keyspace; 
    public ExceptionsTranslator exceptionsTranslator; 
    
    public DescribeRingOperation(String keyspace) {
      super(OperationType.META_READ);
      
      this.keyspace = keyspace; 
    }

    @Override
    public List<TokenRange> execute(Client cassandra) throws Exception {
      try {
        return cassandra.describe_ring(keyspace);
      } catch (Exception e) {
        throw exceptionsTranslator.translate(e);
      }
    } 
    
  }
}
