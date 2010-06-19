package me.prettyprint.cassandra.model;

public interface Result extends ExecutionResult {

  // get
  Column asColumn();
  
  // get of super
  SuperColumn asSuperColumn();
  
  // get_slice
  ColumnSlice asColumnSlice();
  
  // multiget_slice
  Rows asRows();
  
  // get_range_slices
  OrderedRows asOrderedRows();
  
  // get_count
  int asInt();

  // Short for asColumn().getValue().asString()
  String asString();
  
  // Short for asColumn().getValue().raw(). Returns the bytes of the first column value.
  byte[] raw();

  // maybe: <T> T asType(Class<T> type);
  // or: <T> T asType();
  
  Query getQuery();
  
}