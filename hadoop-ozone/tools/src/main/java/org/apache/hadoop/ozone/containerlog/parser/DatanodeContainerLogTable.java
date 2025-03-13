package org.apache.hadoop.ozone.containerlog.parser;

import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import java.io.File;
import java.io.IOException;
import java.util.List;

public class DatanodeContainerLogTable <KEY, VALUE> implements Table<KEY, VALUE> {
  private final Table<KEY, VALUE> table;

  public DatanodeContainerLogTable(Table<KEY, VALUE> table) {
    this.table = table;
  }

  public Table<KEY, VALUE> getTable() {
    return table;
  }

  @Override
  public void put(KEY key, VALUE value) throws IOException {
    table.put(key, value);
  }

  @Override
  public void putWithBatch(BatchOperation batch, KEY key,
                           VALUE value) throws IOException {
    table.putWithBatch(batch, key, value);
  }

  @Override
  public boolean isEmpty() throws IOException {
    return table.isEmpty();
  }

  @Override
  public void delete(KEY key) throws IOException {
    table.delete(key);
  }

  @Override
  public void deleteRange(KEY beginKey, KEY endKey) throws IOException {
    table.deleteRange(beginKey, endKey);
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, KEY key)
      throws IOException {
    table.deleteWithBatch(batch, key);
  }

  @Override
  public TableIterator<KEY, ? extends KeyValue<KEY, VALUE>> iterator() throws IOException {
    return new DatanodeContainerLogTableIterator<>(this);
  }

  @Override
  public final TableIterator<KEY, ? extends Table.KeyValue<KEY, VALUE>> iterator(
      KEY prefix) {
    throw new UnsupportedOperationException("Iterating tables directly is not" +
        " supported");
  }


  @Override
  public String getName() throws IOException {
    return table.getName();
  }

  @Override
  public long getEstimatedKeyCount() throws IOException {
    return table.getEstimatedKeyCount();
  }

  @Override
  public boolean isExist(KEY key) throws IOException {
    return table.isExist(key);
  }

  @Override
  public VALUE get(KEY key) throws IOException {
    return table.get(key);
  }

  @Override
  public VALUE getIfExist(KEY key) throws IOException {
    return table.getIfExist(key);
  }

  @Override
  public VALUE getReadCopy(KEY key) throws IOException {
    return table.getReadCopy(key);
  }

  @Override
  public List<? extends KeyValue<KEY, VALUE>> getRangeKVs(
      KEY startKey, int count, KEY prefix,
      MetadataKeyFilters.MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    return table.getRangeKVs(startKey, count, prefix, filters);
  }

  @Override
  public List<? extends Table.KeyValue<KEY, VALUE>> getSequentialRangeKVs(
      KEY startKey, int count, KEY prefix,
      MetadataKeyFilters.MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    return table.getSequentialRangeKVs(startKey, count, prefix, filters);
  }

  @Override
  public void deleteBatchWithPrefix(BatchOperation batch, KEY prefix)
      throws IOException {
    table.deleteBatchWithPrefix(batch, prefix);
  }

  @Override
  public void dumpToFileWithPrefix(File externalFile, KEY prefix)
      throws IOException {
    table.dumpToFileWithPrefix(externalFile, prefix);
  }

  @Override
  public void loadFromFile(File externalFile) throws IOException {
    table.loadFromFile(externalFile);
  }

  @Override
  public void close() throws Exception {
    table.close();
  }

}
