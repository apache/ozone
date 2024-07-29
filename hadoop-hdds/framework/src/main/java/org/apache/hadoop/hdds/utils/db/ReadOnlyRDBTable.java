package org.apache.hadoop.hdds.utils.db;

import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class ReadOnlyRDBTable extends RDBTable {
  private String unsupportedErrorMessage;

  private static String UNSUPPORTED_OPERATION_EXCEPTION_MESSAGE_FORMAT = "Updating value %s to the table is not " +
      "supported for readOnlyTable";
  /**
   * Constructs a TableStore.
   *
   * @param db         - DBstore that we are using.
   * @param family     - ColumnFamily Handle.
   * @param rdbMetrics
   */
  ReadOnlyRDBTable(RocksDatabase db, RocksDatabase.ColumnFamily family, RDBMetrics rdbMetrics) {
    super(db, family, rdbMetrics);
    unsupportedErrorMessage = String.format(UNSUPPORTED_OPERATION_EXCEPTION_MESSAGE_FORMAT, family.getName());
  }

  @Override
  void put(ByteBuffer key, ByteBuffer value) throws IOException {
    throw new UnsupportedOperationException(unsupportedErrorMessage);
  }

  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    throw new UnsupportedOperationException(unsupportedErrorMessage);
  }

  @Override
  void putWithBatch(BatchOperation batch, CodecBuffer key, CodecBuffer value) throws IOException {
    throw new UnsupportedOperationException(unsupportedErrorMessage);
  }

  @Override
  public void putWithBatch(BatchOperation batch, byte[] key, byte[] value) throws IOException {
    throw new UnsupportedOperationException(unsupportedErrorMessage);
  }

  @Override
  public void delete(byte[] key) throws IOException {
    throw new UnsupportedOperationException(unsupportedErrorMessage);
  }

  @Override
  public void delete(ByteBuffer key) throws IOException {
    throw new UnsupportedOperationException(unsupportedErrorMessage);
  }

  @Override
  public void deleteRange(byte[] beginKey, byte[] endKey) throws IOException {
    throw new UnsupportedOperationException(unsupportedErrorMessage);
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, byte[] key) throws IOException {
    throw new UnsupportedOperationException(unsupportedErrorMessage);
  }

  @Override
  public void deleteBatchWithPrefix(BatchOperation batch, byte[] prefix) throws IOException {
    throw new UnsupportedOperationException(unsupportedErrorMessage);
  }

  @Override
  public byte[] getReadCopy(byte[] bytes) throws IOException {
    throw new UnsupportedOperationException(unsupportedErrorMessage);
  }

  @Override
  public void addCacheEntry(CacheKey<byte[]> cacheKey, CacheValue<byte[]> cacheValue) {
    throw new UnsupportedOperationException(unsupportedErrorMessage);
  }

  @Override
  public void addCacheEntry(byte[] cacheKey, long epoch) {
    throw new UnsupportedOperationException(unsupportedErrorMessage);
  }

  @Override
  public void addCacheEntry(byte[] cacheKey, byte[] bytes, long epoch) {
    throw new UnsupportedOperationException(unsupportedErrorMessage);
  }

  @Override
  public CacheValue<byte[]> getCacheValue(CacheKey<byte[]> cacheKey) {
    throw new UnsupportedOperationException(unsupportedErrorMessage);
  }

  @Override
  public void cleanupCache(List<Long> epochs) {
    throw new UnsupportedOperationException(unsupportedErrorMessage);
  }
}
