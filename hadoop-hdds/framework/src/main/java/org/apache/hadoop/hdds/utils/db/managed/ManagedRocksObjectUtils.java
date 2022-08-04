package org.apache.hadoop.hdds.utils.db.managed;

import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.rocksdb.RocksObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities to help assert RocksObject closures.
 */
public final class ManagedRocksObjectUtils {
  private ManagedRocksObjectUtils() {
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(ManagedRocksObjectUtils.class);

  public static void assertClosed(RocksDatabase rocksDb) {
    ManagedRocksObjectMetrics.INSTANCE.increaseManagedObject();
    if (!rocksDb.isClosed()) {
      ManagedRocksObjectMetrics.INSTANCE.increaseLeakObject();
      LOG.warn("{} is not closed properly", rocksDb.getClass().getSimpleName());
      rocksDb.close();
    }
  }

  static void assertClosed(RocksObject rocksObject) {
    ManagedRocksObjectMetrics.INSTANCE.increaseManagedObject();
    if (rocksObject.isOwningHandle()) {
      ManagedRocksObjectMetrics.INSTANCE.increaseLeakObject();
      LOG.warn("{} is not closed properly",
          rocksObject.getClass().getSimpleName());
      rocksObject.close();
    }
  }

  static void assertClosed(RocksObject rocksObject, Throwable stack) {
    ManagedRocksObjectMetrics.INSTANCE.increaseManagedObject();
    if (rocksObject.isOwningHandle()) {
      ManagedRocksObjectMetrics.INSTANCE.increaseLeakObject();
      LOG.warn("{} is not closed properly",
          rocksObject.getClass().getSimpleName(), stack);
      rocksObject.close();
    }
  }
}
