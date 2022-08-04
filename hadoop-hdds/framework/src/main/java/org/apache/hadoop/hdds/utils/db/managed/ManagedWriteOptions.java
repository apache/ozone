package org.apache.hadoop.hdds.utils.db.managed;

import org.rocksdb.WriteOptions;

/**
 * Managed WriteOptions.
 */
public class ManagedWriteOptions extends WriteOptions {
  @Override
  protected void finalize() throws Throwable {
    ManagedRocksObjectUtils.assertClosed(this);
    super.finalize();
  }
}
