package org.apache.hadoop.hdds.utils.db.managed;

import org.rocksdb.RocksObject;
/**
 * Managed RocksObject.
 */
public abstract class ManagedRocksObject extends RocksObject {
  protected ManagedRocksObject(long nativeHandle) {
    super(nativeHandle);
  }

  @Override
  protected void finalize() throws Throwable {
    ManagedRocksObjectUtils.assertClosed(this);
    super.finalize();
  }
}
