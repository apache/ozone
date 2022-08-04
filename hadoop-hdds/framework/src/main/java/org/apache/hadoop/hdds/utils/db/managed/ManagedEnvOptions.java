package org.apache.hadoop.hdds.utils.db.managed;

import org.rocksdb.EnvOptions;

/**
 * Managed EnvOptions.
 */
public class ManagedEnvOptions extends EnvOptions {
  @Override
  protected void finalize() throws Throwable {
    ManagedRocksObjectUtils.assertClosed(this);
    super.finalize();
  }
}
