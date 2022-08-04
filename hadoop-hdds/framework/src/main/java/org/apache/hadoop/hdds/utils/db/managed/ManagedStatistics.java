package org.apache.hadoop.hdds.utils.db.managed;

import org.rocksdb.Statistics;

/**
 * Managed Statistics.
 */
public class ManagedStatistics extends Statistics {
  @Override
  protected void finalize() throws Throwable {
    ManagedRocksObjectUtils.assertClosed(this);
    super.finalize();
  }
}
