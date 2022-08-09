package org.apache.hadoop.hdds.utils.db.managed;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;

public class ManagedBlockBasedTableConfig extends BlockBasedTableConfig {
  private Cache blockCacheHolder;

  @Override
  public BlockBasedTableConfig setBlockCache(Cache blockCache) {
    // Close the previous Cache before overwriting.
    if (blockCacheHolder != null) {
      blockCacheHolder.close();
    }

    blockCacheHolder = blockCache;
    return super.setBlockCache(blockCache);
  }

  /**
   * Close children resources.
   * See org.apache.hadoop.hdds.utils.db.DBProfile.getBlockBasedTableConfig
   */
  public void close() {
    if (filterPolicy() != null) {
      filterPolicy().close();
    }
    if (blockCacheHolder != null) {
      blockCacheHolder.close();
    }
  }
}
