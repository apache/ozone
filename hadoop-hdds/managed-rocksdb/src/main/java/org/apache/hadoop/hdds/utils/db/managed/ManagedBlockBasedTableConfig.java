/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.utils.db.managed;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Managed BlockBasedTableConfig.
 */
public class ManagedBlockBasedTableConfig extends BlockBasedTableConfig {
  private Cache blockCacheHolder;
  private AtomicBoolean closed = new AtomicBoolean(false);

  public synchronized ManagedBlockBasedTableConfig closeAndSetBlockCache(
      Cache blockCache) {
    Cache previous = blockCacheHolder;
    if (previous.isOwningHandle()) {
      previous.close();
    }
    return setBlockCache(blockCache);
  }

  @Override
  public synchronized ManagedBlockBasedTableConfig setBlockCache(
      Cache blockCache) {
    // Close the previous Cache before overwriting.
    Cache previous = blockCacheHolder;
    if (previous != null && previous.isOwningHandle()) {
      throw new IllegalStateException("Overriding an unclosed value.");
    }

    blockCacheHolder = blockCache;
    super.setBlockCache(blockCache);
    return this;
  }

  public boolean isClosed() {
    return closed.get();
  }

  /**
   * Close children resources.
   * See org.apache.hadoop.hdds.utils.db.DBProfile.getBlockBasedTableConfig
   */
  public void close() {
    if (closed.compareAndSet(false, true)) {
      if (filterPolicy() != null) {
        filterPolicy().close();
      }
      if (blockCacheHolder != null) {
        blockCacheHolder.close();
      }
    }
  }
}
