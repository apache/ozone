/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.utils.db.managed;

import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils.track;

import org.apache.ratis.util.UncheckedAutoCloseable;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.TableFormatConfig;

/**
 * Managed ColumnFamilyOptions.
 */
public class ManagedColumnFamilyOptions extends ColumnFamilyOptions {
  /**
   * Indicate if this ColumnFamilyOptions is intentionally used across RockDB
   * instances.
   */
  private boolean reused = false;
  private final UncheckedAutoCloseable leakTracker = track(this);

  public ManagedColumnFamilyOptions() {
  }

  public ManagedColumnFamilyOptions(ColumnFamilyOptions columnFamilyOptions) {
    super(columnFamilyOptions);
  }

  @Override
  public synchronized ManagedColumnFamilyOptions setTableFormatConfig(
      TableFormatConfig tableFormatConfig) {
    TableFormatConfig previous = tableFormatConfig();
    if (previous instanceof ManagedBlockBasedTableConfig) {
      if (!((ManagedBlockBasedTableConfig) previous).isClosed()) {
        throw new IllegalStateException("Overriding an unclosed value.");
      }
    } else if (previous != null && !(previous instanceof BlockBasedTableConfig)) {
      //Note that the type of tableFormatConfig read directly from
      //the ini file is org.rocksdb.BlockBasedTableConfig
      throw new UnsupportedOperationException("Overwrite is not supported for "
          + previous.getClass());
    }

    super.setTableFormatConfig(tableFormatConfig);
    return this;
  }

  public synchronized ManagedColumnFamilyOptions closeAndSetTableFormatConfig(
      TableFormatConfig tableFormatConfig) {
    TableFormatConfig previous = tableFormatConfig();
    if (previous instanceof ManagedBlockBasedTableConfig) {
      ((ManagedBlockBasedTableConfig) previous).close();
    }
    setTableFormatConfig(tableFormatConfig);
    return this;
  }

  public void setReused(boolean reused) {
    this.reused = reused;
  }

  public boolean isReused() {
    return reused;
  }

  @Override
  public void close() {
    try {
      super.close();
    } finally {
      leakTracker.close();
    }
  }

  /**
   * Close ColumnFamilyOptions and its child resources.
   * See org.apache.hadoop.hdds.utils.db.DBProfile.getColumnFamilyOptions
   *
   * @param options
   */
  public static void closeDeeply(ColumnFamilyOptions options) {
    TableFormatConfig tableFormatConfig = options.tableFormatConfig();
    if (tableFormatConfig instanceof ManagedBlockBasedTableConfig) {
      ((ManagedBlockBasedTableConfig) tableFormatConfig).close();
    }
    options.close();
  }
}
