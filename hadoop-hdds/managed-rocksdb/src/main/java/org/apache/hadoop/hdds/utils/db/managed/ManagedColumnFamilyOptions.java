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

import java.util.Collections;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.Options;
import org.rocksdb.TableFormatConfig;
import org.rocksdb.TablePropertiesCollectorFactory;

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

  private ManagedColumnFamilyOptions(Options options) {
    super(options);
  }

  /** Creates options that request compaction when an SST's deletion ratio reaches the given threshold. */
  public static ManagedColumnFamilyOptions withDeletionRatio(double deletionRatio) {
    if (!Double.isFinite(deletionRatio) || deletionRatio < 0 || deletionRatio > 1) {
      throw new IllegalArgumentException("Deletion ratio must be between 0 and 1 inclusive: " + deletionRatio);
    }
    if (deletionRatio == 0) {
      return new ManagedColumnFamilyOptions();
    }
    // RocksDB exposes the collector setter only on Options. The native options retain a shared pointer to the factory.
    try (ManagedOptions options = new ManagedOptions();
         TablePropertiesCollectorFactory factory =
             TablePropertiesCollectorFactory.NewCompactOnDeletionCollectorFactory(0, 0, deletionRatio)) {
      options.setTablePropertiesCollectorFactory(Collections.singletonList(factory));
      return new ManagedColumnFamilyOptions(options);
    }
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
