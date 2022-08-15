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

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.TableFormatConfig;

/**
 * Managed ColumnFamilyOptions.
 */
public class ManagedColumnFamilyOptions extends ColumnFamilyOptions {
  public ManagedColumnFamilyOptions() {
    super();
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
    } else if (previous != null) {
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


  @Override
  protected void finalize() throws Throwable {
    ManagedRocksObjectUtils.assertClosed(this);
    super.finalize();
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
