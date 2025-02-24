/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.repair;

import jakarta.annotation.Nonnull;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tool to perform compaction on a table.
 */
@CommandLine.Command(
    name = "compact",
    description = "CLI to compact a table in the DB.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class RocksDBManualCompaction extends RepairTool {

  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "Database File Path")
  private String dbPath;

  @CommandLine.Option(names = {"--column_family", "--column-family", "--cf"},
      required = true,
      description = "Table name")
  private String columnFamilyName;

  @Override
  public void execute() throws Exception {
    List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
    List<ColumnFamilyDescriptor> cfDescList = RocksDBUtils.getColumnFamilyDescriptors(
        dbPath);

    try (ManagedRocksDB db = ManagedRocksDB.open(dbPath, cfDescList, cfHandleList)) {
      ColumnFamilyHandle cfh = RocksDBUtils.getColumnFamilyHandle(columnFamilyName, cfHandleList);
      if (cfh == null) {
        throw new IllegalArgumentException(columnFamilyName +
            " is not in a column family in DB for the given path.");
      }

      info("Running compaction on " + (columnFamilyName == null ? "entire DB" : columnFamilyName));

      if (!isDryRun()) {
        db.get().compactRange(cfh);
        info("Compaction completed.");
      }
    } catch (RocksDBException exception) {
      error("Failed to compact the RocksDB for the given path: %s, column-family:%s", dbPath, columnFamilyName);
      error("Exception: " + exception);
      throw new IOException("Failed to compact RocksDB.", exception);
    } finally {
      IOUtils.closeQuietly(cfHandleList);
    }
  }

  @Override
  @Nonnull
  protected Component serviceToBeOffline() {
    final String parent = spec().parent().name();
    switch (parent) {
    case "om":
      return Component.OM;
    case "scm":
      return Component.SCM;
    default:
      throw new IllegalStateException("Unknown component: " + parent);
    }
  }
}
