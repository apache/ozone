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

package org.apache.hadoop.ozone.repair.ldb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedConfigOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.repair.RepairTool;
import org.apache.hadoop.util.Time;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

/**
 * Tool to perform compaction on a table.
 */
@CommandLine.Command(
    name = "compact",
    description = "CLI to compact a column-family in the DB while the service is offline.\n" +
        "Note: If om.db is compacted with this tool then it will negatively impact " +
        "the Ozone Manager's efficient snapshot diff." + 
        " The corresponding OM, SCM or Datanode role should be stopped for this tool.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class RocksDBManualCompaction extends RepairTool {

  private static final String WARNING_TO_STOP_SERVICE =
      "WARNING: Ensure the related service is stopped before compacting this database." +
          " Do you want to continue (y/N)? ";

  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "Database File Path")
  private String dbPath;

  @CommandLine.Option(names = {"--column-family", "--column_family", "--cf"},
      required = true,
      description = "Column family name")
  private String columnFamilyName;

  private String getConsoleReadLineWithFormat() {
    err().printf(WARNING_TO_STOP_SERVICE);
    return getScanner().nextLine().trim();
  }

  /**
   * This tool does not override {@link RepairTool#serviceToBeOffline()}
   * as it is a generic RocksDB compaction tool that can be used for ANY
   * RocksDB database. Added a warning to ensure users stop the service
   * before running compaction.
   */
  @Override
  public void execute() throws Exception {
    if (!isDryRun()) {
      confirmUser();
      final boolean confirmed = "y".equalsIgnoreCase(getConsoleReadLineWithFormat());
      if (!confirmed) {
        throw new IllegalStateException("Aborting compaction.");
      }
    }

    ManagedConfigOptions configOptions = new ManagedConfigOptions();
    ManagedDBOptions dbOptions = new ManagedDBOptions();
    List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
    List<ColumnFamilyDescriptor> cfDescList = new ArrayList<>();

    try (ManagedRocksDB db = ManagedRocksDB.openWithLatestOptions(
        configOptions, dbOptions, dbPath, cfDescList, cfHandleList)) {
      ColumnFamilyHandle cfh = RocksDBUtils.getColumnFamilyHandle(columnFamilyName, cfHandleList);
      if (cfh == null) {
        throw new IllegalArgumentException(columnFamilyName +
            " is not in a column family in DB for the given path.");
      }

      info("Running compaction on " + columnFamilyName);
      long startTime = Time.monotonicNow();
      if (!isDryRun()) {
        ManagedCompactRangeOptions compactOptions = new ManagedCompactRangeOptions();
        compactOptions.setBottommostLevelCompaction(ManagedCompactRangeOptions.BottommostLevelCompaction.kForce);
        db.get().compactRange(cfh, null, null, compactOptions);
      }
      long duration = Time.monotonicNow() - startTime;
      info("Compaction completed in " + duration + "ms.");

    } catch (RocksDBException exception) {
      error("Exception: " + exception);
      String errorMsg = "Failed to compact RocksDB for the given path: " + dbPath +
          ", column family: " + columnFamilyName;
      throw new IOException(errorMsg, exception);
    } finally {
      IOUtils.closeQuietly(configOptions);
      IOUtils.closeQuietly(dbOptions);
      IOUtils.closeQuietly(cfHandleList);
    }
  }
}
