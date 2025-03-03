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

package org.apache.hadoop.ozone.repair.om;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.repair.RepairTool;
import org.apache.hadoop.ozone.utils.OmClientFactory;
import picocli.CommandLine;

/**
 * Tool to perform compaction on a column family of a rocksDB.
 */
@CommandLine.Command(
    name = "compact",
    description = "CLI to compact a column family in the DB.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class RocksDBManualCompaction extends RepairTool {

  @CommandLine.Option(names = {"--column_family", "--column-family", "--cf"},
      required = true,
      description = "Column family name")
  private String columnFamilyName;

  @CommandLine.Option(
      names = {"--service-id", "--om-service-id"},
      description = "Ozone Manager Service ID",
      required = false
  )
  private String omServiceId;

  @CommandLine.Option(
      names = {"--service-host"},
      description = "Ozone Manager Host. If OM HA is enabled, use --service-id instead. "
          + "If you must use --service-host with OM HA, this must point directly to the leader OM. "
          + "This option is required when --service-id is not provided or when HA is not enabled."
  )
  private String omHost;

  @Override
  public void execute() throws Exception {

    OzoneManagerProtocol omClient = OmClientFactory.createOmClient(omServiceId, omHost, false);
    omClient.compactOMDB(columnFamilyName);
    info("Compaction request issued for om.db of host: %s, column-family: %s.", omHost, columnFamilyName);
    /*List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
    List<ColumnFamilyDescriptor> cfDescList = RocksDBUtils.getColumnFamilyDescriptors(
        dbPath);
    try (ManagedRocksDB db = ManagedRocksDB.open(dbPath, cfDescList, cfHandleList)) {
      ColumnFamilyHandle cfh = RocksDBUtils.getColumnFamilyHandle(columnFamilyName, cfHandleList);
      if (cfh == null) {
        throw new IllegalArgumentException(columnFamilyName + " is not in a column family in DB for the given path.");
      }

      info("Running compaction on " + columnFamilyName);

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
    }*/
  }
}
