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

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.SNAPSHOT_INFO_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.managed.ManagedConfigOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.repair.RepairTool;
import org.apache.hadoop.ozone.shell.bucket.BucketUri;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Tool to repair snapshotInfoTable in case it has corrupted entries.
 */
@CommandLine.Command(
    name = "chain",
    description = "CLI to update global and path previous snapshot for a snapshot in case snapshot chain is corrupted."
        + " OM should be stopped for this tool."
)
public class SnapshotChainRepair extends RepairTool {

  protected static final Logger LOG = LoggerFactory.getLogger(SnapshotChainRepair.class);

  @CommandLine.Mixin
  private BucketUri bucketUri;

  @CommandLine.Parameters(description = "Snapshot name to update", index = "1")
  private String snapshotName;

  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "Database File Path")
  private String dbPath;

  @CommandLine.Option(names = {"--global-previous", "--gp"},
      required = true,
      description = "Global previous snapshotId to set for the given snapshot")
  private UUID globalPreviousSnapshotId;

  @CommandLine.Option(names = {"--path-previous", "--pp"},
      required = true,
      description = "Path previous snapshotId to set for the given snapshot")
  private UUID pathPreviousSnapshotId;

  @Nonnull
  @Override
  protected Component serviceToBeOffline() {
    return Component.OM;
  }

  @Override
  public void execute() throws Exception {
    ManagedConfigOptions configOptions = new ManagedConfigOptions();
    ManagedDBOptions dbOptions = new ManagedDBOptions();
    List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
    List<ColumnFamilyDescriptor> cfDescList = new ArrayList<>();

    try (ManagedRocksDB db = ManagedRocksDB.openWithLatestOptions(
        configOptions, dbOptions, dbPath, cfDescList, cfHandleList)) {
      ColumnFamilyHandle snapshotInfoCfh = RocksDBUtils.getColumnFamilyHandle(SNAPSHOT_INFO_TABLE, cfHandleList);
      if (snapshotInfoCfh == null) {
        error("%s is not in a column family in DB for the given path.", SNAPSHOT_INFO_TABLE);
        return;
      }

      String snapshotInfoTableKey = SnapshotInfo.getTableKey(bucketUri.getValue().getVolumeName(),
          bucketUri.getValue().getBucketName(), snapshotName);

      SnapshotInfo snapshotInfo = RocksDBUtils.getValue(db, snapshotInfoCfh, snapshotInfoTableKey,
          SnapshotInfo.getCodec());

      if (snapshotInfo == null) {
        error("%s does not exist for given bucketUri: %s", snapshotName, OM_KEY_PREFIX +
            bucketUri.getValue().getVolumeName() + OM_KEY_PREFIX + bucketUri.getValue().getBucketName());
        return;
      }

      // snapshotIdSet is the set of the all existed snapshots ID to make that the provided global previous and path
      // previous exist and after the update snapshot does not point to ghost snapshot.
      Set<UUID> snapshotIdSet = getSnapshotIdSet(db, snapshotInfoCfh);

      if (Objects.equals(snapshotInfo.getSnapshotId(), globalPreviousSnapshotId)) {
        error("globalPreviousSnapshotId: '%s' is equal to given snapshot's ID: '%s'.",
            globalPreviousSnapshotId, snapshotInfo.getSnapshotId());
        return;
      }

      if (Objects.equals(snapshotInfo.getSnapshotId(), pathPreviousSnapshotId)) {
        error("pathPreviousSnapshotId: '%s' is equal to given snapshot's ID: '%s'.",
            pathPreviousSnapshotId, snapshotInfo.getSnapshotId());
        return;
      }

      if (!snapshotIdSet.contains(globalPreviousSnapshotId)) {
        error("globalPreviousSnapshotId: '%s' does not exist in snapshotInfoTable.",
            globalPreviousSnapshotId);
        return;
      }

      if (!snapshotIdSet.contains(pathPreviousSnapshotId)) {
        error("pathPreviousSnapshotId: '%s' does not exist in snapshotInfoTable.",
            pathPreviousSnapshotId);
        return;
      }

      snapshotInfo.setGlobalPreviousSnapshotId(globalPreviousSnapshotId);
      snapshotInfo.setPathPreviousSnapshotId(pathPreviousSnapshotId);

      info("Updating SnapshotInfo to %s", snapshotInfo);

      byte[] snapshotInfoBytes = SnapshotInfo.getCodec().toPersistedFormat(snapshotInfo);
      byte[] persistedFormat = StringCodec.get().toPersistedFormat(snapshotInfoTableKey);

      if (!isDryRun()) {
        db.get().put(snapshotInfoCfh, persistedFormat, snapshotInfoBytes);

        info("Snapshot Info is updated to : %s",
            RocksDBUtils.getValue(db, snapshotInfoCfh, snapshotInfoTableKey, SnapshotInfo.getCodec()));
      }
    } catch (RocksDBException exception) {
      error("Failed to update the RocksDB for the given path: %s", dbPath);
      error(
          "Make sure that Ozone entity (OM, SCM or DN) is not running for the give dbPath and current host.");
      LOG.error(exception.toString());
    } finally {
      IOUtils.closeQuietly(configOptions);
      IOUtils.closeQuietly(dbOptions);
      IOUtils.closeQuietly(cfHandleList);
    }
  }

  private Set<UUID> getSnapshotIdSet(ManagedRocksDB db, ColumnFamilyHandle snapshotInfoCfh)
      throws IOException {
    Set<UUID> snapshotIdSet = new HashSet<>();
    try (ManagedRocksIterator iterator = new ManagedRocksIterator(db.get().newIterator(snapshotInfoCfh))) {
      iterator.get().seekToFirst();

      while (iterator.get().isValid()) {
        SnapshotInfo snapshotInfo = SnapshotInfo.getCodec().fromPersistedFormat(iterator.get().value());
        snapshotIdSet.add(snapshotInfo.getSnapshotId());
        iterator.get().next();
      }
    }
    return snapshotIdSet;
  }
}
