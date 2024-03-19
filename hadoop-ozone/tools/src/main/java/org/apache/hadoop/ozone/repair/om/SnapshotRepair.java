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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.repair.om;

import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.repair.RDBRepair;
import org.apache.hadoop.ozone.shell.bucket.BucketUri;
import org.kohsuke.MetaInfServices;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.SNAPSHOT_INFO_TABLE;

/**
 * Tool to repair snapshotInfoTable in case it has corrupted entries.
 */
@CommandLine.Command(
    name = "snapshot",
    description = "CLI to update global and path previous snapshot for a snapshot in case snapshot chain is corrupted."
)
@MetaInfServices(SubcommandWithParent.class)
public class SnapshotRepair implements Callable<Void>, SubcommandWithParent {

  @CommandLine.Spec
  private static CommandSpec spec;

  @CommandLine.ParentCommand
  private RDBRepair parent;

  @CommandLine.Mixin
  private BucketUri bucketUri;

  @CommandLine.Parameters(description = "Snapshot name to update", index = "1")
  private String snapshotName;

  @CommandLine.Option(names = {"--global-previous", "--gp"},
      required = true,
      description = "Global previous snapshotId to set for the given snapshot")
  private UUID globalPreviousSnapshotId;

  @CommandLine.Option(names = {"--path-previous", "--pp"},
      required = true,
      description = "Path previous snapshotId to set for the given snapshot")
  private UUID pathPreviousSnapshotId;

  @CommandLine.Option(names = {"--dry-run"},
      required = true,
      description = "To dry-run the command.", defaultValue = "true")
  private boolean dryRun;

  @Override
  public Void call() throws Exception {
    List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
    List<ColumnFamilyDescriptor> cfDescList = RocksDBUtils.getColumnFamilyDescriptors(parent.getDbPath());

    try (ManagedRocksDB db = ManagedRocksDB.open(parent.getDbPath(), cfDescList, cfHandleList)) {
      ColumnFamilyHandle snapshotInfoCfh = getSnapshotInfoCfh(cfHandleList);
      if (snapshotInfoCfh == null) {
        System.err.println(SNAPSHOT_INFO_TABLE + " is not in a column family in DB for the given path.");
        return null;
      }

      String snapshotInfoTableKey = SnapshotInfo.getTableKey(bucketUri.getValue().getVolumeName(),
          bucketUri.getValue().getBucketName(), snapshotName);

      SnapshotInfo snapshotInfo = getSnapshotInfo(db, snapshotInfoCfh, snapshotInfoTableKey);
      if (snapshotInfo == null) {
        System.err.println(snapshotName + " does not exist for given bucketUri: " + OM_KEY_PREFIX +
            bucketUri.getValue().getVolumeName() + OM_KEY_PREFIX + bucketUri.getValue().getBucketName());
        return null;
      }

      // snapshotIdSet is the set of the all existed snapshots ID to make that the provided global previous and path
      // previous exist and after the update snapshot does not point to ghost snapshot.
      Set<UUID> snapshotIdSet = getSnapshotIdSet(db, snapshotInfoCfh);

      if (Objects.equals(snapshotInfo.getSnapshotId(), globalPreviousSnapshotId)) {
        System.err.println("globalPreviousSnapshotId: '" + globalPreviousSnapshotId +
            "' is equal to given snapshot's ID: '" + snapshotInfo.getSnapshotId() + "'.");
        return null;
      }

      if (Objects.equals(snapshotInfo.getSnapshotId(), pathPreviousSnapshotId)) {
        System.err.println("pathPreviousSnapshotId: '" + pathPreviousSnapshotId +
            "' is equal to given snapshot's ID: '" + snapshotInfo.getSnapshotId() + "'.");
        return null;
      }

      if (!snapshotIdSet.contains(globalPreviousSnapshotId)) {
        System.err.println("globalPreviousSnapshotId: '" + globalPreviousSnapshotId +
            "' does not exist in snapshotInfoTable.");
        return null;
      }

      if (!snapshotIdSet.contains(pathPreviousSnapshotId)) {
        System.err.println("pathPreviousSnapshotId: '" + pathPreviousSnapshotId +
            "' does not exist in snapshotInfoTable.");
        return null;
      }

      snapshotInfo.setGlobalPreviousSnapshotId(globalPreviousSnapshotId);
      snapshotInfo.setPathPreviousSnapshotId(pathPreviousSnapshotId);

      if (dryRun) {
        System.out.println("SnapshotInfo would be updated to : " + snapshotInfo);
      } else {
        byte[] snapshotInfoBytes = SnapshotInfo.getCodec().toPersistedFormat(snapshotInfo);
        db.get()
            .put(snapshotInfoCfh, StringCodec.get().toPersistedFormat(snapshotInfoTableKey), snapshotInfoBytes);

        System.out.println("Snapshot Info is updated to : " +
            getSnapshotInfo(db, snapshotInfoCfh, snapshotInfoTableKey));
      }
    } catch (RocksDBException exception) {
      System.err.println("Failed to update the RocksDB for the given path: " + parent.getDbPath());
      System.err.println(
          "Make sure that Ozone entity (OM, SCM or DN) is not running for the give dbPath and current host.");
      System.err.println(exception);
    } finally {
      IOUtils.closeQuietly(cfHandleList);
    }

    return null;
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

  private ColumnFamilyHandle getSnapshotInfoCfh(List<ColumnFamilyHandle> cfHandleList) throws RocksDBException {
    byte[] nameBytes = SNAPSHOT_INFO_TABLE.getBytes(StandardCharsets.UTF_8);

    for (ColumnFamilyHandle cf : cfHandleList) {
      if (Arrays.equals(cf.getName(), nameBytes)) {
        return cf;
      }
    }

    return null;
  }

  private SnapshotInfo getSnapshotInfo(ManagedRocksDB db, ColumnFamilyHandle snapshotInfoCfh, String snapshotInfoLKey)
      throws IOException, RocksDBException {
    byte[] bytes = db.get().get(snapshotInfoCfh, StringCodec.get().toPersistedFormat(snapshotInfoLKey));
    return bytes != null ? SnapshotInfo.getCodec().fromPersistedFormat(bytes) : null;
  }

  @Override
  public Class<?> getParentType() {
    return RDBRepair.class;
  }
}
