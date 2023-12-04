/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.cli.container.upgrade;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.FixedLengthStringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.apache.hadoop.ozone.container.common.utils.RawDB;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaTwoDBDefinition;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_NAME;

/**
 * This class implements the v2 to v3 container upgrade process.
 */
public class UpgradeTask {

  public static final Logger LOG =
      LoggerFactory.getLogger(UpgradeTask.class);

  private final ConfigurationSource config;
  private final HddsVolume hddsVolume;
  private DatanodeStoreSchemaThreeImpl dataStore;
  private final Map volumeStoreMap;

  private static final String BACKUP_CONTAINER_DATA_FILE_SUFFIX = ".backup";
  public static final String UPGRADE_COMPLETE_FILE_NAME = "upgrade.complete";
  public static final String UPGRADE_LOCK_FILE_NAME = "upgrade.lock";

  private static final Set<String> COLUMN_FAMILIES_NAME =
      (new DatanodeSchemaTwoDBDefinition("", new OzoneConfiguration()))
          .getMap().keySet();

  public UpgradeTask(ConfigurationSource config, HddsVolume hddsVolume,
      Map storeMap) {
    this.config = config;
    this.hddsVolume = hddsVolume;
    this.volumeStoreMap = storeMap;
  }

  public CompletableFuture<UpgradeManager.Result> getUpgradeFuture() {
    final File lockFile = UpgradeUtils.getVolumeUpgradeLockFile(hddsVolume);

    return CompletableFuture.supplyAsync(() -> {

      final UpgradeManager.Result result =
          new UpgradeManager.Result(hddsVolume);

      List<UpgradeContainerResult> resultList = new ArrayList<>();
      final File hddsVolumeRootDir = hddsVolume.getHddsRootDir();

      Preconditions.checkNotNull(hddsVolumeRootDir, "hddsVolumeRootDir" +
          "cannot be null");

      // check CID directory and current file
      File clusterIDDir = new File(hddsVolume.getStorageDir(),
          hddsVolume.getClusterID());
      if (!clusterIDDir.exists() || !clusterIDDir.isDirectory()) {
        result.fail(new Exception("Volume " + hddsVolumeRootDir +
            " is in an inconsistent state. Expected " +
            "clusterID directory " + clusterIDDir +
            " is not found or not a directory."));
        return result;
      }
      File currentDir = new File(clusterIDDir, Storage.STORAGE_DIR_CURRENT);
      if (!currentDir.exists() || !currentDir.isDirectory()) {
        result.fail(new Exception(
            "Current dir " + currentDir + " is not found or not a directory,"
                + " skip upgrade."));
        return result;
      }

      try {
        // create lock file
        if (!lockFile.createNewFile()) {
          result.fail(new Exception("Upgrade lock file already exists " +
              lockFile.getAbsolutePath() + ", skip upgrade."));
          return result;
        }
      } catch (IOException e) {
        result.fail(new Exception("Failed to create upgrade lock file " +
            lockFile.getAbsolutePath() + ", skip upgrade."));
        return result;
      }

      // check complete file again
      final File completeFile =
          UpgradeUtils.getVolumeUpgradeCompleteFile(hddsVolume);
      if (completeFile.exists()) {
        result.fail(new Exception("Upgrade complete file already exists " +
            completeFile.getAbsolutePath() + ", skip upgrade."));
        if (!lockFile.delete()) {
          LOG.warn("Failed to delete upgrade lock file {}.", lockFile);
        }
        return result;
      }

      // backup DB directory
      final File volumeDBPath;
      try {
        volumeDBPath = getVolumeDBPath(hddsVolume);
        dbBackup(volumeDBPath);
      } catch (IOException e) {
        result.fail(new Exception(e.getMessage() + ", skip upgrade."));
        return result;
      }

      // load DB store
      try {
        hddsVolume.loadDbStore(false);
        RawDB db = DatanodeStoreCache.getInstance().getDB(
            volumeDBPath.getAbsolutePath(), config);
        dataStore = (DatanodeStoreSchemaThreeImpl) db.getStore();
        volumeStoreMap.put(
            hddsVolume.getStorageDir().getAbsolutePath(), dataStore);
      } catch (IOException e) {
        result.fail(new Exception(
            "Failed to load db for volume " + hddsVolume.getVolumeRootDir() +
                " for " + e.getMessage() + ", skip upgrade."));
        return result;
      }

      LOG.info("Start to upgrade containers on volume {}",
          hddsVolume.getVolumeRootDir());
      File[] containerTopDirs = currentDir.listFiles();
      if (containerTopDirs != null) {
        for (File containerTopDir : containerTopDirs) {
          try {
            final List<UpgradeContainerResult> results =
                upgradeSubContainerDir(containerTopDir);
            resultList.addAll(results);
          } catch (IOException e) {
            result.fail(e);
            return result;
          }
        }
      }

      result.setResultList(resultList);
      result.success();
      return result;
    }).whenComplete((r, e) -> {
      final File hddsRootDir = r.getHddsVolume().getHddsRootDir();
      final File file =
          UpgradeUtils.getVolumeUpgradeCompleteFile(r.getHddsVolume());
      // create a flag file
      if (e == null && r.isSuccess()) {
        try {
          UpgradeUtils.createFile(file);
        } catch (IOException ioe) {
          LOG.warn("Failed to create upgrade complete file {}.", file, ioe);
        }
      }
      if (lockFile.exists()) {
        boolean deleted = lockFile.delete();
        if (!deleted) {
          LOG.warn("Failed to delete upgrade lock file {}.", file);
        }
      }
    });
  }

  private List<UpgradeContainerResult> upgradeSubContainerDir(
      File containerTopDir) throws IOException {
    List<UpgradeContainerResult> resultList = new ArrayList<>();
    if (containerTopDir.isDirectory()) {
      File[] containerDirs = containerTopDir.listFiles();
      if (containerDirs != null) {
        for (File containerDir : containerDirs) {
          final ContainerData containerData = parseContainerData(containerDir);
          if (containerData != null &&
              ((KeyValueContainerData) containerData)
                  .hasSchema(OzoneConsts.SCHEMA_V2)) {
            final UpgradeContainerResult result =
                new UpgradeContainerResult(containerData);
            upgradeContainer(containerData, result);
            resultList.add(result);
          }
        }
      }
    }
    return resultList;
  }

  private ContainerData parseContainerData(File containerDir) {
    try {
      File containerFile = ContainerUtils.getContainerFile(containerDir);
      long containerID = ContainerUtils.getContainerID(containerDir);
      if (!containerFile.exists()) {
        LOG.error("Missing .container file: {}.", containerDir);
        return null;
      }
      try {
        ContainerData containerData =
            ContainerDataYaml.readContainerFile(containerFile);
        if (containerID != containerData.getContainerID()) {
          LOG.error("ContainerID in file {} mismatch with expected {}.",
              containerFile, containerID);
          return null;
        }
        if (containerData.getContainerType().equals(
            ContainerProtos.ContainerType.KeyValueContainer) &&
            containerData instanceof KeyValueContainerData) {
          KeyValueContainerData kvContainerData =
              (KeyValueContainerData) containerData;
          containerData.setVolume(hddsVolume);
          KeyValueContainerUtil.parseKVContainerData(kvContainerData, config);
          return kvContainerData;
        } else {
          LOG.error("Container is not KeyValueContainer type: {}.",
              containerDir);
          return null;
        }
      } catch (IOException ex) {
        LOG.error("Failed to parse ContainerFile: {}.", containerFile, ex);
        return null;
      }
    } catch (Throwable e) {
      LOG.error("Failed to load container: {}.", containerDir, e);
      return null;
    }
  }

  private void upgradeContainer(ContainerData containerData,
      UpgradeContainerResult result) throws IOException {
    final DBStore targetDBStore = dataStore.getStore();

    // open container schema v2 rocksdb
    final DatanodeStore dbStore = BlockUtils
        .getUncachedDatanodeStore((KeyValueContainerData) containerData, config,
            true);
    final DBStore sourceDBStore = dbStore.getStore();

    long total = 0L;
    for (String tableName : COLUMN_FAMILIES_NAME) {
      total += transferTableData(targetDBStore, sourceDBStore, tableName,
          containerData);
    }

    rewriteAndBackupContainerDataFile(containerData, result);
    result.success(total);
  }

  private long transferTableData(DBStore targetDBStore,
      DBStore sourceDBStore, String tableName, ContainerData containerData)
      throws IOException {
    final Table<byte[], byte[]> deleteTransactionTable =
        sourceDBStore.getTable(tableName);
    final Table<byte[], byte[]> targetDeleteTransactionTable =
        targetDBStore.getTable(tableName);
    return transferTableData(targetDeleteTransactionTable,
        deleteTransactionTable, containerData);
  }

  private long transferTableData(Table<byte[], byte[]> targetTable,
      Table<byte[], byte[]> sourceTable, ContainerData containerData)
      throws IOException {
    long count = 0;
    try (TableIterator<byte[], ? extends Table.KeyValue<byte[], byte[]>>
             iter = sourceTable.iterator()) {
      while (iter.hasNext()) {
        count++;
        Table.KeyValue<byte[], byte[]> next = iter.next();
        String key = DatanodeSchemaThreeDBDefinition
            .getContainerKeyPrefix(containerData.getContainerID())
            + StringUtils.bytes2String(next.getKey());
        targetTable
            .put(FixedLengthStringCodec.string2Bytes(key), next.getValue());
      }
    }
    return count;
  }

  private void rewriteAndBackupContainerDataFile(ContainerData containerData,
      UpgradeContainerResult result) throws IOException {
    if (containerData instanceof KeyValueContainerData) {
      final KeyValueContainerData keyValueContainerData =
          (KeyValueContainerData) containerData;

      final KeyValueContainerData copyContainerData =
          new KeyValueContainerData(keyValueContainerData);

      copyContainerData.setSchemaVersion(OzoneConsts.SCHEMA_V3);
      copyContainerData.setState(keyValueContainerData.getState());
      copyContainerData.setVolume(keyValueContainerData.getVolume());

      final File originContainerFile = KeyValueContainer
          .getContainerFile(keyValueContainerData.getMetadataPath(),
              keyValueContainerData.getContainerID());

      final File bakFile = new File(keyValueContainerData.getMetadataPath(),
          keyValueContainerData.getContainerID() +
              BACKUP_CONTAINER_DATA_FILE_SUFFIX);

      // backup v2 container data file
      NativeIO.renameTo(originContainerFile, bakFile);
      result.setBackupContainerFilePath(bakFile.getAbsolutePath());

      // gen new v3 container data file
      ContainerDataYaml.createContainerFile(
          ContainerProtos.ContainerType.KeyValueContainer,
          copyContainerData, originContainerFile);

      result.setNewContainerData(copyContainerData);
      result.setNewContainerFilePath(originContainerFile.getAbsolutePath());
    }
  }

  public File getVolumeDBPath(HddsVolume volume) throws IOException {
    File clusterIdDir = new File(volume.getStorageDir(), volume.getClusterID());
    File storageIdDir = new File(clusterIdDir, volume.getStorageID());
    File containerDBPath = new File(storageIdDir, CONTAINER_DB_NAME);
    if (containerDBPath.exists() && containerDBPath.isDirectory()) {
      return containerDBPath;
    } else {
      throw new IOException("DB " + containerDBPath +
          " doesn't exist or is not a directory");
    }
  }

  public void dbBackup(File dbPath) throws IOException {
    final File backup = new File(dbPath.getParentFile(),
        new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date()) +
            "-" + dbPath.getName() + ".backup");
    if (backup.exists()) {
      throw new IOException("Backup dir " + backup + "already exists");
    } else {
      FileUtils.copyDirectory(dbPath, backup, true);
      System.out.println("DB " + dbPath + " is backup to " + backup);
    }
  }

  /**
   * This class represents upgrade v2 to v3 container result.
   */
  public static class UpgradeContainerResult {
    private final ContainerData originContainerData;
    private ContainerData newContainerData;
    private long totalRow = 0L;
    private final long startTimeMs = System.currentTimeMillis();
    private long endTimeMs = 0L;
    private Status status;

    private String backupContainerFilePath;
    private String newContainerFilePath;

    public UpgradeContainerResult(
        ContainerData originContainerData) {
      this.originContainerData = originContainerData;
      this.status = Status.FAIL;
    }

    public long getTotalRow() {
      return totalRow;
    }

    public Status getStatus() {
      return status;
    }

    public void setNewContainerData(
        ContainerData newContainerData) {
      this.newContainerData = newContainerData;
    }

    public long getCostMs() {
      return endTimeMs - startTimeMs;
    }

    public ContainerData getOriginContainerData() {
      return originContainerData;
    }

    public ContainerData getNewContainerData() {
      return newContainerData;
    }

    public void setBackupContainerFilePath(String backupContainerFilePath) {
      this.backupContainerFilePath = backupContainerFilePath;
    }

    public void setNewContainerFilePath(String newContainerFilePath) {
      this.newContainerFilePath = newContainerFilePath;
    }

    public void success(long rowCount) {
      this.totalRow = rowCount;
      this.endTimeMs = System.currentTimeMillis();
      this.status = Status.SUCCESS;
    }

    @Override
    public String toString() {
      final StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("Result:{");
      stringBuilder.append("containerID=");
      stringBuilder.append(originContainerData.getContainerID());
      stringBuilder.append(", originContainerSchemaVersion=");
      stringBuilder.append(
          ((KeyValueContainerData) originContainerData).getSchemaVersion());

      if (newContainerData != null) {
        stringBuilder.append(", schemaV2ContainerFileBackupPath=");
        stringBuilder.append(backupContainerFilePath);

        stringBuilder.append(", newContainerSchemaVersion=");
        stringBuilder.append(
            ((KeyValueContainerData) newContainerData).getSchemaVersion());

        stringBuilder.append(", schemaV3ContainerFilePath=");
        stringBuilder.append(newContainerFilePath);
      }
      stringBuilder.append(", totalRow=");
      stringBuilder.append(totalRow);
      stringBuilder.append(", costMs=");
      stringBuilder.append(getCostMs());
      stringBuilder.append(", status=");
      stringBuilder.append(status);
      stringBuilder.append("}");
      return stringBuilder.toString();
    }

    enum Status {
      SUCCESS,
      FAIL
    }
  }
}
