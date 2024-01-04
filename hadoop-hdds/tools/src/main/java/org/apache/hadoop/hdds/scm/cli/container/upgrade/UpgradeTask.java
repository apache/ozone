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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * This class implements the v2 to v3 container upgrade process.
 */
public class UpgradeTask {

  public static final Logger LOG =
      LoggerFactory.getLogger(UpgradeTask.class);

  private final ConfigurationSource config;
  private final HddsVolume hddsVolume;
  private final DatanodeStoreSchemaThreeImpl datanodeStoreSchemaThree;

  private static final String BACKUP_CONTAINER_DATA_FILE_SUFFIX = ".backup";
  private static final Set<String> COLUMN_FAMILIES_NAME =
      (new DatanodeSchemaTwoDBDefinition("", new OzoneConfiguration()))
          .getMap().keySet();

  public UpgradeTask(ConfigurationSource config,
      HddsVolume hddsVolume,
      DatanodeStoreSchemaThreeImpl datanodeStoreSchemaThree) {
    this.config = config;
    this.hddsVolume = hddsVolume;
    this.datanodeStoreSchemaThree = datanodeStoreSchemaThree;
  }

  public CompletableFuture<UpgradeManager.Result> getUpgradeFutureByVolume() {
    return CompletableFuture.supplyAsync(() -> {

      final UpgradeManager.Result result =
          new UpgradeManager.Result(hddsVolume);

      List<UpgradeContainerResult> resultList = new ArrayList<>();
      final File hddsVolumeRootDir = hddsVolume.getHddsRootDir();

      Preconditions.checkNotNull(hddsVolumeRootDir, "hddsVolumeRootDir" +
          "cannot be null");

      //filtering storage directory
      File[] storageDirs = hddsVolumeRootDir.listFiles(File::isDirectory);

      if (storageDirs == null) {
        result.fail(new Exception(
            "Storage dir not found for the volume " + hddsVolumeRootDir +
                ", skipped upgrade."));
        return result;
      }

      if (storageDirs.length > 0) {
        File clusterIDDir = new File(hddsVolume.getStorageDir(),
            hddsVolume.getClusterID());
        File idDir = clusterIDDir;
        if (storageDirs.length == 1 && !clusterIDDir.exists()) {
          idDir = storageDirs[0];
        } else {
          if (!clusterIDDir.exists()) {
            result.fail(new Exception("Volume " + hddsVolumeRootDir +
                " is in an inconsistent state. Expected " +
                "clusterID directory " + clusterIDDir + " not found."));
            return result;
          }
        }

        LOG.info("Start to upgrade containers on volume {}", hddsVolumeRootDir);
        File currentDir = new File(idDir, Storage.STORAGE_DIR_CURRENT);

        if (!currentDir.exists()) {
          result.fail(new Exception(
              "Storage current dir not found for the volume " +
                  currentDir + ", skipped upgrade."));
          return result;
        }
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
      } else {
        result.fail(new Exception(
            "Storage dir not found for the volume " + hddsVolumeRootDir +
                ", skipped upgrade."));
        return result;
      }

      result.setResultList(resultList);
      result.success();
      return result;
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
      File containerFile = ContainerUtils.getContainerFile(
          containerDir);
      long containerID =
          ContainerUtils.getContainerID(containerDir);
      if (containerFile.exists()) {
        try {
          ContainerData containerData =
              ContainerDataYaml.readContainerFile(
                  containerFile);
          if (containerID != containerData.getContainerID()) {
            LOG.error("Invalid ContainerID in file {}. " +
                    "Skipping loading of this container.",
                containerFile);
            return null;
          }
          if (containerData.getContainerType().equals(
              ContainerProtos.ContainerType.KeyValueContainer) &&
              containerData instanceof KeyValueContainerData) {
            KeyValueContainerData kvContainerData =
                (KeyValueContainerData)
                    containerData;
            containerData.setVolume(hddsVolume);
            KeyValueContainerUtil
                .parseKVContainerData(kvContainerData, config);

            return kvContainerData;
          } else {
            LOG.error("Only KeyValueContainer support. ");
            return null;
          }

        } catch (IOException ex) {
          LOG.error(
              "Failed to parse ContainerFile for ContainerID: {}",
              containerID, ex);
          return null;
        }
      } else {
        LOG.error("Missing .container file for ContainerID: {}",
            containerDir.getName());
        return null;
      }
    } catch (Throwable e) {
      LOG.error("Failed to load container from {}",
          containerDir.getAbsolutePath(), e);
      return null;
    }
  }

  private void upgradeContainer(ContainerData containerData,
      UpgradeContainerResult result) throws IOException {
    final DBStore targetDBStore = datanodeStoreSchemaThree.getStore();

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
      stringBuilder.append("Result{");
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
