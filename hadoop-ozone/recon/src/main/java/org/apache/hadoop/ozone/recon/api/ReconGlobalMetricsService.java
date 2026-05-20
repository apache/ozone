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

package org.apache.hadoop.ozone.recon.api;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.MULTIPART_INFO_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.KeyEntityInfo;
import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconGlobalStatsManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.tasks.GlobalStatsValue;
import org.apache.hadoop.ozone.recon.tasks.OmTableInsightTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for getting global storage metric values.
 */
@Singleton
public class ReconGlobalMetricsService {

  private final ReconGlobalStatsManager reconGlobalStatsManager;
  private final ReconOMMetadataManager omMetadataManager;
  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private static final Logger LOG =
      LoggerFactory.getLogger(ReconGlobalMetricsService.class);

  @Inject
  public ReconGlobalMetricsService(ReconGlobalStatsManager reconGlobalStatsManager,
                                   ReconOMMetadataManager omMetadataManager,
                                   ReconNamespaceSummaryManager reconNamespaceSummaryManager) {
    this.reconGlobalStatsManager = reconGlobalStatsManager;
    this.omMetadataManager = omMetadataManager;
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
  }

  public Map<String, Long> getOpenKeySummary() {
    Map<String, Long> keysSummary = new HashMap<>();
    try {
      Long replicatedSizeOpenKey = getValueFromId(reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getReplicatedSizeKeyFromTable(OPEN_KEY_TABLE)));
      Long replicatedSizeOpenFile = getValueFromId(reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getReplicatedSizeKeyFromTable(OPEN_FILE_TABLE)));
      Long unreplicatedSizeOpenKey = getValueFromId(reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getUnReplicatedSizeKeyFromTable(OPEN_KEY_TABLE)));
      Long unreplicatedSizeOpenFile = getValueFromId(reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getUnReplicatedSizeKeyFromTable(OPEN_FILE_TABLE)));
      Long openKeyCountForKeyTable = getValueFromId(reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getTableCountKeyFromTable(OPEN_KEY_TABLE)));
      Long openKeyCountForFileTable = getValueFromId(reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getTableCountKeyFromTable(OPEN_FILE_TABLE)));

      // Calculate the total number of open keys
      keysSummary.put("totalOpenKeys",
          openKeyCountForKeyTable + openKeyCountForFileTable);
      // Calculate the total replicated and unreplicated sizes
      keysSummary.put("totalReplicatedDataSize",
          replicatedSizeOpenKey + replicatedSizeOpenFile);
      keysSummary.put("totalUnreplicatedDataSize",
          unreplicatedSizeOpenKey + unreplicatedSizeOpenFile);
    } catch (IOException e) {
      LOG.error("Error retrieving open key summary from RocksDB", e);
      // Return zeros in case of error
      keysSummary.put("totalOpenKeys", 0L);
      keysSummary.put("totalReplicatedDataSize", 0L);
      keysSummary.put("totalUnreplicatedDataSize", 0L);
    }
    return keysSummary;
  }

  public Map<String, Long> getDeletedKeySummary() {
    Map<String, Long> keysSummary = new HashMap<>();
    try {
      // Fetch the necessary metrics for deleted keys
      Long replicatedSizeDeleted = getValueFromId(reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getReplicatedSizeKeyFromTable(DELETED_TABLE)));
      Long unreplicatedSizeDeleted = getValueFromId(reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getUnReplicatedSizeKeyFromTable(DELETED_TABLE)));
      Long deletedKeyCount = getValueFromId(reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getTableCountKeyFromTable(DELETED_TABLE)));

      // Calculate the total number of deleted keys
      keysSummary.put("totalDeletedKeys", deletedKeyCount);
      // Calculate the total replicated and unreplicated sizes
      keysSummary.put("totalReplicatedDataSize", replicatedSizeDeleted);
      keysSummary.put("totalUnreplicatedDataSize", unreplicatedSizeDeleted);
    } catch (IOException e) {
      LOG.error("Error retrieving deleted key summary from RocksDB", e);
      // Return zeros in case of error
      keysSummary.put("totalDeletedKeys", 0L);
      keysSummary.put("totalReplicatedDataSize", 0L);
      keysSummary.put("totalUnreplicatedDataSize", 0L);
    }
    return keysSummary;
  }

  public Map<String, Long> getMPUKeySummary() {
    Map<String, Long> keysSummary = new HashMap<>();
    try {
      Long replicatedSizeOpenMPUKey = getValueFromId(reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getReplicatedSizeKeyFromTable(MULTIPART_INFO_TABLE)));
      Long unreplicatedSizeOpenMPUKey = getValueFromId(reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getUnReplicatedSizeKeyFromTable(MULTIPART_INFO_TABLE)));
      Long openMPUKeyCount = getValueFromId(reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getTableCountKeyFromTable(MULTIPART_INFO_TABLE)));
      // Calculate the total number of open MPU keys
      keysSummary.put("totalOpenMPUKeys", openMPUKeyCount);
      // Calculate the total replicated and unreplicated sizes of open MPU keys
      keysSummary.put("totalReplicatedDataSize", replicatedSizeOpenMPUKey);
      keysSummary.put("totalDataSize", unreplicatedSizeOpenMPUKey);
    } catch (IOException ex) {
      LOG.error("Error retrieving open mpu key summary from RocksDB", ex);
      // Return zeros in case of error
      keysSummary.put("totalOpenMPUKeys", 0L);
      // Calculate the total replicated and unreplicated sizes of open MPU keys
      keysSummary.put("totalReplicatedDataSize", 0L);
      keysSummary.put("totalDataSize", 0L);
    }
    return keysSummary;
  }

  public KeyInsightInfoResponse getPendingForDeletionDirInfo(int limit, String prevKey) {
    KeyInsightInfoResponse deletedDirInsightInfo = new KeyInsightInfoResponse();
    List<KeyEntityInfo> deletedDirInfoList = deletedDirInsightInfo.getDeletedDirInfoList();
    Table<String, OmKeyInfo> deletedDirTable = omMetadataManager.getDeletedDirTable();
    if (deletedDirTable == null) {
      return deletedDirInsightInfo;
    }
    try (
        TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
            keyIter = deletedDirTable.iterator()) {
      boolean skipPrevKey = false;
      String lastKey = "";
      if (isNotBlank(prevKey)) {
        skipPrevKey = true;
        Table.KeyValue<String, OmKeyInfo> seekKeyValue =
            keyIter.seek(prevKey);
        // check if RocksDB was able to seek correctly to the given key prefix
        // if not, then return empty result
        // In case of an empty prevKeyPrefix; all the keys are returned
        if (seekKeyValue == null ||
            (isNotBlank(prevKey) &&
                !seekKeyValue.getKey().equals(prevKey))) {
          return deletedDirInsightInfo;
        }
      }
      while (keyIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
        String key = kv.getKey();
        lastKey = key;
        OmKeyInfo omKeyInfo = kv.getValue();
        // skip the prev key if a prev key is present
        if (skipPrevKey && key.equals(prevKey)) {
          continue;
        }
        KeyEntityInfo keyEntityInfo = new KeyEntityInfo();
        keyEntityInfo.setIsKey(omKeyInfo.isFile());
        keyEntityInfo.setKey(omKeyInfo.getFileName());
        keyEntityInfo.setPath(createPath(omKeyInfo));
        keyEntityInfo.setInStateSince(omKeyInfo.getCreationTime());
        Pair<Long, Long> sizeInfo = fetchSizeForDeletedDirectory(omKeyInfo.getObjectID());
        keyEntityInfo.setSize(sizeInfo.getLeft());
        keyEntityInfo.setReplicatedSize(sizeInfo.getRight());
        keyEntityInfo.setReplicationConfig(omKeyInfo.getReplicationConfig());
        deletedDirInsightInfo.setUnreplicatedDataSize(
            deletedDirInsightInfo.getUnreplicatedDataSize() +
                keyEntityInfo.getSize());
        deletedDirInsightInfo.setReplicatedDataSize(
            deletedDirInsightInfo.getReplicatedDataSize() +
                keyEntityInfo.getReplicatedSize());
        deletedDirInfoList.add(keyEntityInfo);
        if (limit > 0 && deletedDirInfoList.size() == limit) {
          break;
        }
      }
      deletedDirInsightInfo.setLastKey(lastKey);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
    } catch (Exception ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return deletedDirInsightInfo;
  }

  public Map<String, Long> calculatePendingSizes() {
    Map<String, Long> result = new HashMap<>();
    long pendingDirectorySize = -1L;
    long pendingKeySizeValue = -1L;

    //Getting pending deletion directory size
    try {
      KeyInsightInfoResponse response = getPendingForDeletionDirInfo(-1, "");
      pendingDirectorySize = response.getReplicatedDataSize();
    } catch (Exception ex) {
      LOG.error("Error calculating pending directory size", ex);
    }
    result.put("pendingDirectorySize", pendingDirectorySize);

    //Getting pending deletion key size
    try {
      Map<String, Long> pendingKeySizeMap = getDeletedKeySummary();
      pendingKeySizeValue = pendingKeySizeMap.getOrDefault("totalReplicatedDataSize", 0L);
    } catch (Exception ex) {
      LOG.error("Error calculating pending key size", ex);
    }
    result.put("pendingKeySize", pendingKeySizeValue);

    if (pendingDirectorySize < 0 || pendingKeySizeValue < 0) {
      result.put("totalSize", -1L);
    } else {
      result.put("totalSize", pendingDirectorySize + pendingKeySizeValue);
    }
    return result;
  }

  private Long getValueFromId(GlobalStatsValue record) {
    // If the record is null, return 0
    return record != null ? record.getValue() : 0L;
  }

  private Pair<Long, Long> fetchSizeForDeletedDirectory(long objectId)
      throws IOException {
    NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    if (nsSummary != null) {
      return Pair.of(nsSummary.getSizeOfFiles(), nsSummary.getReplicatedSizeOfFiles());
    }
    return Pair.of(0L, 0L);
  }

  private String createPath(OmKeyInfo omKeyInfo) {
    return omKeyInfo.getVolumeName() + OM_KEY_PREFIX +
        omKeyInfo.getBucketName() + OM_KEY_PREFIX + omKeyInfo.getKeyName();
  }
}
