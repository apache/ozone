/**
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

package org.apache.hadoop.ozone.recon.api;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.KeyEntityInfo;
import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconNamespaceSummaryManagerImpl;
import org.apache.hadoop.ozone.recon.tasks.OmTableInsightTask;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.hadoop.ozone.recon.schema.tables.pojos.GlobalStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_FETCH_COUNT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_PREVKEY;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_OPEN_KEY_INCLUDE_FSO;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_OPEN_KEY_INCLUDE_NON_FSO;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OPEN_KEY_INCLUDE_FSO;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OPEN_KEY_INCLUDE_NON_FSO;


/**
 * Endpoint to get following key level info under OM DB Insight page of Recon.
 * 1. Number of open keys for Legacy/OBS buckets.
 * 2. Number of open files for FSO buckets.
 * 3. Amount of data mapped to open keys and open files.
 * 4. Number of pending delete keys in legacy/OBS buckets and pending
 * delete files in FSO buckets.
 * 5. Amount of data mapped to pending delete keys in legacy/OBS buckets and
 * pending delete files in FSO buckets.
 */
@Path("/keys")
@Produces(MediaType.APPLICATION_JSON)
@AdminOnly
public class OMDBInsightEndpoint {

  @Inject
  private ContainerEndpoint containerEndpoint;
  @Inject
  private ReconContainerMetadataManager reconContainerMetadataManager;
  private final ReconOMMetadataManager omMetadataManager;
  private final ReconContainerManager containerManager;
  private static final Logger LOG =
      LoggerFactory.getLogger(OMDBInsightEndpoint.class);
  private final GlobalStatsDao globalStatsDao;
  private ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager;


  @Inject
  public OMDBInsightEndpoint(OzoneStorageContainerManager reconSCM,
                             ReconOMMetadataManager omMetadataManager,
                             GlobalStatsDao globalStatsDao,
                             ReconNamespaceSummaryManagerImpl
                                 reconNamespaceSummaryManager) {
    this.containerManager =
        (ReconContainerManager) reconSCM.getContainerManager();
    this.omMetadataManager = omMetadataManager;
    this.globalStatsDao = globalStatsDao;
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
  }

  /**
   * This method retrieves set of keys/files which are open.
   *
   * @return the http json response wrapped in below format:
   *
   * {
   *   "lastKey": "/-4611686018427388160/-9223372036854775552/-922777620354",
   *   "replicatedTotal": 2147483648,
   *   "unreplicatedTotal": 2147483648,
   *   "fso": [
   *     {
   *       "key": "/-4611686018427388160/-9223372036/-922337203977722380527",
   *       "path": "239",
   *       "inStateSince": 1686156886632,
   *       "size": 268435456,
   *       "replicatedSize": 268435456,
   *       "replicationInfo": {
   *         "replicationFactor": "ONE",
   *         "requiredNodes": 1,
   *         "replicationType": "RATIS"
   *       }
   *     },
   *     {
   *       "key": "/-4611686018427388160/-9223372036854775552/0397777586240",
   *       "path": "244",
   *       "inStateSince": 1686156887186,
   *       "size": 268435456,
   *       "replicatedSize": 268435456,
   *       "replicationInfo": {
   *         "replicationFactor": "ONE",
   *         "requiredNodes": 1,
   *         "replicationType": "RATIS"
   *       }
   *     }
   *   ],
   *   "nonFSO": [
   *     {
   *       "key": "/vol1/bucket1/object1",
   *       "path": "239",
   *       "inStateSince": 1686156886632,
   *       "size": 268435456,
   *       "replicatedSize": 268435456,
   *       "replicationInfo": {
   *         "replicationFactor": "ONE",
   *         "requiredNodes": 1,
   *         "replicationType": "RATIS"
   *       }
   *     }
   *   ],
   *   "status": "OK"
   * }
   */

  @GET
  @Path("/open")
  public Response getOpenKeyInfo(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
          int limit,
      @DefaultValue(StringUtils.EMPTY) @QueryParam(RECON_QUERY_PREVKEY)
          String prevKey,
      @DefaultValue(DEFAULT_OPEN_KEY_INCLUDE_FSO)
      @QueryParam(RECON_OPEN_KEY_INCLUDE_FSO)
          boolean includeFso,
      @DefaultValue(DEFAULT_OPEN_KEY_INCLUDE_NON_FSO)
      @QueryParam(RECON_OPEN_KEY_INCLUDE_NON_FSO)
          boolean includeNonFso) {
    KeyInsightInfoResponse openKeyInsightInfo = new KeyInsightInfoResponse();
    List<KeyEntityInfo> nonFSOKeyInfoList =
        openKeyInsightInfo.getNonFSOKeyInfoList();

    boolean skipPrevKeyDone = false;
    boolean isLegacyBucketLayout = true;
    boolean recordsFetchedLimitReached = false;

    String lastKey = "";
    List<KeyEntityInfo> fsoKeyInfoList = openKeyInsightInfo.getFsoKeyInfoList();
    for (BucketLayout layout : Arrays.asList(
        BucketLayout.LEGACY, BucketLayout.FILE_SYSTEM_OPTIMIZED)) {
      isLegacyBucketLayout = (layout == BucketLayout.LEGACY);
      // Skip bucket iteration based on parameters includeFso and includeNonFso
      if ((!includeFso && !isLegacyBucketLayout) ||
          (!includeNonFso && isLegacyBucketLayout)) {
        continue;
      }

      Table<String, OmKeyInfo> openKeyTable =
          omMetadataManager.getOpenKeyTable(layout);
      try (
          TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
              keyIter = openKeyTable.iterator()) {
        boolean skipPrevKey = false;
        String seekKey = prevKey;
        if (!skipPrevKeyDone && StringUtils.isNotBlank(prevKey)) {
          skipPrevKey = true;
          Table.KeyValue<String, OmKeyInfo> seekKeyValue =
              keyIter.seek(seekKey);
          // check if RocksDB was able to seek correctly to the given key prefix
          // if not, then return empty result
          // In case of an empty prevKeyPrefix, all the keys are returned
          if (seekKeyValue == null ||
              (StringUtils.isNotBlank(prevKey) &&
                  !seekKeyValue.getKey().equals(prevKey))) {
            continue;
          }
        }
        while (keyIter.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
          String key = kv.getKey();
          lastKey = key;
          OmKeyInfo omKeyInfo = kv.getValue();
          // skip the prev key if prev key is present
          if (skipPrevKey && key.equals(prevKey)) {
            skipPrevKeyDone = true;
            continue;
          }
          KeyEntityInfo keyEntityInfo = new KeyEntityInfo();
          keyEntityInfo.setKey(key);
          keyEntityInfo.setPath(omKeyInfo.getKeyName());
          keyEntityInfo.setInStateSince(omKeyInfo.getCreationTime());
          keyEntityInfo.setSize(omKeyInfo.getDataSize());
          keyEntityInfo.setReplicatedSize(omKeyInfo.getReplicatedSize());
          keyEntityInfo.setReplicationConfig(omKeyInfo.getReplicationConfig());
          openKeyInsightInfo.setUnreplicatedDataSize(
              openKeyInsightInfo.getUnreplicatedDataSize() +
                  keyEntityInfo.getSize());
          openKeyInsightInfo.setReplicatedDataSize(
              openKeyInsightInfo.getReplicatedDataSize() +
                  keyEntityInfo.getReplicatedSize());
          boolean added =
              isLegacyBucketLayout ? nonFSOKeyInfoList.add(keyEntityInfo) :
                  fsoKeyInfoList.add(keyEntityInfo);
          if ((nonFSOKeyInfoList.size() + fsoKeyInfoList.size()) == limit) {
            recordsFetchedLimitReached = true;
            break;
          }
        }
      } catch (IOException ex) {
        throw new WebApplicationException(ex,
            Response.Status.INTERNAL_SERVER_ERROR);
      } catch (IllegalArgumentException e) {
        throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
      } catch (Exception ex) {
        throw new WebApplicationException(ex,
            Response.Status.INTERNAL_SERVER_ERROR);
      }
      if (recordsFetchedLimitReached) {
        break;
      }
    }

    openKeyInsightInfo.setLastKey(lastKey);
    return Response.ok(openKeyInsightInfo).build();
  }

  /**
   * Retrieves the summary of open keys.
   *
   * This method calculates and returns a summary of open keys.
   *
   * @return The HTTP  response body includes a map with the following entries:
   * - "totalOpenKeys": the total number of open keys
   * - "totalReplicatedDataSize": the total replicated size for open keys
   * - "totalUnreplicatedDataSize": the total unreplicated size for open keys
   *
   *
   * Example response:
   *   {
   *    "totalOpenKeys": 8,
   *    "totalReplicatedDataSize": 90000,
   *    "totalUnreplicatedDataSize": 30000
   *   }
   */
  @GET
  @Path("/open/summary")
  public Response getOpenKeySummary() {
    // Create a HashMap for the keysSummary
    Map<String, Long> keysSummary = new HashMap<>();
    // Create a keys summary for open keys
    createKeysSummaryForOpenKey(keysSummary);
    return Response.ok(keysSummary).build();
  }

  /**
   * Creates a keys summary for open keys and updates the provided
   * keysSummary map. Calculates the total number of open keys, replicated
   * data size, and unreplicated data size.
   *
   * @param keysSummary A map to store the keys summary information.
   */
  private void createKeysSummaryForOpenKey(
      Map<String, Long> keysSummary) {
    Long replicatedSizeOpenKey = getValueFromId(globalStatsDao.findById(
        OmTableInsightTask.getReplicatedSizeKeyFromTable(OPEN_KEY_TABLE)));
    Long replicatedSizeOpenFile = getValueFromId(globalStatsDao.findById(
        OmTableInsightTask.getReplicatedSizeKeyFromTable(OPEN_FILE_TABLE)));
    Long unreplicatedSizeOpenKey = getValueFromId(globalStatsDao.findById(
        OmTableInsightTask.getUnReplicatedSizeKeyFromTable(OPEN_KEY_TABLE)));
    Long unreplicatedSizeOpenFile = getValueFromId(globalStatsDao.findById(
        OmTableInsightTask.getUnReplicatedSizeKeyFromTable(OPEN_FILE_TABLE)));
    Long openKeyCountForKeyTable = getValueFromId(globalStatsDao.findById(
        OmTableInsightTask.getTableCountKeyFromTable(OPEN_KEY_TABLE)));
    Long openKeyCountForFileTable = getValueFromId(globalStatsDao.findById(
        OmTableInsightTask.getTableCountKeyFromTable(OPEN_FILE_TABLE)));

    // Calculate the total number of open keys
    keysSummary.put("totalOpenKeys",
        openKeyCountForKeyTable + openKeyCountForFileTable);
    // Calculate the total replicated and unreplicated sizes
    keysSummary.put("totalReplicatedDataSize",
        replicatedSizeOpenKey + replicatedSizeOpenFile);
    keysSummary.put("totalUnreplicatedDataSize",
        unreplicatedSizeOpenKey + unreplicatedSizeOpenFile);

  }

  private Long getValueFromId(GlobalStats record) {
    // If the record is null, return 0
    return record != null ? record.getValue() : 0L;
  }

  private void getPendingForDeletionKeyInfo(
      int limit,
      String prevKey,
      KeyInsightInfoResponse deletedKeyAndDirInsightInfo) {
    List<RepeatedOmKeyInfo> repeatedOmKeyInfoList =
        deletedKeyAndDirInsightInfo.getRepeatedOmKeyInfoList();
    Table<String, RepeatedOmKeyInfo> deletedTable =
        omMetadataManager.getDeletedTable();
    try (
        TableIterator<String, ? extends Table.KeyValue<String,
            RepeatedOmKeyInfo>>
            keyIter = deletedTable.iterator()) {
      boolean skipPrevKey = false;
      String seekKey = prevKey;
      String lastKey = "";
      if (StringUtils.isNotBlank(prevKey)) {
        skipPrevKey = true;
        Table.KeyValue<String, RepeatedOmKeyInfo> seekKeyValue =
            keyIter.seek(seekKey);
        // check if RocksDB was able to seek correctly to the given key prefix
        // if not, then return empty result
        // In case of an empty prevKeyPrefix, all the keys are returned
        if (seekKeyValue == null ||
            (StringUtils.isNotBlank(prevKey) &&
                !seekKeyValue.getKey().equals(prevKey))) {
          return;
        }
      }
      while (keyIter.hasNext()) {
        Table.KeyValue<String, RepeatedOmKeyInfo> kv = keyIter.next();
        String key = kv.getKey();
        lastKey = key;
        RepeatedOmKeyInfo repeatedOmKeyInfo = kv.getValue();
        // skip the prev key if prev key is present
        if (skipPrevKey && key.equals(prevKey)) {
          continue;
        }
        updateReplicatedAndUnReplicatedTotal(deletedKeyAndDirInsightInfo,
            repeatedOmKeyInfo);
        repeatedOmKeyInfoList.add(repeatedOmKeyInfo);
        if ((repeatedOmKeyInfoList.size()) == limit) {
          break;
        }
      }
      deletedKeyAndDirInsightInfo.setLastKey(lastKey);
    } catch (IOException ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
    } catch (Exception ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /** Retrieves the summary of deleted keys.
   *
   * This method calculates and returns a summary of deleted keys.
   *
   * @return The HTTP  response body includes a map with the following entries:
   * - "totalDeletedKeys": the total number of deleted keys
   * - "totalReplicatedDataSize": the total replicated size for deleted keys
   * - "totalUnreplicatedDataSize": the total unreplicated size for deleted keys
   *
   *
   * Example response:
   *   {
   *    "totalDeletedKeys": 8,
   *    "totalReplicatedDataSize": 90000,
   *    "totalUnreplicatedDataSize": 30000
   *   }
   */
  @GET
  @Path("/deletePending/summary")
  public Response getDeletedKeySummary() {
    // Create a HashMap for the keysSummary
    Map<String, Long> keysSummary = new HashMap<>();
    // Create a keys summary for deleted keys
    createKeysSummaryForDeletedKey(keysSummary);
    return Response.ok(keysSummary).build();
  }

  /**
   * This method retrieves set of keys/files pending for deletion.
   * <p>
   * limit - limits the number of key/files returned.
   * prevKey - E.g. /vol1/bucket1/key1, this will skip keys till it
   * seeks correctly to the given prevKey.
   * Sample API Response:
   * {
   *   "lastKey": "vol1/bucket1/key1",
   *   "replicatedTotal": -1530804718628866300,
   *   "unreplicatedTotal": -1530804718628866300,
   *   "deletedkeyinfo": [
   *     {
   *       "omKeyInfoList": [
   *         {
   *           "metadata": {},
   *           "objectID": 0,
   *           "updateID": 0,
   *           "parentObjectID": 0,
   *           "volumeName": "sampleVol",
   *           "bucketName": "bucketOne",
   *           "keyName": "key_one",
   *           "dataSize": -1530804718628866300,
   *           "keyLocationVersions": [],
   *           "creationTime": 0,
   *           "modificationTime": 0,
   *           "replicationConfig": {
   *             "replicationFactor": "ONE",
   *             "requiredNodes": 1,
   *             "replicationType": "STANDALONE"
   *           },
   *           "fileChecksum": null,
   *           "fileName": "key_one",
   *           "acls": [],
   *           "path": "0/key_one",
   *           "file": false,
   *           "latestVersionLocations": null,
   *           "replicatedSize": -1530804718628866300,
   *           "fileEncryptionInfo": null,
   *           "objectInfo": "OMKeyInfo{volume='sampleVol', bucket='bucketOne',
   *           key='key_one', dataSize='-1530804718628866186', creationTime='0',
   *           objectID='0', parentID='0', replication='STANDALONE/ONE',
   *           fileChecksum='null}",
   *           "updateIDset": false
   *         }
   *       ]
   *     }
   *   ],
   *   "status": "OK"
   * }
   */
  @GET
  @Path("/deletePending")
  public Response getDeletedKeyInfo(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
      int limit,
      @DefaultValue(StringUtils.EMPTY) @QueryParam(RECON_QUERY_PREVKEY)
      String prevKey) {
    KeyInsightInfoResponse
        deletedKeyInsightInfo = new KeyInsightInfoResponse();
    getPendingForDeletionKeyInfo(limit, prevKey,
        deletedKeyInsightInfo);
    return Response.ok(deletedKeyInsightInfo).build();
  }

  /**
   * Creates a keys summary for deleted keys and updates the provided
   * keysSummary map. Calculates the total number of deleted keys, replicated
   * data size, and unreplicated data size.
   *
   * @param keysSummary A map to store the keys summary information.
   */
  private void createKeysSummaryForDeletedKey(Map<String, Long> keysSummary) {
    // Fetch the necessary metrics for deleted keys
    Long replicatedSizeDeleted = getValueFromId(globalStatsDao.findById(
        OmTableInsightTask.getReplicatedSizeKeyFromTable(DELETED_TABLE)));
    Long unreplicatedSizeDeleted = getValueFromId(globalStatsDao.findById(
        OmTableInsightTask.getUnReplicatedSizeKeyFromTable(DELETED_TABLE)));
    Long deletedKeyCount = getValueFromId(globalStatsDao.findById(
        OmTableInsightTask.getTableCountKeyFromTable(DELETED_TABLE)));

    // Calculate the total number of deleted keys
    keysSummary.put("totalDeletedKeys", deletedKeyCount);
    // Calculate the total replicated and unreplicated sizes
    keysSummary.put("totalReplicatedDataSize", replicatedSizeDeleted);
    keysSummary.put("totalUnreplicatedDataSize", unreplicatedSizeDeleted);
  }


  private void getPendingForDeletionDirInfo(
      int limit, String prevKey,
      KeyInsightInfoResponse pendingForDeletionKeyInfo) {

    List<KeyEntityInfo> deletedDirInfoList =
        pendingForDeletionKeyInfo.getDeletedDirInfoList();

    Table<String, OmKeyInfo> deletedDirTable =
        omMetadataManager.getDeletedDirTable();
    try (
        TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
            keyIter = deletedDirTable.iterator()) {
      boolean skipPrevKey = false;
      String seekKey = prevKey;
      String lastKey = "";
      if (StringUtils.isNotBlank(prevKey)) {
        skipPrevKey = true;
        Table.KeyValue<String, OmKeyInfo> seekKeyValue =
            keyIter.seek(seekKey);
        // check if RocksDB was able to seek correctly to the given key prefix
        // if not, then return empty result
        // In case of an empty prevKeyPrefix, all the keys are returned
        if (seekKeyValue == null ||
            (StringUtils.isNotBlank(prevKey) &&
                !seekKeyValue.getKey().equals(prevKey))) {
          return;
        }
      }
      while (keyIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
        String key = kv.getKey();
        lastKey = key;
        OmKeyInfo omKeyInfo = kv.getValue();
        // skip the prev key if prev key is present
        if (skipPrevKey && key.equals(prevKey)) {
          continue;
        }
        KeyEntityInfo keyEntityInfo = new KeyEntityInfo();
        keyEntityInfo.setKey(key);
        keyEntityInfo.setPath(omKeyInfo.getKeyName());
        keyEntityInfo.setInStateSince(omKeyInfo.getCreationTime());
        keyEntityInfo.setSize(
            fetchSizeForDeletedDirectory(omKeyInfo.getObjectID()));
        keyEntityInfo.setReplicatedSize(omKeyInfo.getReplicatedSize());
        keyEntityInfo.setReplicationConfig(omKeyInfo.getReplicationConfig());
        pendingForDeletionKeyInfo.setUnreplicatedDataSize(
            pendingForDeletionKeyInfo.getUnreplicatedDataSize() +
                keyEntityInfo.getSize());
        pendingForDeletionKeyInfo.setReplicatedDataSize(
            pendingForDeletionKeyInfo.getReplicatedDataSize() +
                keyEntityInfo.getReplicatedSize());
        deletedDirInfoList.add(keyEntityInfo);
        if (deletedDirInfoList.size() == limit) {
          break;
        }
      }
      pendingForDeletionKeyInfo.setLastKey(lastKey);
    } catch (IOException ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
    } catch (Exception ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Given an object ID, return total data size (no replication)
   * under this object. Note:- This method is RECURSIVE.
   *
   * @param objectId the object's ID
   * @return total used data size in bytes
   * @throws IOException ioEx
   */
  protected long fetchSizeForDeletedDirectory(long objectId)
      throws IOException {
    NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    if (nsSummary == null) {
      return 0L;
    }
    long totalSize = nsSummary.getSizeOfFiles();
    for (long childId : nsSummary.getChildDir()) {
      totalSize += fetchSizeForDeletedDirectory(childId);
    }
    return totalSize;
  }

  /** This method retrieves set of directories pending for deletion.
   *
   * limit - limits the number of directories returned.
   * prevKey - E.g. /vol1/bucket1/bucket1/dir1, this will skip dirs till it
   * seeks correctly to the given prevKey.
   * Sample API Response:
   * {
   *   "lastKey": "vol1/bucket1/bucket1/dir1"
   *   "replicatedTotal": -1530804718628866300,
   *   "unreplicatedTotal": -1530804718628866300,
   *   "deletedkeyinfo": [
   *     {
   *       "omKeyInfoList": [
   *         {
   *           "metadata": {},
   *           "objectID": 0,
   *           "updateID": 0,
   *           "parentObjectID": 0,
   *           "volumeName": "sampleVol",
   *           "bucketName": "bucketOne",
   *           "keyName": "key_one",
   *           "dataSize": -1530804718628866300,
   *           "keyLocationVersions": [],
   *           "creationTime": 0,
   *           "modificationTime": 0,
   *           "replicationConfig": {
   *             "replicationFactor": "ONE",
   *             "requiredNodes": 1,
   *             "replicationType": "STANDALONE"
   *           },
   *           "fileChecksum": null,
   *           "fileName": "key_one",
   *           "acls": [],
   *           "path": "0/key_one",
   *           "file": false,
   *           "latestVersionLocations": null,
   *           "replicatedSize": -1530804718628866300,
   *           "fileEncryptionInfo": null,
   *           "objectInfo": "OMKeyInfo{volume='sampleVol', bucket='bucketOne',
   *           key='key_one', dataSize='-1530804718628866186', creationTime='0',
   *           objectID='0', parentID='0', replication='STANDALONE/ONE',
   *           fileChecksum='null}",
   *           "updateIDset": false
   *         }
   *       ]
   *     }
   *   ],
   *   "status": "OK"
   * }
   */
  @GET
  @Path("/deletePending/dirs")
  public Response getDeletedDirInfo(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
      int limit,
      @DefaultValue(StringUtils.EMPTY) @QueryParam(RECON_QUERY_PREVKEY)
      String prevKey) {
    KeyInsightInfoResponse
        deletedDirInsightInfo = new KeyInsightInfoResponse();
    getPendingForDeletionDirInfo(limit, prevKey,
        deletedDirInsightInfo);
    return Response.ok(deletedDirInsightInfo).build();
  }

  private void updateReplicatedAndUnReplicatedTotal(
      KeyInsightInfoResponse deletedKeyAndDirInsightInfo,
      RepeatedOmKeyInfo repeatedOmKeyInfo) {
    repeatedOmKeyInfo.getOmKeyInfoList().forEach(omKeyInfo -> {
      deletedKeyAndDirInsightInfo.setUnreplicatedDataSize(
          deletedKeyAndDirInsightInfo.getUnreplicatedDataSize() +
              omKeyInfo.getDataSize());
      deletedKeyAndDirInsightInfo.setReplicatedDataSize(
          deletedKeyAndDirInsightInfo.getReplicatedDataSize() +
              omKeyInfo.getReplicatedSize());
    });
  }

  @VisibleForTesting
  public GlobalStatsDao getDao() {
    return this.globalStatsDao;
  }

  @VisibleForTesting
  public Table<Long, NSSummary> getNsSummaryTable() {
    return this.reconNamespaceSummaryManager.getNSSummaryTable();
  }
}
