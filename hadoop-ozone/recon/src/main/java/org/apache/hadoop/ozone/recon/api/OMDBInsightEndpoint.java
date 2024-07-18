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
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconResponseUtils;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.handlers.BucketHandler;
import org.apache.hadoop.ozone.recon.api.types.KeyEntityInfo;
import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResponse;
import org.apache.hadoop.ozone.recon.api.types.ListKeysResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.ParamInfo;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_FETCH_COUNT;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_KEY_SIZE;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_PREVKEY;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_OPEN_KEY_INCLUDE_FSO;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_OPEN_KEY_INCLUDE_NON_FSO;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OPEN_KEY_INCLUDE_FSO;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OPEN_KEY_INCLUDE_NON_FSO;
import static org.apache.hadoop.ozone.recon.api.handlers.BucketHandler.getBucketHandler;
import static org.apache.hadoop.ozone.recon.api.handlers.EntityHandler.normalizePath;
import static org.apache.hadoop.ozone.recon.api.handlers.EntityHandler.parseRequestPath;


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

  private final ReconOMMetadataManager omMetadataManager;
  private static final Logger LOG =
      LoggerFactory.getLogger(OMDBInsightEndpoint.class);
  private final GlobalStatsDao globalStatsDao;
  private ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager;
  private final OzoneStorageContainerManager reconSCM;


  @Inject
  public OMDBInsightEndpoint(OzoneStorageContainerManager reconSCM,
                             ReconOMMetadataManager omMetadataManager,
                             GlobalStatsDao globalStatsDao,
                             ReconNamespaceSummaryManagerImpl
                                 reconNamespaceSummaryManager) {
    this.omMetadataManager = omMetadataManager;
    this.globalStatsDao = globalStatsDao;
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.reconSCM = reconSCM;
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
        keyEntityInfo.setKey(omKeyInfo.getFileName());
        keyEntityInfo.setPath(createPath(omKeyInfo));
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

  /**
   * Retrieves the summary of deleted directories.
   *
   * This method calculates and returns a summary of deleted directories.
   * @return The HTTP  response body includes a map with the following entries:
   * - "totalDeletedDirectories": the total number of deleted directories
   *
   * Example response:
   *   {
   *    "totalDeletedDirectories": 8,
   *   }
   */
  @GET
  @Path("/deletePending/dirs/summary")
  public Response getDeletedDirectorySummary() {
    Map<String, Long> dirSummary = new HashMap<>();
    // Create a keys summary for deleted directories
    createSummaryForDeletedDirectories(dirSummary);
    return Response.ok(dirSummary).build();
  }

  /**
   * This API will list out limited 'count' number of keys after applying below filters in API parameters:
   * Default Values of API param filters:
   *    -- replicationType - empty string and filter will not be applied, so list out all keys irrespective of
   *       replication type.
   *    -- creationTime - empty string and filter will not be applied, so list out keys irrespective of age.
   *    -- keySize - 0 bytes, which means all keys greater than zero bytes will be listed, effectively all.
   *    -- startPrefix - /, API assumes that startPrefix path always starts with /. E.g. /volume/bucket
   *    -- prevKey - ""
   *    -- limit - 1000
   *
   * @param replicationType Filter for RATIS or EC replication keys
   * @param creationDate Filter for keys created after creationDate in "MM-dd-yyyy HH:mm:ss" string format.
   * @param keySize Filter for Keys greater than keySize in bytes.
   * @param startPrefix Filter for startPrefix path.
   * @param prevKey rocksDB last key of page requested.
   * @param limit Filter for limited count of keys.
   *
   * @return the list of keys in JSON structured format as per respective bucket layout.
   *
   * Now lets consider, we have following OBS, LEGACY and FSO bucket key/files namespace tree structure
   *
   * For OBS Bucket
   *
   * /volume1/obs-bucket/key1
   * /volume1/obs-bucket/key1/key2
   * /volume1/obs-bucket/key1/key2/key3
   * /volume1/obs-bucket/key4
   * /volume1/obs-bucket/key5
   * /volume1/obs-bucket/key6
   * For LEGACY Bucket
   *
   * /volume1/legacy-bucket/key
   * /volume1/legacy-bucket/key1/key2
   * /volume1/legacy-bucket/key1/key2/key3
   * /volume1/legacy-bucket/key4
   * /volume1/legacy-bucket/key5
   * /volume1/legacy-bucket/key6
   * For FSO Bucket
   *
   * /volume1/fso-bucket/dir1/dir2/dir3
   * /volume1/fso-bucket/dir1/testfile
   * /volume1/fso-bucket/dir1/file1
   * /volume1/fso-bucket/dir1/dir2/testfile
   * /volume1/fso-bucket/dir1/dir2/file1
   * /volume1/fso-bucket/dir1/dir2/dir3/testfile
   * /volume1/fso-bucket/dir1/dir2/dir3/file1
   * Input Request for OBS bucket:
   *
   *    `api/v1/keys/listKeys?startPrefix=/volume1/obs-bucket&limit=2&replicationType=RATIS`
   * Output Response:
   *
   * {
   *     "status": "OK",
   *     "path": "/volume1/obs-bucket",
   *     "replicatedDataSize": 62914560,
   *     "unReplicatedDataSize": 62914560,
   *     "lastKey": "/volume1/obs-bucket/key6",
   *     "keys": [
   *         {
   *             "key": "/volume1/obs-bucket/key1",
   *             "path": "volume1/obs-bucket/key1",
   *             "size": 10485760,
   *             "replicatedSize": 10485760,
   *             "replicationInfo": {
   *                 "replicationFactor": "ONE",
   *                 "requiredNodes": 1,
   *                 "replicationType": "RATIS"
   *             },
   *             "creationTime": 1715781418742,
   *             "modificationTime": 1715781419762,
   *             "isKey": true
   *         },
   *         {
   *             "key": "/volume1/obs-bucket/key1/key2",
   *             "path": "volume1/obs-bucket/key1/key2",
   *             "size": 10485760,
   *             "replicatedSize": 10485760,
   *             "replicationInfo": {
   *                 "replicationFactor": "ONE",
   *                 "requiredNodes": 1,
   *                 "replicationType": "RATIS"
   *             },
   *             "creationTime": 1715781421716,
   *             "modificationTime": 1715781422723,
   *             "isKey": true
   *         },
   *         {
   *             "key": "/volume1/obs-bucket/key1/key2/key3",
   *             "path": "volume1/obs-bucket/key1/key2/key3",
   *             "size": 10485760,
   *             "replicatedSize": 10485760,
   *             "replicationInfo": {
   *                 "replicationFactor": "ONE",
   *                 "requiredNodes": 1,
   *                 "replicationType": "RATIS"
   *             },
   *             "creationTime": 1715781424718,
   *             "modificationTime": 1715781425598,
   *             "isKey": true
   *         },
   *         {
   *             "key": "/volume1/obs-bucket/key4",
   *             "path": "volume1/obs-bucket/key4",
   *             "size": 10485760,
   *             "replicatedSize": 10485760,
   *             "replicationInfo": {
   *                 "replicationFactor": "ONE",
   *                 "requiredNodes": 1,
   *                 "replicationType": "RATIS"
   *             },
   *             "creationTime": 1715781427561,
   *             "modificationTime": 1715781428407,
   *             "isKey": true
   *         },
   *         {
   *             "key": "/volume1/obs-bucket/key5",
   *             "path": "volume1/obs-bucket/key5",
   *             "size": 10485760,
   *             "replicatedSize": 10485760,
   *             "replicationInfo": {
   *                 "replicationFactor": "ONE",
   *                 "requiredNodes": 1,
   *                 "replicationType": "RATIS"
   *             },
   *             "creationTime": 1715781430347,
   *             "modificationTime": 1715781431185,
   *             "isKey": true
   *         },
   *         {
   *             "key": "/volume1/obs-bucket/key6",
   *             "path": "volume1/obs-bucket/key6",
   *             "size": 10485760,
   *             "replicatedSize": 10485760,
   *             "replicationInfo": {
   *                 "replicationFactor": "ONE",
   *                 "requiredNodes": 1,
   *                 "replicationType": "RATIS"
   *             },
   *             "creationTime": 1715781433154,
   *             "modificationTime": 1715781433962,
   *             "isKey": true
   *         }
   *     ]
   * }
   * Input Request for FSO bucket:
   *
   *        `api/v1/keys/listKeys?startPrefix=/volume1/fso-bucket&limit=2&replicationType=RATIS`
   * Output Response:
   *
   * {
   *     "status": "OK",
   *     "path": "/volume1/fso-bucket",
   *     "replicatedDataSize": 188743680,
   *     "unReplicatedDataSize": 62914560,
   *     "lastKey": "/-9223372036854775552/-9223372036854774016/-9223372036854773503/testfile",
   *     "keys": [
   *         {
   *             "key": "/-9223372036854775552/-9223372036854774016/-9223372036854773501/file1",
   *             "path": "volume1/fso-bucket/dir1/dir2/dir3/file1",
   *             "size": 10485760,
   *             "replicatedSize": 31457280,
   *             "replicationInfo": {
   *                 "replicationFactor": "THREE",
   *                 "requiredNodes": 3,
   *                 "replicationType": "RATIS"
   *             },
   *             "creationTime": 1715781411785,
   *             "modificationTime": 1715781415119,
   *             "isKey": true
   *         },
   *         {
   *             "key": "/-9223372036854775552/-9223372036854774016/-9223372036854773501/testfile",
   *             "path": "volume1/fso-bucket/dir1/dir2/dir3/testfile",
   *             "size": 10485760,
   *             "replicatedSize": 31457280,
   *             "replicationInfo": {
   *                 "replicationFactor": "THREE",
   *                 "requiredNodes": 3,
   *                 "replicationType": "RATIS"
   *             },
   *             "creationTime": 1715781409146,
   *             "modificationTime": 1715781409882,
   *             "isKey": true
   *         },
   *         {
   *             "key": "/-9223372036854775552/-9223372036854774016/-9223372036854773502/file1",
   *             "path": "volume1/fso-bucket/dir1/dir2/file1",
   *             "size": 10485760,
   *             "replicatedSize": 31457280,
   *             "replicationInfo": {
   *                 "replicationFactor": "THREE",
   *                 "requiredNodes": 3,
   *                 "replicationType": "RATIS"
   *             },
   *             "creationTime": 1715781406333,
   *             "modificationTime": 1715781407140,
   *             "isKey": true
   *         },
   *         {
   *             "key": "/-9223372036854775552/-9223372036854774016/-9223372036854773502/testfile",
   *             "path": "volume1/fso-bucket/dir1/dir2/testfile",
   *             "size": 10485760,
   *             "replicatedSize": 31457280,
   *             "replicationInfo": {
   *                 "replicationFactor": "THREE",
   *                 "requiredNodes": 3,
   *                 "replicationType": "RATIS"
   *             },
   *             "creationTime": 1715781403655,
   *             "modificationTime": 1715781404460,
   *             "isKey": true
   *         },
   *         {
   *             "key": "/-9223372036854775552/-9223372036854774016/-9223372036854773503/file1",
   *             "path": "volume1/fso-bucket/dir1/file1",
   *             "size": 10485760,
   *             "replicatedSize": 31457280,
   *             "replicationInfo": {
   *                 "replicationFactor": "THREE",
   *                 "requiredNodes": 3,
   *                 "replicationType": "RATIS"
   *             },
   *             "creationTime": 1715781400980,
   *             "modificationTime": 1715781401768,
   *             "isKey": true
   *         },
   *         {
   *             "key": "/-9223372036854775552/-9223372036854774016/-9223372036854773503/testfile",
   *             "path": "volume1/fso-bucket/dir1/testfile",
   *             "size": 10485760,
   *             "replicatedSize": 31457280,
   *             "replicationInfo": {
   *                 "replicationFactor": "THREE",
   *                 "requiredNodes": 3,
   *                 "replicationType": "RATIS"
   *             },
   *             "creationTime": 1715781397636,
   *             "modificationTime": 1715781398919,
   *             "isKey": true
   *         }
   *     ]
   * }
   *
   * ********************************************************
   * @throws IOException
   */
  @GET
  @Path("/listKeys")
  @SuppressWarnings("methodlength")
  public Response listKeys(@QueryParam("replicationType") String replicationType,
                           @QueryParam("creationDate") String creationDate,
                           @DefaultValue(DEFAULT_KEY_SIZE) @QueryParam("keySize") long keySize,
                           @DefaultValue(OM_KEY_PREFIX) @QueryParam("startPrefix") String startPrefix,
                           @DefaultValue(StringUtils.EMPTY) @QueryParam(RECON_QUERY_PREVKEY) String prevKey,
                           @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam("limit") int limit) {


    // This API supports startPrefix from bucket level.
    if (startPrefix == null || startPrefix.length() == 0) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    String[] names = startPrefix.split(OM_KEY_PREFIX);
    if (names.length < 3) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    ListKeysResponse listKeysResponse = new ListKeysResponse();
    if (!ReconUtils.isInitializationComplete(omMetadataManager)) {
      listKeysResponse.setStatus(ResponseStatus.INITIALIZING);
      return Response.ok(listKeysResponse).build();
    }
    ParamInfo paramInfo = new ParamInfo(replicationType, creationDate, keySize, startPrefix, prevKey,
        limit, false, "");
    Response response = getListKeysResponse(paramInfo);
    if ((response.getStatus() != Response.Status.OK.getStatusCode()) &&
        (response.getStatus() != Response.Status.NOT_FOUND.getStatusCode())) {
      return response;
    }
    if (response.getEntity() instanceof ListKeysResponse) {
      listKeysResponse = (ListKeysResponse) response.getEntity();
    }

    List<KeyEntityInfo> keyInfoList = listKeysResponse.getKeys();
    if (!keyInfoList.isEmpty()) {
      listKeysResponse.setLastKey(keyInfoList.get(keyInfoList.size() - 1).getKey());
    }
    return Response.ok(listKeysResponse).build();
  }

  private Response getListKeysResponse(ParamInfo paramInfo) {
    try {
      paramInfo.setLimit(Math.max(0, paramInfo.getLimit())); // Ensure limit is non-negative
      ListKeysResponse listKeysResponse = new ListKeysResponse();
      listKeysResponse.setPath(paramInfo.getStartPrefix());
      long replicatedTotal = 0;
      long unreplicatedTotal = 0;
      boolean keysFound = false; // Flag to track if any keys are found

      // Search keys from non-FSO layout.
      Map<String, OmKeyInfo> obsKeys;
      Table<String, OmKeyInfo> keyTable =
          omMetadataManager.getKeyTable(BucketLayout.LEGACY);
      obsKeys = retrieveKeysFromTable(keyTable, paramInfo);
      for (Map.Entry<String, OmKeyInfo> entry : obsKeys.entrySet()) {
        keysFound = true;
        KeyEntityInfo keyEntityInfo =
            createKeyEntityInfoFromOmKeyInfo(entry.getKey(), entry.getValue());

        listKeysResponse.getKeys().add(keyEntityInfo);
        replicatedTotal += entry.getValue().getReplicatedSize();
        unreplicatedTotal += entry.getValue().getDataSize();
      }

      // Search keys from FSO layout.
      Map<String, OmKeyInfo> fsoKeys = searchKeysInFSO(paramInfo);
      for (Map.Entry<String, OmKeyInfo> entry : fsoKeys.entrySet()) {
        keysFound = true;
        KeyEntityInfo keyEntityInfo =
            createKeyEntityInfoFromOmKeyInfo(entry.getKey(), entry.getValue());

        listKeysResponse.getKeys().add(keyEntityInfo);
        replicatedTotal += entry.getValue().getReplicatedSize();
        unreplicatedTotal += entry.getValue().getDataSize();
      }

      // If no keys were found, return a response indicating that no keys matched
      if (!keysFound) {
        return ReconResponseUtils.noMatchedKeysResponse(paramInfo.getStartPrefix());
      }

      // Set the aggregated totals in the response
      listKeysResponse.setReplicatedDataSize(replicatedTotal);
      listKeysResponse.setUnReplicatedDataSize(unreplicatedTotal);

      return Response.ok(listKeysResponse).build();
    } catch (IOException e) {
      return ReconResponseUtils.createInternalServerErrorResponse(
          "Error listing keys from OM DB: " + e.getMessage());
    } catch (RuntimeException e) {
      return ReconResponseUtils.createInternalServerErrorResponse(
          "Unexpected runtime error while searching keys in OM DB: " + e.getMessage());
    } catch (Exception e) {
      return ReconResponseUtils.createInternalServerErrorResponse(
          "Error listing keys from OM DB: " + e.getMessage());
    }
  }

  public Map<String, OmKeyInfo> searchKeysInFSO(ParamInfo paramInfo)
      throws IOException {
    int originalLimit = paramInfo.getLimit();
    Map<String, OmKeyInfo> matchedKeys = new LinkedHashMap<>();
    // Convert the search prefix to an object path for FSO buckets
    String startPrefixObjectPath = convertStartPrefixPathToObjectIdPath(paramInfo.getStartPrefix());
    String[] names = parseRequestPath(startPrefixObjectPath);
    Table<String, OmKeyInfo> fileTable =
        omMetadataManager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // If names.length > 2, then the search prefix is at the level above bucket level hence
    // no need to find parent or extract id's or find subpaths as the fileTable is
    // suitable for volume and bucket level search
    if (names.length > 2) {
      // Fetch the parent ID to search for
      long parentId = Long.parseLong(names[names.length - 1]);

      // Fetch the nameSpaceSummary for the parent ID
      NSSummary parentSummary =
          reconNamespaceSummaryManager.getNSSummary(parentId);
      if (parentSummary == null) {
        return matchedKeys;
      }
      List<String> subPaths = new ArrayList<>();
      // Add the initial search prefix object path because it can have both files and subdirectories with files.
      subPaths.add(startPrefixObjectPath);

      // Recursively gather all subpaths
      ReconUtils.gatherSubPaths(parentId, subPaths, Long.parseLong(names[0]),
          Long.parseLong(names[1]), reconNamespaceSummaryManager);
      // Iterate over the subpaths and retrieve the files
      for (String subPath : subPaths) {
        paramInfo.setStartPrefix(subPath);
        matchedKeys.putAll(
            retrieveKeysFromTable(fileTable, paramInfo));
        paramInfo.setLimit(originalLimit - matchedKeys.size());
        if (matchedKeys.size() >= originalLimit) {
          break;
        }
      }
      return matchedKeys;
    }

    paramInfo.setStartPrefix(startPrefixObjectPath);
    // Iterate over for bucket and volume level search
    matchedKeys.putAll(
        retrieveKeysFromTable(fileTable, paramInfo));
    return matchedKeys;
  }


  /**
   * Converts a startPrefix path into an objectId path for FSO buckets, using IDs.
   * <p>
   * This method transforms a user-provided path (e.g., "volume/bucket/dir1") into
   * a database-friendly format ("/volumeID/bucketID/ParentId/") by replacing names
   * with their corresponding IDs. It simplifies database queries for FSO bucket operations.
   *
   * @param startPrefixPath The path to be converted.
   * @return The objectId path as "/volumeID/bucketID/ParentId/".
   * @throws IOException If database access fails.
   */
  public String convertStartPrefixPathToObjectIdPath(String startPrefixPath)
      throws IOException {

    String[] names = parseRequestPath(
        normalizePath(startPrefixPath, BucketLayout.FILE_SYSTEM_OPTIMIZED));

    // Root-Level :- Return the original path
    if (names.length == 0) {
      return startPrefixPath;
    }

    // Volume-Level :- Fetch the volumeID
    String volumeName = names[0];
    ReconUtils.validateNames(volumeName);
    String volumeKey = omMetadataManager.getVolumeKey(volumeName);
    long volumeId = omMetadataManager.getVolumeTable().getSkipCache(volumeKey)
        .getObjectID();
    if (names.length == 1) {
      return ReconUtils.constructObjectPathWithPrefix(volumeId);
    }

    // Bucket-Level :- Fetch the bucketID
    String bucketName = names[1];
    ReconUtils.validateNames(bucketName);
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo bucketInfo =
        omMetadataManager.getBucketTable().getSkipCache(bucketKey);
    long bucketId = bucketInfo.getObjectID();
    if (names.length == 2) {
      return ReconUtils.constructObjectPathWithPrefix(volumeId, bucketId);
    }

    // Fetch the immediate parentID which could be a directory or the bucket itself
    BucketHandler handler =
        getBucketHandler(reconNamespaceSummaryManager, omMetadataManager,
            reconSCM, bucketInfo);
    long dirObjectId = -1;
    try {
      OmDirectoryInfo dirInfo = handler.getDirInfo(names);
      if (null != dirInfo) {
        dirObjectId = dirInfo.getObjectID();
      } else {
        throw new IllegalArgumentException("Not valid path");
      }
    } catch (Exception ioe) {
      throw new IllegalArgumentException("Not valid path: " + ioe);
    }
    return ReconUtils.constructObjectPathWithPrefix(volumeId, bucketId, dirObjectId);
  }

  /**
   * Common method to retrieve keys from a table based on a search prefix and a limit.
   *
   * @param table     The table to retrieve keys from.
   * @param paramInfo The stats object holds total count, count and limit.
   * @return A map of keys and their corresponding OmKeyInfo objects.
   * @throws IOException If there are problems accessing the table.
   */
  private Map<String, OmKeyInfo> retrieveKeysFromTable(
      Table<String, OmKeyInfo> table, ParamInfo paramInfo)
      throws IOException {
    boolean skipPrevKey = false;
    String seekKey = paramInfo.getPrevKey();
    Map<String, OmKeyInfo> matchedKeys = new LinkedHashMap<>();
    try (
        TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> keyIter = table.iterator()) {

      if (!paramInfo.isSkipPrevKeyDone() && StringUtils.isNotBlank(seekKey)) {
        skipPrevKey = true;
        Table.KeyValue<String, OmKeyInfo> seekKeyValue =
            keyIter.seek(seekKey);

        // check if RocksDB was able to seek correctly to the given key prefix
        // if not, then return empty result
        // In case of an empty prevKeyPrefix, all the keys are returned
        if (seekKeyValue == null || (!seekKeyValue.getKey().equals(paramInfo.getPrevKey()))) {
          return matchedKeys;
        }
      } else {
        keyIter.seek(paramInfo.getStartPrefix());
      }

      while (keyIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> entry = keyIter.next();
        String dbKey = entry.getKey();
        if (!dbKey.startsWith(paramInfo.getStartPrefix())) {
          break; // Exit the loop if the key no longer matches the prefix
        }
        if (skipPrevKey && dbKey.equals(paramInfo.getPrevKey())) {
          paramInfo.setSkipPrevKeyDone(true);
          continue;
        }
        if (applyFilters(entry, paramInfo)) {
          matchedKeys.put(dbKey, entry.getValue());
          paramInfo.setLastKey(dbKey);
          if (matchedKeys.size() >= paramInfo.getLimit()) {
            break;
          }
        }
      }
    } catch (IOException exception) {
      LOG.error("Error retrieving keys from table for path: {}", paramInfo.getStartPrefix(), exception);
      throw exception;
    }
    return matchedKeys;
  }

  private boolean applyFilters(Table.KeyValue<String, OmKeyInfo> entry, ParamInfo paramInfo) throws IOException {

    LOG.debug("Applying filters on : {}", entry.getKey());

    long epochMillis =
        ReconUtils.convertToEpochMillis(paramInfo.getCreationDate(), "MM-dd-yyyy HH:mm:ss", TimeZone.getDefault());
    Predicate<Table.KeyValue<String, OmKeyInfo>> keyAgeFilter = keyData -> {
      try {
        return keyData.getValue().getCreationTime() >= epochMillis;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
    Predicate<Table.KeyValue<String, OmKeyInfo>> keyReplicationFilter =
        keyData -> {
          try {
            return keyData.getValue().getReplicationConfig().getReplicationType().name()
                .equals(paramInfo.getReplicationType());
          } catch (IOException e) {
            try {
              throw new IOException(e);
            } catch (IOException ex) {
              throw new RuntimeException(ex);
            }
          }
        };
    Predicate<Table.KeyValue<String, OmKeyInfo>> keySizeFilter = keyData -> {
      try {
        return keyData.getValue().getDataSize() >= paramInfo.getKeySize();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };

    List<Table.KeyValue<String, OmKeyInfo>> filteredKeyList = Stream.of(entry)
        .filter(keyData -> !StringUtils.isEmpty(paramInfo.getCreationDate()) ? keyAgeFilter.test(keyData) : true)
        .filter(
            keyData -> !StringUtils.isEmpty(paramInfo.getReplicationType()) ? keyReplicationFilter.test(keyData) : true)
        .filter(keySizeFilter)
        .collect(Collectors.toList());

    LOG.debug("After applying filter on : {}, filtered list size: {}", entry.getKey(), filteredKeyList.size());

    return (filteredKeyList.size() > 0);
  }

  /**
   * Creates a KeyEntityInfo object from an OmKeyInfo object and the corresponding key.
   *
   * @param dbKey   The key in the database corresponding to the OmKeyInfo object.
   * @param keyInfo The OmKeyInfo object to create the KeyEntityInfo from.
   * @return The KeyEntityInfo object created from the OmKeyInfo object and the key.
   */
  private KeyEntityInfo createKeyEntityInfoFromOmKeyInfo(String dbKey,
                                                         OmKeyInfo keyInfo) throws IOException {
    KeyEntityInfo keyEntityInfo = new KeyEntityInfo();
    keyEntityInfo.setKey(dbKey); // Set the DB key
    keyEntityInfo.setPath(ReconUtils.constructFullPath(keyInfo, reconNamespaceSummaryManager,
        omMetadataManager));
    keyEntityInfo.setSize(keyInfo.getDataSize());
    keyEntityInfo.setCreationTime(keyInfo.getCreationTime());
    keyEntityInfo.setModificationTime(keyInfo.getModificationTime());
    keyEntityInfo.setReplicatedSize(keyInfo.getReplicatedSize());
    keyEntityInfo.setReplicationConfig(keyInfo.getReplicationConfig());
    return keyEntityInfo;
  }

  private void createSummaryForDeletedDirectories(
      Map<String, Long> dirSummary) {
    // Fetch the necessary metrics for deleted directories.
    Long deletedDirCount = getValueFromId(globalStatsDao.findById(
        OmTableInsightTask.getTableCountKeyFromTable(DELETED_DIR_TABLE)));
    // Calculate the total number of deleted directories
    dirSummary.put("totalDeletedDirectories", deletedDirCount);
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

  private String createPath(OmKeyInfo omKeyInfo) {
    return omKeyInfo.getVolumeName() + OM_KEY_PREFIX +
        omKeyInfo.getBucketName() + OM_KEY_PREFIX + omKeyInfo.getKeyName();
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
