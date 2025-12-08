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
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_FETCH_COUNT;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_KEY_SIZE;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_OPEN_KEY_INCLUDE_FSO;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_OPEN_KEY_INCLUDE_NON_FSO;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OPEN_KEY_INCLUDE_FSO;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OPEN_KEY_INCLUDE_NON_FSO;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_PREVKEY;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_START_PREFIX;
import static org.apache.hadoop.ozone.recon.ReconResponseUtils.createBadRequestResponse;
import static org.apache.hadoop.ozone.recon.ReconResponseUtils.createInternalServerErrorResponse;
import static org.apache.hadoop.ozone.recon.ReconResponseUtils.noMatchedKeysResponse;
import static org.apache.hadoop.ozone.recon.api.handlers.BucketHandler.getBucketHandler;
import static org.apache.hadoop.ozone.recon.api.handlers.EntityHandler.normalizePath;
import static org.apache.hadoop.ozone.recon.api.handlers.EntityHandler.parseRequestPath;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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
import org.apache.hadoop.ozone.recon.api.types.ReconBasicOmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconGlobalStatsManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconNamespaceSummaryManagerImpl;
import org.apache.hadoop.ozone.recon.tasks.GlobalStatsValue;
import org.apache.hadoop.ozone.recon.tasks.OmTableInsightTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final ReconGlobalStatsManager reconGlobalStatsManager;
  private ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager;
  private final OzoneStorageContainerManager reconSCM;
  private final ReconGlobalMetricsService reconGlobalMetricsService;

  @Inject
  public OMDBInsightEndpoint(OzoneStorageContainerManager reconSCM,
                             ReconOMMetadataManager omMetadataManager,
                             ReconGlobalStatsManager reconGlobalStatsManager,
                             ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager,
                             ReconGlobalMetricsService reconGlobalMetricsService) {
    this.omMetadataManager = omMetadataManager;
    this.reconGlobalStatsManager = reconGlobalStatsManager;
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.reconSCM = reconSCM;
    this.reconGlobalMetricsService = reconGlobalMetricsService;
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
      @DefaultValue(StringUtils.EMPTY) @QueryParam(RECON_QUERY_START_PREFIX)
      String startPrefix,
      @DefaultValue(DEFAULT_OPEN_KEY_INCLUDE_FSO) @QueryParam(RECON_OPEN_KEY_INCLUDE_FSO)
      boolean includeFso,
      @DefaultValue(DEFAULT_OPEN_KEY_INCLUDE_NON_FSO) @QueryParam(RECON_OPEN_KEY_INCLUDE_NON_FSO)
      boolean includeNonFso) {

    KeyInsightInfoResponse openKeyInsightInfo = new KeyInsightInfoResponse();

    try {
      long replicatedTotal = 0;
      long unreplicatedTotal = 0;
      boolean skipPrevKeyDone = false;  // Tracks if prevKey was used earlier
      boolean keysFound = false; // Flag to track if any keys are found
      String lastKey = null;
      Map<String, OmKeyInfo> obsKeys = Collections.emptyMap();
      Map<String, OmKeyInfo> fsoKeys = Collections.emptyMap();

      // Validate startPrefix if it's provided
      if (isNotBlank(startPrefix) && !validateStartPrefix(startPrefix)) {
        return createBadRequestResponse("Invalid startPrefix: Path must be at the bucket level or deeper.");
      }

      // Use searchOpenKeys logic with adjustments for FSO and Non-FSO filtering
      if (includeNonFso) {
        // Search for non-FSO keys in KeyTable
        Table<String, OmKeyInfo> openKeyTable = omMetadataManager.getOpenKeyTable(BucketLayout.LEGACY);
        obsKeys = ReconUtils.extractKeysFromTable(openKeyTable, startPrefix, limit, prevKey);
        for (Map.Entry<String, OmKeyInfo> entry : obsKeys.entrySet()) {
          keysFound = true;
          skipPrevKeyDone = true; // Don't use the prevKey for the file table
          KeyEntityInfo keyEntityInfo = createKeyEntityInfoFromOmKeyInfo(entry.getKey(), entry.getValue());
          openKeyInsightInfo.getNonFSOKeyInfoList().add(keyEntityInfo); // Add to non-FSO list
          replicatedTotal += entry.getValue().getReplicatedSize();
          unreplicatedTotal += entry.getValue().getDataSize();
          lastKey = entry.getKey(); // Update lastKey
        }
      }

      if (includeFso) {
        // Search for FSO keys in FileTable
        // If prevKey was used for non-FSO keys, skip it for FSO keys.
        String effectivePrevKey = skipPrevKeyDone ? "" : prevKey;
        // If limit = -1 then we need to fetch all keys without limit
        int effectiveLimit = limit == -1 ? limit : limit - obsKeys.size();
        fsoKeys = searchOpenKeysInFSO(startPrefix, effectiveLimit, effectivePrevKey);
        for (Map.Entry<String, OmKeyInfo> entry : fsoKeys.entrySet()) {
          keysFound = true;
          KeyEntityInfo keyEntityInfo = createKeyEntityInfoFromOmKeyInfo(entry.getKey(), entry.getValue());
          openKeyInsightInfo.getFsoKeyInfoList().add(keyEntityInfo); // Add to FSO list
          replicatedTotal += entry.getValue().getReplicatedSize();
          unreplicatedTotal += entry.getValue().getDataSize();
          lastKey = entry.getKey(); // Update lastKey
        }
      }

      // If no keys were found, return a response indicating that no keys matched
      if (!keysFound) {
        return noMatchedKeysResponse(startPrefix);
      }

      // Set the aggregated totals in the response
      openKeyInsightInfo.setReplicatedDataSize(replicatedTotal);
      openKeyInsightInfo.setUnreplicatedDataSize(unreplicatedTotal);
      openKeyInsightInfo.setLastKey(lastKey);

      // Return the response with the matched keys and their data sizes
      return Response.ok(openKeyInsightInfo).build();
    } catch (IOException e) {
      // Handle IO exceptions and return an internal server error response
      return createInternalServerErrorResponse("Error searching open keys in OM DB: " + e.getMessage());
    } catch (IllegalArgumentException e) {
      // Handle illegal argument exceptions and return a bad request response
      return createBadRequestResponse("Invalid argument: " + e.getMessage());
    }
  }

  public Map<String, OmKeyInfo> searchOpenKeysInFSO(String startPrefix,
                                                    int limit, String prevKey)
      throws IOException, IllegalArgumentException {
    Map<String, OmKeyInfo> matchedKeys = new LinkedHashMap<>();
    // Convert the search prefix to an object path for FSO buckets
    String startPrefixObjectPath = ReconUtils.convertToObjectPathForOpenKeySearch(
            startPrefix, omMetadataManager, reconNamespaceSummaryManager, reconSCM);
    String[] names = parseRequestPath(startPrefixObjectPath);
    Table<String, OmKeyInfo> openFileTable =
        omMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // If names.length <= 2, then the search prefix is at the volume or bucket level hence
    // no need to find parent or extract id's or find subpaths as the openFileTable is
    // suitable for volume and bucket level search
    if (names.length > 2 && startPrefixObjectPath.endsWith(OM_KEY_PREFIX)) {
      // Fetch the parent ID to search for
      long parentId = Long.parseLong(names[names.length - 1]);

      // Fetch the nameSpaceSummary for the parent ID
      NSSummary parentSummary = reconNamespaceSummaryManager.getNSSummary(parentId);
      if (parentSummary == null) {
        return matchedKeys;
      }
      List<String> subPaths = new ArrayList<>();
      // Add the initial search prefix object path because it can have both openFiles
      // and subdirectories with openFiles
      subPaths.add(startPrefixObjectPath);

      // Recursively gather all subpaths
      ReconUtils.gatherSubPaths(parentId, subPaths, Long.parseLong(names[0]), Long.parseLong(names[1]),
          reconNamespaceSummaryManager);

      // Iterate over the subpaths and retrieve the open files
      for (String subPath : subPaths) {
        matchedKeys.putAll(
            ReconUtils.extractKeysFromTable(openFileTable, subPath, limit - matchedKeys.size(), prevKey));
        if (matchedKeys.size() >= limit) {
          break;
        }
      }
      return matchedKeys;
    }

    // If the search level is at the volume, bucket or key level, directly search the openFileTable
    matchedKeys.putAll(
        ReconUtils.extractKeysFromTable(openFileTable, startPrefixObjectPath, limit, prevKey));
    return matchedKeys;
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
    Map<String, Long> keysSummary = reconGlobalMetricsService.getOpenKeySummary();
    return Response.ok(keysSummary).build();
  }

  private Long getValueFromId(GlobalStatsValue record) {
    // If the record is null, return 0
    return record != null ? record.getValue() : 0L;
  }

  /**
   * Retrieves the summary of open MPU keys.
   *
   * @return The HTTP response body includes a map with the following entries:
   * - "totalOpenMPUKeys": the total number of open MPU keys
   * - "totalReplicatedDataSize": the total replicated size for open MPU keys
   * - "totalUnreplicatedDataSize": the total unreplicated size for open MPU keys
   *
   * Example response:
   *   {
   *    "totalOpenMPUKeys": 2,
   *    "totalReplicatedDataSize": 90000,
   *    "totalDataSize": 30000
   *   }
   */
  @GET
  @Path("/open/mpu/summary")
  public Response getOpenMPUKeySummary() {
    // Create a HashMap for the keysSummary
    Map<String, Long> keysSummary = reconGlobalMetricsService.getMPUKeySummary();
    return Response.ok(keysSummary).build();
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
    Map<String, Long> keysSummary = reconGlobalMetricsService.getDeletedKeySummary();
    return Response.ok(keysSummary).build();
  }

  /**
   * This method retrieves set of keys/files pending for deletion.
   * <p>
   * limit - limits the number of key/files returned.
   * prevKey - E.g. /vol1/bucket1/key1, this will skip keys till it
   * seeks correctly to the given prevKey.
   * startPrefix - E.g. /vol1/bucket1, this will return keys matching this prefix.
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
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT) int limit,
      @DefaultValue(StringUtils.EMPTY) @QueryParam(RECON_QUERY_PREVKEY) String prevKey,
      @DefaultValue(StringUtils.EMPTY) @QueryParam(RECON_QUERY_START_PREFIX) String startPrefix) {

    // Initialize the response object to hold the key information
    KeyInsightInfoResponse deletedKeyInsightInfo = new KeyInsightInfoResponse();

    boolean keysFound = false;

    try {
      // Validate startPrefix if it's provided
      if (isNotBlank(startPrefix) && !validateStartPrefix(startPrefix)) {
        return createBadRequestResponse("Invalid startPrefix: Path must be at the bucket level or deeper.");
      }

      // Perform the search based on the limit, prevKey, and startPrefix
      keysFound = getPendingForDeletionKeyInfo(limit, prevKey, startPrefix, deletedKeyInsightInfo);

    } catch (IllegalArgumentException e) {
      LOG.error("Invalid startPrefix provided: {}", startPrefix, e);
      return createBadRequestResponse("Invalid startPrefix: " + e.getMessage());
    } catch (IOException e) {
      LOG.error("I/O error while searching deleted keys in OM DB", e);
      return createInternalServerErrorResponse("Error searching deleted keys in OM DB: " + e.getMessage());
    } catch (Exception e) {
      LOG.error("Unexpected error occurred while searching deleted keys", e);
      return createInternalServerErrorResponse("Unexpected error: " + e.getMessage());
    }

    if (!keysFound) {
      return noMatchedKeysResponse("");
    }

    return Response.ok(deletedKeyInsightInfo).build();
  }

  /**
   * Retrieves keys pending deletion based on startPrefix, filtering keys matching the prefix.
   *
   * @param limit                 The limit of records to return.
   * @param prevKey               Pagination key.
   * @param startPrefix           The search prefix.
   * @param deletedKeyInsightInfo The response object to populate.
   */
  private boolean getPendingForDeletionKeyInfo(
      int limit, String prevKey, String startPrefix,
      KeyInsightInfoResponse deletedKeyInsightInfo) throws IOException {

    long replicatedTotal = 0;
    long unreplicatedTotal = 0;
    boolean keysFound = false;
    String lastKey = null;

    // Search for deleted keys in DeletedTable
    Table<String, RepeatedOmKeyInfo> deletedTable = omMetadataManager.getDeletedTable();
    Map<String, RepeatedOmKeyInfo> deletedKeys =
        ReconUtils.extractKeysFromTable(deletedTable, startPrefix, limit, prevKey);

    // Iterate over the retrieved keys and populate the response
    for (Map.Entry<String, RepeatedOmKeyInfo> entry : deletedKeys.entrySet()) {
      keysFound = true;
      RepeatedOmKeyInfo repeatedOmKeyInfo = entry.getValue();

      // We know each RepeatedOmKeyInfo has just one OmKeyInfo object
      OmKeyInfo keyInfo = repeatedOmKeyInfo.getOmKeyInfoList().get(0);

      // Add the key directly to the list without classification
      deletedKeyInsightInfo.getRepeatedOmKeyInfoList().add(repeatedOmKeyInfo);

      replicatedTotal += keyInfo.getReplicatedSize();
      unreplicatedTotal += keyInfo.getDataSize();

      lastKey = entry.getKey(); // Update lastKey
    }

    // Set the aggregated totals in the response
    deletedKeyInsightInfo.setReplicatedDataSize(replicatedTotal);
    deletedKeyInsightInfo.setUnreplicatedDataSize(unreplicatedTotal);
    deletedKeyInsightInfo.setLastKey(lastKey);

    return keysFound;
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
        deletedDirInsightInfo = reconGlobalMetricsService.getPendingForDeletionDirInfo(limit, prevKey);
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
   *    {@literal `api/v1/keys/listKeys?startPrefix=/volume1/obs-bucket&limit=2&replicationType=RATIS`}
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
   *        {@literal `api/v1/keys/listKeys?startPrefix=/volume1/fso-bucket&limit=2&replicationType=RATIS`}
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
    if (startPrefix == null || startPrefix.isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    String[] names = startPrefix.split(OM_KEY_PREFIX);
    if (names.length < 3) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    ListKeysResponse listKeysResponse = new ListKeysResponse();
    if (!ReconUtils.isInitializationComplete(omMetadataManager)) {
      listKeysResponse.setStatus(ResponseStatus.INITIALIZING);
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(listKeysResponse).build();
    }
    ParamInfo paramInfo = new ParamInfo(replicationType, creationDate, keySize, startPrefix, prevKey,
        limit, false, "");
    Response response = getListKeysResponse(paramInfo);
    if ((response.getStatus() != Response.Status.OK.getStatusCode()) &&
        (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode())) {
      return response;
    }
    if (response.getEntity() instanceof ListKeysResponse) {
      listKeysResponse = (ListKeysResponse) response.getEntity();
    }

    List<ReconBasicOmKeyInfo> keyInfoList = listKeysResponse.getKeys();
    if (!keyInfoList.isEmpty()) {
      listKeysResponse.setLastKey(keyInfoList.get(keyInfoList.size() - 1).getKey());
    }
    return Response.ok(listKeysResponse).build();
  }

  private Response getListKeysResponse(ParamInfo paramInfo) {
    ListKeysResponse listKeysResponse = new ListKeysResponse();
    try {
      paramInfo.setLimit(Math.max(0, paramInfo.getLimit())); // Ensure limit is non-negative
      listKeysResponse.setPath(paramInfo.getStartPrefix());
      long replicatedTotal = 0;
      long unreplicatedTotal = 0;

      Table<String, ReconBasicOmKeyInfo> keyTable =
          omMetadataManager.getKeyTableBasic(BucketLayout.LEGACY);

      retrieveKeysFromTable(keyTable, paramInfo, listKeysResponse.getKeys());

      // Search keys from FSO layout.
      searchKeysInFSO(paramInfo, listKeysResponse.getKeys());

      // If no keys were found, return a response indicating that no keys matched
      if (listKeysResponse.getKeys().isEmpty()) {
        return ReconResponseUtils.noMatchedKeysResponse(paramInfo.getStartPrefix());
      }

      for (ReconBasicOmKeyInfo keyEntityInfo : listKeysResponse.getKeys()) {
        replicatedTotal += keyEntityInfo.getReplicatedSize();
        unreplicatedTotal += keyEntityInfo.getSize();
      }

      // Set the aggregated totals in the response
      listKeysResponse.setReplicatedDataSize(replicatedTotal);
      listKeysResponse.setUnReplicatedDataSize(unreplicatedTotal);

      return Response.ok(listKeysResponse).build();
    } catch (RuntimeException e) {
      if (e instanceof ServiceNotReadyException) {
        listKeysResponse.setStatus(ResponseStatus.INITIALIZING);
        return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(listKeysResponse).build();
      }
      LOG.error("Error generating listKeys response", e);
      return ReconResponseUtils.createInternalServerErrorResponse(
          "Unexpected runtime error while searching keys in OM DB: " + e.getMessage());
    } catch (Exception e) {
      LOG.error("Error generating listKeys response", e);
      return ReconResponseUtils.createInternalServerErrorResponse(
          "Error listing keys from OM DB: " + e.getMessage());
    }
  }

  public void searchKeysInFSO(ParamInfo paramInfo, List<ReconBasicOmKeyInfo> results)
      throws IOException {
    // Convert the search prefix to an object path for FSO buckets
    String startPrefixObjectPath = convertStartPrefixPathToObjectIdPath(paramInfo.getStartPrefix());
    String[] names = parseRequestPath(startPrefixObjectPath);

    Table<String, ReconBasicOmKeyInfo> fileTable =
        omMetadataManager.getKeyTableBasic(BucketLayout.FILE_SYSTEM_OPTIMIZED);

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
        return;
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
        retrieveKeysFromTable(fileTable, paramInfo, results);
        if (results.size() >= paramInfo.getLimit()) {
          break;
        }
      }
      return;
    }

    paramInfo.setStartPrefix(startPrefixObjectPath);
    // Iterate over for bucket and volume level search
    retrieveKeysFromTable(fileTable, paramInfo, results);
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
  private void retrieveKeysFromTable(
      Table<String, ReconBasicOmKeyInfo> table, ParamInfo paramInfo, List<ReconBasicOmKeyInfo> results)
      throws IOException {
    boolean skipPrevKey = false;
    String seekKey = paramInfo.getPrevKey();
    try (
        TableIterator<String, ? extends Table.KeyValue<String, ReconBasicOmKeyInfo>> keyIter = table.iterator()) {

      if (!paramInfo.isSkipPrevKeyDone() && isNotBlank(seekKey)) {
        skipPrevKey = true;
        Table.KeyValue<String, ReconBasicOmKeyInfo> seekKeyValue =
            keyIter.seek(seekKey);

        // check if RocksDB was able to seek correctly to the given key prefix
        // if not, then return empty result
        // In case of an empty prevKeyPrefix, all the keys are returned
        if (seekKeyValue == null || (!seekKeyValue.getKey().equals(paramInfo.getPrevKey()))) {
          return;
        }
      } else {
        keyIter.seek(paramInfo.getStartPrefix());
      }

      long prevParentID = -1;
      StringBuilder keyPrefix = null;
      int keyPrefixLength = 0;
      while (keyIter.hasNext()) {
        Table.KeyValue<String, ReconBasicOmKeyInfo> entry = keyIter.next();
        String dbKey = entry.getKey();
        if (!dbKey.startsWith(paramInfo.getStartPrefix())) {
          break; // Exit the loop if the key no longer matches the prefix
        }
        if (skipPrevKey && dbKey.equals(paramInfo.getPrevKey())) {
          paramInfo.setSkipPrevKeyDone(true);
          continue;
        }
        if (applyFilters(entry, paramInfo)) {
          ReconBasicOmKeyInfo keyEntityInfo = entry.getValue();
          keyEntityInfo.setKey(dbKey);
          if (keyEntityInfo.getParentId() == 0) {
            // Legacy bucket keys have a parentID of zero. OBS bucket keys have a parentID of the bucketID.
            // FSO keys have a parent of the immediate parent directory.
            // Legacy buckets are obsolete, so this code path is not optimized. We don't expect to see many Legacy
            // buckets in practice.
            prevParentID = -1;
            String fullPath = ReconUtils.constructFullPath(keyEntityInfo.getKeyName(), keyEntityInfo.getParentId(),
                keyEntityInfo.getVolumeName(), keyEntityInfo.getBucketName(), reconNamespaceSummaryManager);
            if (fullPath.isEmpty()) {
              LOG.warn("Full path is empty for volume: {}, bucket: {}, key: {}",
                  keyEntityInfo.getVolumeName(), keyEntityInfo.getBucketName(), keyEntityInfo.getKeyName());
              continue;
            }
            keyEntityInfo.setPath(fullPath);
          } else {
            // As we iterate keys in sorted order, its highly likely that keys have the same prefix for many keys in a
            // row. Especially for FSO buckets, its expensive to construct the path for each key. So, we construct the
            // prefix once and reuse it for each identical parent. Only if the parent changes do we need to construct
            // a new prefix path.
            if (prevParentID != keyEntityInfo.getParentId()) {
              prevParentID = keyEntityInfo.getParentId();
              keyPrefix = ReconUtils.constructFullPathPrefix(keyEntityInfo.getParentId(),
                  keyEntityInfo.getVolumeName(), keyEntityInfo.getBucketName(), reconNamespaceSummaryManager);
              keyPrefixLength = keyPrefix.length();
            }
            keyPrefix.setLength(keyPrefixLength);
            keyPrefix.append(keyEntityInfo.getKeyName());
            String keyPrefixFullPath = keyPrefix.toString();
            if (keyPrefixFullPath.isEmpty()) {
              LOG.warn("Full path is empty for volume: {}, bucket: {}, key: {}",
                  keyEntityInfo.getVolumeName(), keyEntityInfo.getBucketName(), keyEntityInfo.getKeyName());
              continue;
            }
            keyEntityInfo.setPath(keyPrefixFullPath);
          }

          results.add(keyEntityInfo);
          paramInfo.setLastKey(dbKey);
          if (results.size() >= paramInfo.getLimit()) {
            break;
          }
        }
      }
    } catch (IOException exception) {
      LOG.error("Error retrieving keys from table for path: {}", paramInfo.getStartPrefix(), exception);
      throw exception;
    }
  }

  private boolean applyFilters(Table.KeyValue<String, ReconBasicOmKeyInfo> entry, ParamInfo paramInfo)
      throws IOException {

    LOG.debug("Applying filters on : {}", entry.getKey());

    if (!StringUtils.isEmpty(paramInfo.getCreationDate())
        && (entry.getValue().getCreationTime() < paramInfo.getCreationDateEpoch())) {
      return false;
    }

    if (!StringUtils.isEmpty(paramInfo.getReplicationType())
        && !entry.getValue().getReplicationConfig().getReplicationType().name().equals(
            paramInfo.getReplicationType())) {
      return false;
    }

    return entry.getValue().getSize() >= paramInfo.getKeySize();
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
    keyEntityInfo.setIsKey(keyInfo.isFile());
    String fullKeyPath = ReconUtils.constructFullPath(keyInfo, reconNamespaceSummaryManager);
    keyEntityInfo.setPath(fullKeyPath.isEmpty() ? keyInfo.getKeyName() : fullKeyPath);
    keyEntityInfo.setSize(keyInfo.getDataSize());
    keyEntityInfo.setCreationTime(keyInfo.getCreationTime());
    keyEntityInfo.setModificationTime(keyInfo.getModificationTime());
    keyEntityInfo.setReplicatedSize(keyInfo.getReplicatedSize());
    keyEntityInfo.setReplicationConfig(keyInfo.getReplicationConfig());
    return keyEntityInfo;
  }

  private void createSummaryForDeletedDirectories(
      Map<String, Long> dirSummary) {
    try {
      // Fetch the necessary metrics for deleted directories.
      Long deletedDirCount = getValueFromId(reconGlobalStatsManager.getGlobalStatsValue(
          OmTableInsightTask.getTableCountKeyFromTable(DELETED_DIR_TABLE)));
      // Calculate the total number of deleted directories
      dirSummary.put("totalDeletedDirectories", deletedDirCount);
    } catch (IOException e) {
      LOG.error("Error retrieving deleted directory summary from RocksDB", e);
      // Return zero in case of error
      dirSummary.put("totalDeletedDirectories", 0L);
    }
  }

  private boolean validateStartPrefix(String startPrefix) {

    // Ensure startPrefix starts with '/' for non-empty values
    startPrefix = startPrefix.startsWith("/") ? startPrefix : "/" + startPrefix;

    // Split the path to ensure it's at least at the bucket level (volume/bucket).
    String[] pathComponents = startPrefix.split("/");
    if (pathComponents.length < 3 || pathComponents[2].isEmpty()) {
      return false; // Invalid if not at bucket level or deeper
    }

    return true;
  }

  @VisibleForTesting
  public ReconGlobalStatsManager getReconGlobalStatsManager() {
    return this.reconGlobalStatsManager;
  }

  @VisibleForTesting
  public Table<Long, NSSummary> getNsSummaryTable() {
    return this.reconNamespaceSummaryManager.getNSSummaryTable();
  }
}
