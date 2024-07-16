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

import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.handlers.BucketHandler;
import org.apache.hadoop.ozone.recon.api.types.KeyEntityInfo;
import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconNamespaceSummaryManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_START_PREFIX;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OPEN_KEY_DEFAULT_SEARCH_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OPEN_KEY_SEARCH_DEFAULT_PREV_KEY;
import static org.apache.hadoop.ozone.recon.ReconResponseUtils.noMatchedKeysResponse;
import static org.apache.hadoop.ozone.recon.ReconResponseUtils.createBadRequestResponse;
import static org.apache.hadoop.ozone.recon.ReconResponseUtils.createInternalServerErrorResponse;
import static org.apache.hadoop.ozone.recon.ReconUtils.constructObjectPathWithPrefix;
import static org.apache.hadoop.ozone.recon.ReconUtils.validateNames;
import static org.apache.hadoop.ozone.recon.api.handlers.BucketHandler.getBucketHandler;
import static org.apache.hadoop.ozone.recon.api.handlers.EntityHandler.normalizePath;
import static org.apache.hadoop.ozone.recon.api.handlers.EntityHandler.parseRequestPath;

/**
 * REST endpoint for search implementation in OM DB Insight.
 */
@Path("/keys")
@Produces(MediaType.APPLICATION_JSON)
@AdminOnly
public class OMDBInsightSearchEndpoint {

  private OzoneStorageContainerManager reconSCM;
  private final ReconOMMetadataManager omMetadataManager;
  private static final Logger LOG =
      LoggerFactory.getLogger(OMDBInsightSearchEndpoint.class);
  private ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager;


  @Inject
  public OMDBInsightSearchEndpoint(OzoneStorageContainerManager reconSCM,
                                   ReconOMMetadataManager omMetadataManager,
                                   ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager) {
    this.reconSCM = reconSCM;
    this.omMetadataManager = omMetadataManager;
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
  }


  /**
   * Performs a search for open keys in the Ozone Manager (OM) database using a specified search prefix.
   * This endpoint searches across both File System Optimized (FSO) and Object Store (non-FSO) layouts,
   * compiling a list of keys that match the given prefix along with their data sizes.
   * <p>
   * The search prefix must start from the bucket level ('/volumeName/bucketName/') or any specific directory
   * or key level (e.g., '/volA/bucketA/dir1' for everything under 'dir1' inside 'bucketA' of 'volA').
   * The search operation matches the prefix against the start of keys' names within the OM DB.
   * <p>
   * Example Usage:
   * 1. A startPrefix of "/volA/bucketA/" retrieves every key under bucket 'bucketA' in volume 'volA'.
   * 2. Specifying "/volA/bucketA/dir1" focuses the search within 'dir1' inside 'bucketA' of 'volA'.
   *
   * @param startPrefix The prefix for searching keys, starting from the bucket level or any specific path.
   * @param limit       Limits the number of returned keys.
   * @param prevKey     The key to start after for the next set of records.
   * @return A KeyInsightInfoResponse, containing matching keys and their data sizes.
   * @throws IOException On failure to access the OM database or process the operation.
   * @throws IllegalArgumentException If the provided startPrefix or other arguments are invalid.
   */
  @GET
  @Path("/open/search")
  public Response searchOpenKeys(
      @DefaultValue(DEFAULT_START_PREFIX) @QueryParam("startPrefix")
      String startPrefix,
      @DefaultValue(RECON_OPEN_KEY_DEFAULT_SEARCH_LIMIT) @QueryParam("limit")
      int limit,
      @DefaultValue(RECON_OPEN_KEY_SEARCH_DEFAULT_PREV_KEY) @QueryParam("prevKey") String prevKey) throws IOException {

    try {
      // Ensure startPrefix is not null or empty and starts with '/'
      if (startPrefix == null || startPrefix.length() == 0) {
        return createBadRequestResponse(
            "Invalid startPrefix: Path must be at the bucket level or deeper.");
      }
      startPrefix = startPrefix.startsWith("/") ? startPrefix : "/" + startPrefix;

      // Split the path to ensure it's at least at the bucket level
      String[] pathComponents = startPrefix.split("/");
      if (pathComponents.length < 3 || pathComponents[2].isEmpty()) {
        return createBadRequestResponse(
            "Invalid startPrefix: Path must be at the bucket level or deeper.");
      }

      // Ensure the limit is non-negative
      limit = Math.max(0, limit);

      // Initialize response object
      KeyInsightInfoResponse insightResponse = new KeyInsightInfoResponse();
      long replicatedTotal = 0;
      long unreplicatedTotal = 0;
      boolean keysFound = false; // Flag to track if any keys are found
      String lastKey = null;

      // Search for non-fso keys in KeyTable
      Table<String, OmKeyInfo> openKeyTable =
          omMetadataManager.getOpenKeyTable(BucketLayout.LEGACY);
      Map<String, OmKeyInfo> obsKeys =
          retrieveKeysFromTable(openKeyTable, startPrefix, limit, prevKey);
      for (Map.Entry<String, OmKeyInfo> entry : obsKeys.entrySet()) {
        keysFound = true;
        KeyEntityInfo keyEntityInfo =
            createKeyEntityInfoFromOmKeyInfo(entry.getKey(), entry.getValue());
        insightResponse.getNonFSOKeyInfoList()
            .add(keyEntityInfo); // Add to non-FSO list
        replicatedTotal += entry.getValue().getReplicatedSize();
        unreplicatedTotal += entry.getValue().getDataSize();
        lastKey = entry.getKey(); // Update lastKey
      }

      // Search for fso keys in FileTable
      Map<String, OmKeyInfo> fsoKeys = searchOpenKeysInFSO(startPrefix, limit, prevKey);
      for (Map.Entry<String, OmKeyInfo> entry : fsoKeys.entrySet()) {
        keysFound = true;
        KeyEntityInfo keyEntityInfo =
            createKeyEntityInfoFromOmKeyInfo(entry.getKey(), entry.getValue());
        insightResponse.getFsoKeyInfoList()
            .add(keyEntityInfo); // Add to FSO list
        replicatedTotal += entry.getValue().getReplicatedSize();
        unreplicatedTotal += entry.getValue().getDataSize();
        lastKey = entry.getKey(); // Update lastKey
      }

      // If no keys were found, return a response indicating that no keys matched
      if (!keysFound) {
        return noMatchedKeysResponse(startPrefix);
      }

      // Set the aggregated totals in the response
      insightResponse.setReplicatedDataSize(replicatedTotal);
      insightResponse.setUnreplicatedDataSize(unreplicatedTotal);
      insightResponse.setLastKey(lastKey);

      // Return the response with the matched keys and their data sizes
      return Response.ok(insightResponse).build();
    } catch (IOException e) {
      // Handle IO exceptions and return an internal server error response
      return createInternalServerErrorResponse(
          "Error searching open keys in OM DB: " + e.getMessage());
    } catch (IllegalArgumentException e) {
      // Handle illegal argument exceptions and return a bad request response
      return createBadRequestResponse(
          "Invalid startPrefix: " + e.getMessage());
    }
  }

  public Map<String, OmKeyInfo> searchOpenKeysInFSO(String startPrefix,
                                                    int limit, String prevKey)
      throws IOException, IllegalArgumentException {
    Map<String, OmKeyInfo> matchedKeys = new LinkedHashMap<>();
    // Convert the search prefix to an object path for FSO buckets
    String startPrefixObjectPath = convertToObjectPath(startPrefix);
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
        matchedKeys.putAll(retrieveKeysFromTable(openFileTable, subPath, limit - matchedKeys.size(), prevKey));
        if (matchedKeys.size() >= limit) {
          break;
        }
      }
      return matchedKeys;
    }

    // If the search level is at the volume, bucket or key level, directly search the openFileTable
    matchedKeys.putAll(retrieveKeysFromTable(openFileTable, startPrefixObjectPath, limit, prevKey));
    return matchedKeys;
  }

  /**
   * Converts a key prefix into an object path for FSO buckets, using IDs.
   *
   * This method transforms a user-provided path (e.g., "volume/bucket/dir1") into
   * a database-friendly format ("/volumeID/bucketID/ParentId/") by replacing names
   * with their corresponding IDs. It simplifies database queries for FSO bucket operations.
   *
   * Examples:
   * - Input: "volume/bucket/key" -> Output: "/volumeID/bucketID/parentDirID/key"
   * - Input: "volume/bucket/dir1" -> Output: "/volumeID/bucketID/dir1ID/"
   * - Input: "volume/bucket/dir1/key1" -> Output: "/volumeID/bucketID/dir1ID/key1"
   * - Input: "volume/bucket/dir1/dir2" -> Output: "/volumeID/bucketID/dir2ID/"
   *
   * @param prevKeyPrefix The path to be converted.
   * @return The object path as "/volumeID/bucketID/ParentId/" or an empty string if an error occurs.
   * @throws IOException If database access fails.
   * @throws IllegalArgumentException If the provided path is invalid or cannot be converted.
   */
  public String convertToObjectPath(String prevKeyPrefix) throws IOException {
    try {
      String[] names = parseRequestPath(normalizePath(prevKeyPrefix, BucketLayout.FILE_SYSTEM_OPTIMIZED));
      Table<String, OmKeyInfo> openFileTable = omMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);

      // Root-Level: Return the original path
      if (names.length == 0) {
        return prevKeyPrefix;
      }

      // Volume-Level: Fetch the volumeID
      String volumeName = names[0];
      validateNames(volumeName);
      String volumeKey = omMetadataManager.getVolumeKey(volumeName);
      long volumeId = omMetadataManager.getVolumeTable().getSkipCache(volumeKey).getObjectID();
      if (names.length == 1) {
        return constructObjectPathWithPrefix(volumeId);
      }

      // Bucket-Level: Fetch the bucketID
      String bucketName = names[1];
      validateNames(bucketName);
      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
      OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().getSkipCache(bucketKey);
      long bucketId = bucketInfo.getObjectID();
      if (names.length == 2 || bucketInfo.getBucketLayout() != BucketLayout.FILE_SYSTEM_OPTIMIZED) {
        return constructObjectPathWithPrefix(volumeId, bucketId);
      }

      // Directory or Key-Level: Check both key and directory
      BucketHandler handler =
          getBucketHandler(reconNamespaceSummaryManager, omMetadataManager, reconSCM, bucketInfo);

      if (names.length >= 3) {
        String lastEntiry = names[names.length - 1];

        // Check if the directory exists
        OmDirectoryInfo dirInfo = handler.getDirInfo(names);
        if (dirInfo != null && dirInfo.getName().equals(lastEntiry)) {
          return constructObjectPathWithPrefix(volumeId, bucketId, dirInfo.getObjectID()) + OM_KEY_PREFIX;
        }

        // Check if the key exists
        long dirID = handler.getDirObjectId(names, names.length);
        String keyKey = constructObjectPathWithPrefix(volumeId, bucketId, dirID) +
                OM_KEY_PREFIX + lastEntiry;
        OmKeyInfo keyInfo = openFileTable.getSkipCache(keyKey);
        if (keyInfo != null && keyInfo.getFileName().equals(lastEntiry)) {
          return constructObjectPathWithPrefix(volumeId, bucketId,
              keyInfo.getParentObjectID()) + OM_KEY_PREFIX + lastEntiry;
        }

        return prevKeyPrefix;
      }
    } catch (IllegalArgumentException e) {
      LOG.error(
          "IllegalArgumentException encountered while converting key prefix to object path: {}",
          prevKeyPrefix, e);
      throw e;
    } catch (RuntimeException e) {
      LOG.error(
          "RuntimeException encountered while converting key prefix to object path: {}",
          prevKeyPrefix, e);
      return prevKeyPrefix;
    }
    return prevKeyPrefix;
  }


  /**
   * Common method to retrieve keys from a table based on a search prefix and a limit.
   *
   * @param table       The table to retrieve keys from.
   * @param startPrefix The search prefix to match keys against.
   * @param limit       The maximum number of keys to retrieve.
   * @param prevKey     The key to start after for the next set of records.
   * @return A map of keys and their corresponding OmKeyInfo objects.
   * @throws IOException If there are problems accessing the table.
   */
  private Map<String, OmKeyInfo> retrieveKeysFromTable(
      Table<String, OmKeyInfo> table, String startPrefix, int limit, String prevKey)
      throws IOException {
    Map<String, OmKeyInfo> matchedKeys = new LinkedHashMap<>();
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> keyIter = table.iterator()) {
      // If a previous key is provided, seek to the previous key and skip it.
      if (!prevKey.isEmpty()) {
        keyIter.seek(prevKey);
        if (keyIter.hasNext()) {
          // Skip the previous key
          keyIter.next();
        }
      } else {
        // If no previous key is provided, start from the search prefix.
        keyIter.seek(startPrefix);
      }
      while (keyIter.hasNext() && matchedKeys.size() < limit) {
        Table.KeyValue<String, OmKeyInfo> entry = keyIter.next();
        String dbKey = entry.getKey();
        if (!dbKey.startsWith(startPrefix)) {
          break; // Exit the loop if the key no longer matches the prefix
        }
        matchedKeys.put(dbKey, entry.getValue());
      }
    } catch (IOException exception) {
      LOG.error("Error retrieving keys from table for path: {}", startPrefix, exception);
      throw exception;
    }
    return matchedKeys;
  }

  /**
   * Creates a KeyEntityInfo object from an OmKeyInfo object and the corresponding key.
   *
   * @param dbKey   The key in the database corresponding to the OmKeyInfo object.
   * @param keyInfo The OmKeyInfo object to create the KeyEntityInfo from.
   * @return The KeyEntityInfo object created from the OmKeyInfo object and the key.
   */
  private KeyEntityInfo createKeyEntityInfoFromOmKeyInfo(String dbKey,
                                                         OmKeyInfo keyInfo) {
    KeyEntityInfo keyEntityInfo = new KeyEntityInfo();
    keyEntityInfo.setKey(dbKey); // Set the DB key
    keyEntityInfo.setPath(keyInfo.getKeyName()); // Assuming path is the same as key name
    keyEntityInfo.setInStateSince(keyInfo.getCreationTime());
    keyEntityInfo.setSize(keyInfo.getDataSize());
    keyEntityInfo.setReplicatedSize(keyInfo.getReplicatedSize());
    keyEntityInfo.setReplicationConfig(keyInfo.getReplicationConfig());
    return keyEntityInfo;
  }

}
