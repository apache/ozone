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

import org.antlr.v4.runtime.misc.Pair;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.recon.api.handlers.BucketHandler;
import org.apache.hadoop.ozone.recon.api.types.KeyEntityInfo;
import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
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
import java.util.Set;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.recon.ReconConstants.*;
import static org.apache.hadoop.ozone.recon.api.handlers.BucketHandler.getBucketHandler;
import static org.apache.hadoop.ozone.recon.api.handlers.EntityHandler.normalizePath;
import static org.apache.hadoop.ozone.recon.api.handlers.EntityHandler.parseRequestPath;

/**
 * REST endpoint for search implementation in OM DB Insight.
 */
@Path("/insights")
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
   * This endpoint can search across both File System Optimized (FSO) and Object Store (non-FSO) layouts,
   * compiling a list of keys that match the given prefix along with their data sizes.
   *
   * The search prefix may range from the root level ('/') to any specific directory
   * or key level (e.g., '/volA/' for everything under 'volA'). The search operation matches
   * the prefix against the start of keys' names within the OM DB.
   *
   * Example Usage:
   * 1. A startPrefix of "/" will return all keys in the database.
   * 2. A startPrefix of "/volA/" retrieves every key under volume 'volA'.
   * 3. Specifying "/volA/bucketA/dir1" focuses the search within 'dir1' inside 'bucketA' of 'volA'.
   *
   * @param startPrefix   The prefix for searching keys, starting from the root ('/') or any specific path.
   * @param includeFso    Indicates whether to include FSO layout keys in the search.
   * @param includeNonFso Indicates whether to include non-FSO layout keys in the search.
   * @param limit         Limits the number of returned keys.
   * @return A KeyInsightInfoResponse, containing matching keys and their data sizes.
   * @throws IOException On failure to access the OM database or process the operation.
   */
  @GET
  @Path("/openKeys/search")
  public Response searchOpenKeys(
      @DefaultValue(DEFAULT_SEARCH_PREFIX) @QueryParam("startPrefix")
      String startPrefix,
      @DefaultValue(DEFAULT_OPEN_KEY_INCLUDE_FSO) @QueryParam("includeFso")
      boolean includeFso,
      @DefaultValue(DEFAULT_OPEN_KEY_INCLUDE_NON_FSO) @QueryParam("includeNonFso")
      boolean includeNonFso,
      @DefaultValue(RECON_OPEN_KEY_SEARCH_LIMIT) @QueryParam("limit")
      int limit) throws IOException {
    if (limit < 0) {
      return createBadRequestResponse("Limit cannot be negative.");
    }

    KeyInsightInfoResponse insightResponse = new KeyInsightInfoResponse();
    long replicatedTotal = 0;
    long unreplicatedTotal = 0;
    boolean keysFound = false; // Flag to track if any keys are found

    // Fetch keys from OBS layout and convert them into KeyEntityInfo objects
    Map<String, OmKeyInfo> obsKeys = new LinkedHashMap<>();
    if (includeNonFso) {
      Table<String, OmKeyInfo> openKeyTable = omMetadataManager.getOpenKeyTable(BucketLayout.LEGACY);
      obsKeys = retrieveKeysFromTable(openKeyTable, startPrefix, limit);
      for (Map.Entry<String, OmKeyInfo> entry : obsKeys.entrySet()) {
        keysFound = true;
        KeyEntityInfo keyEntityInfo =
            createKeyEntityInfoFromOmKeyInfo(entry.getKey(), entry.getValue());
        insightResponse.getNonFSOKeyInfoList()
            .add(keyEntityInfo); // Add to non-FSO list
        replicatedTotal += entry.getValue().getReplicatedSize();
        unreplicatedTotal += entry.getValue().getDataSize();
      }
    }

    // Fetch keys from FSO layout, if the limit is not yet reached
    if (includeFso) {
      Map<String, OmKeyInfo> fsoKeys = searchOpenKeysInFSO(startPrefix, limit);
      for (Map.Entry<String, OmKeyInfo> entry : fsoKeys.entrySet()) {
        keysFound = true;
        KeyEntityInfo keyEntityInfo =
            createKeyEntityInfoFromOmKeyInfo(entry.getKey(), entry.getValue());
        insightResponse.getFsoKeyInfoList()
            .add(keyEntityInfo); // Add to FSO list
        replicatedTotal += entry.getValue().getReplicatedSize();
        unreplicatedTotal += entry.getValue().getDataSize();
      }
    }

    // If no keys were found, return a response indicating that no keys matched
    if (!keysFound) {
      return noMatchedKeysResponse(startPrefix);
    }

    // Set the aggregated totals in the response
    insightResponse.setReplicatedDataSize(replicatedTotal);
    insightResponse.setUnreplicatedDataSize(unreplicatedTotal);

    return Response.ok(insightResponse).build();
  }

  public Map<String, OmKeyInfo> searchOpenKeysInFSO(String startPrefix,
                                                    int limit)
      throws IOException {
    Map<String, OmKeyInfo> matchedKeys = new LinkedHashMap<>();
    // Convert the search prefix to an object path for FSO buckets
    String startPrefixObjectPath = convertToObjectPath(startPrefix);
    String[] names = parseRequestPath(startPrefixObjectPath);
    Table<String, OmKeyInfo> openFileTable = omMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // If names.length > 2, then the search prefix is at the volume or bucket level hence
    // no need to find parent or extract id's or find subpaths as the openFileTable is
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
      // Add the initial search prefix object path because it can have both openFiles
      // and sub-directories with openFiles
      subPaths.add(startPrefixObjectPath);

      // Recursively gather all subpaths
      gatherSubPaths(parentId, subPaths, names);

      // Iterate over the subpaths and retrieve the open files
      for (String subPath : subPaths) {
        matchedKeys.putAll(
            retrieveKeysFromTable(openFileTable, subPath, limit - matchedKeys.size()));
        if (matchedKeys.size() >= limit) {
          break;
        }
      }
      return matchedKeys;
    }

    // Iterate over for bucket and volume level search
    matchedKeys.putAll(retrieveKeysFromTable(openFileTable, startPrefixObjectPath, limit));
    return matchedKeys;
  }

  /**
   * Finds all subdirectories under a parent directory in an FSO bucket. It builds
   * a list of paths for these subdirectories. These sub-directories are then used
   * to search for open files in the openFileTable.
   *
   * How it works:
   * - Starts from a parent directory identified by parentId.
   * - Looks through all child directories of this parent.
   * - For each child, it creates a path that starts with volumeID/bucketID/parentId,
   * following our openFileTable format
   * - Adds these paths to a list and explores each child further for more subdirectories.
   *
   * @param parentId The ID of the directory we start exploring from.
   * @param subPaths A list where we collect paths to all subdirectories.
   * @param names    An array with at least two elements: the first is volumeID and
   *                 the second is bucketID. These are used to start each path.
   * @throws IOException If there are problems accessing directory information.
   */
  private void gatherSubPaths(long parentId, List<String> subPaths,
                              String[] names) throws IOException {
    // Fetch the NSSummary object for parentId
    NSSummary parentSummary =
        reconNamespaceSummaryManager.getNSSummary(parentId);
    if (parentSummary == null) {
      return;
    }

    Set<Long> childDirIds = parentSummary.getChildDir();
    for (Long childId : childDirIds) {
      // Fetch the NSSummary for each child directory
      NSSummary childSummary =
          reconNamespaceSummaryManager.getNSSummary(childId);
      if (childSummary != null) {
        long volumeID = Long.parseLong(names[0]);
        long bucketID = Long.parseLong(names[1]);
        String subPath =
            constructObjectPathWithPrefix(volumeID, bucketID, childId);
        // Add to subPaths
        subPaths.add(subPath);
        // Recurse into this child directory
        gatherSubPaths(childId, subPaths, names);
      }
    }
  }


  /**
   * Converts a key prefix into an object path for FSO buckets, using IDs.
   * <p>
   * This method transforms a user-provided path (e.g., "volume/bucket/dir1") into
   * a database-friendly format ("/volumeID/bucketID/ParentId/") by replacing names
   * with their corresponding IDs. It simplifies database queries for FSO bucket operations.
   *
   * @param prevKeyPrefix The path to be converted, not including key or directory names/IDs.
   * @return The object path as "/volumeID/bucketID/ParentId/".
   * @throws IOException If database access fails.
   */
  public String convertToObjectPath(String prevKeyPrefix) {
    if (prevKeyPrefix.isEmpty()) {
      return "";
    }

    try {
      String[] names = parseRequestPath(normalizePath(prevKeyPrefix));

      // Fetch the volumeID
      String volumeName = names[0];
      String volumeKey = omMetadataManager.getVolumeKey(volumeName);
      long volumeId = omMetadataManager.getVolumeTable().getSkipCache(volumeKey).getObjectID();
      if (names.length == 1) {
        return constructObjectPathWithPrefix(volumeId);
      }

      // Fetch the bucketID
      String bucketName = names[1];
      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
      OmBucketInfo bucketInfo =
          omMetadataManager.getBucketTable().getSkipCache(bucketKey);
      long bucketId = bucketInfo.getObjectID();
      if (names.length == 2) {
        return constructObjectPathWithPrefix(volumeId, bucketId);
      }

      // Fetch the immediate parentID which could be a directory or the bucket itself
      BucketHandler handler =
          getBucketHandler(reconNamespaceSummaryManager, omMetadataManager, reconSCM, bucketInfo);
      long dirObjectId = handler.getDirInfo(names).getObjectID();
      return constructObjectPathWithPrefix(volumeId, bucketId, dirObjectId);

    } catch (IOException e) {
      LOG.error("Error converting key prefix to object path: {}", prevKeyPrefix, e);
      return prevKeyPrefix; // Fallback to original prefix in case of exception
    } catch (Exception e) {
      LOG.error("Unexpected error during conversion: {}", prevKeyPrefix, e);
      return prevKeyPrefix;
    }
  }

  /**
   * Common method to retrieve keys from a table based on a search prefix and a limit.
   *
   * @param table       The table to retrieve keys from.
   * @param startPrefix The search prefix to match keys against.
   * @param limit       The maximum number of keys to retrieve.
   * @return A map of keys and their corresponding OmKeyInfo objects.
   * @throws IOException If there are problems accessing the table.
   */
  private Map<String, OmKeyInfo> retrieveKeysFromTable(
      Table<String, OmKeyInfo> table, String startPrefix, int limit)
      throws IOException {
    Map<String, OmKeyInfo> matchedKeys = new LinkedHashMap<>();
    try (
        TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> keyIter = table.iterator()) {
      keyIter.seek(startPrefix);
      while (keyIter.hasNext() && matchedKeys.size() < limit) {
        Table.KeyValue<String, OmKeyInfo> entry = keyIter.next();
        String dbKey = entry.getKey();
        if (!dbKey.startsWith(startPrefix)) {
          break; // Exit the loop if the key no longer matches the prefix
        }
        matchedKeys.put(dbKey, entry.getValue());
      }
    } catch (IOException exception) {
      LOG.error("Error retrieving keys from table for path: {}", startPrefix,
          exception);
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
    keyEntityInfo.setPath(
        keyInfo.getKeyName()); // Assuming path is the same as key name
    keyEntityInfo.setInStateSince(keyInfo.getCreationTime());
    keyEntityInfo.setSize(keyInfo.getDataSize());
    keyEntityInfo.setReplicatedSize(keyInfo.getReplicatedSize());
    keyEntityInfo.setReplicationConfig(keyInfo.getReplicationConfig());
    return keyEntityInfo;
  }

  /**
   * Constructs an object path with the given IDs.
   *
   * @param ids The IDs to construct the object path with.
   * @return The constructed object path.
   */
  private String constructObjectPathWithPrefix(long... ids) {
    StringBuilder pathBuilder = new StringBuilder();
    for (long id : ids) {
      pathBuilder.append(OM_KEY_PREFIX).append(id);
    }
    return pathBuilder.toString();
  }

  /**
   * Returns a response indicating that no keys matched the search prefix.
   *
   * @param startPrefix The search prefix that was used.
   * @return The response indicating that no keys matched the search prefix.
   */
  private Response noMatchedKeysResponse(String startPrefix) {
    String jsonResponse = String.format(
        "{\"message\": \"No keys matched the search prefix: '%s'.\"}",
        startPrefix);
    return Response.status(Response.Status.NOT_FOUND)
        .entity(jsonResponse)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  /**
   * Utility method to create a bad request response with a custom message.
   * Which means the request sent by the client to the server is incorrect
   * or malformed and cannot be processed by the server.
   *
   * @param message The message to include in the response body.
   * @return A Response object configured with the provided message.
   */
  private Response createBadRequestResponse(String message) {
    String jsonResponse = String.format("{\"message\": \"%s\"}", message);
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(jsonResponse)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  /**
   * Utility method to create an internal server error response with a custom message.
   * Which means the server encountered an unexpected condition that prevented it
   * from fulfilling the request.
   *
   * @param message The message to include in the response body.
   * @return A Response object configured with the provided message.
   */
  private Response createInternalServerErrorResponse(String message) {
    String jsonResponse = String.format("{\"message\": \"%s\"}", message);
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(jsonResponse)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

}
