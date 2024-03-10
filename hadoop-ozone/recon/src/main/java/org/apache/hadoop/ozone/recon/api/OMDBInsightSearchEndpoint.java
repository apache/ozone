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
import org.apache.hadoop.ozone.om.helpers.*;
import org.apache.hadoop.ozone.recon.api.handlers.BucketHandler;
import org.apache.hadoop.ozone.recon.api.types.KeyEntityInfo;
import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconNamespaceSummaryManagerImpl;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
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
import java.util.*;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.recon.ReconConstants.*;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OPEN_KEY_INCLUDE_NON_FSO;
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
  private final ReconContainerManager containerManager;
  private static final Logger LOG =
      LoggerFactory.getLogger(OMDBInsightSearchEndpoint.class);
  private ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager;


  @Inject
  public OMDBInsightSearchEndpoint(OzoneStorageContainerManager reconSCM,
                                   ReconOMMetadataManager omMetadataManager,
                                   ReconNamespaceSummaryManagerImpl
                                       reconNamespaceSummaryManager) {
    this.reconSCM = reconSCM;
    this.containerManager =
        (ReconContainerManager) reconSCM.getContainerManager();
    this.omMetadataManager = omMetadataManager;
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
  }


  /**
   * Search for open keys in the OM DB Insight.
   *
   * @param searchPrefix The prefix to search for.
   * @param limit The maximum number of keys to return.
   * @return The response containing the matched keys.
   * @throws IOException if an error occurs during the search.
   */
  @GET
  @Path("/openKeys/search")
  public Response searchOpenKeys(
      @QueryParam("searchPrefix")
      String searchPrefix,
      @DefaultValue(DEFAULT_OPEN_KEY_INCLUDE_FSO) @QueryParam(RECON_OPEN_KEY_INCLUDE_FSO)
      boolean includeFso,
      @DefaultValue(DEFAULT_OPEN_KEY_INCLUDE_NON_FSO) @QueryParam(RECON_OPEN_KEY_INCLUDE_NON_FSO)
      boolean includeNonFso,
      @DefaultValue("10") @QueryParam("limit")
      int limit) throws IOException {
    if (searchPrefix == null || searchPrefix.trim().isEmpty()) {
      return createBadRequestResponse(
          "The searchPrefix query parameter is required.");
    }

    KeyInsightInfoResponse insightResponse = new KeyInsightInfoResponse();
    long replicatedTotal = 0;
    long unreplicatedTotal = 0;

    // Fetch keys from OBS layout and convert them into KeyEntityInfo objects
    Map<String, OmKeyInfo> obsKeys = null;
    if (includeNonFso) {
      obsKeys = searchOpenKeysInOBS(searchPrefix, limit);
      for (Map.Entry<String, OmKeyInfo> entry : obsKeys.entrySet()) {
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
      Map<String, OmKeyInfo> fsoKeys =
          searchOpenKeysInFSO(searchPrefix, limit - obsKeys.size());
      for (Map.Entry<String, OmKeyInfo> entry : fsoKeys.entrySet()) {
        KeyEntityInfo keyEntityInfo =
            createKeyEntityInfoFromOmKeyInfo(entry.getKey(), entry.getValue());
        insightResponse.getFsoKeyInfoList()
            .add(keyEntityInfo); // Add to FSO list
        replicatedTotal += entry.getValue().getReplicatedSize();
        unreplicatedTotal += entry.getValue().getDataSize();
      }
    }

    // Set the aggregated totals in the response
    insightResponse.setReplicatedDataSize(replicatedTotal);
    insightResponse.setUnreplicatedDataSize(unreplicatedTotal);

    return Response.ok(insightResponse).build();
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


  public Map<String, OmKeyInfo> searchOpenKeysInOBS(String searchPrefix,
                                                    int limit)
      throws IOException {

    Map<String, OmKeyInfo> matchedKeys =
        new LinkedHashMap<>(); // Preserves the insertion order
    Table<String, OmKeyInfo> openKeyTable =
        omMetadataManager.getOpenKeyTable(BucketLayout.LEGACY);

    try (
        TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
            keyIter = openKeyTable.iterator()) {
      keyIter.seek(searchPrefix);
      while (keyIter.hasNext() && matchedKeys.size() < limit) {
        Table.KeyValue<String, OmKeyInfo> entry = keyIter.next();
        String dbKey = entry.getKey(); // Get the DB key
        if (!dbKey.startsWith(searchPrefix)) {
          break; // Exit the loop if the key no longer matches the prefix
        }
        // Add the DB key and OmKeyInfo object to the map
        matchedKeys.put(dbKey, entry.getValue());
      }
    } catch (NullPointerException | IOException exception) {
      LOG.error("Error retrieving keys from openKeyTable for path: {}",
          searchPrefix, exception);
    }

    return matchedKeys;
  }


  public Map<String, OmKeyInfo> searchOpenKeysInFSO(String searchPrefix,
                                                    int limit)
      throws IOException {
    Map<String, OmKeyInfo> matchedKeys = new LinkedHashMap<>();
    // Convert the search prefix to an object path for FSO buckets
    String searchPrefixObjectPath = convertToObjectPath(searchPrefix);
    String[] names = parseRequestPath(searchPrefixObjectPath);

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
      subPaths.add(searchPrefixObjectPath);

      // Recursively gather all subpaths
      gatherSubPaths(parentId, subPaths, names);

      // Iterate over the subpaths and retrieve the open files
      for (String subPath : subPaths) {
        matchedKeys.putAll(
            retrieveKeysFromOpenFileTable(subPath, limit - matchedKeys.size()));
        if (matchedKeys.size() >= limit) {
          break;
        }
      }
      return matchedKeys;
    }

    // Iterate over for bucket and volume level search
    matchedKeys.putAll(retrieveKeysFromOpenFileTable(searchPrefixObjectPath,
        limit - matchedKeys.size()));
    return matchedKeys;
  }


  private Map<String, OmKeyInfo> retrieveKeysFromOpenFileTable(String subPath,
                                                               int limit)
      throws IOException {

    Map<String, OmKeyInfo> matchedKeys =
        new LinkedHashMap<>(); // Preserves the insertion order
    Table<String, OmKeyInfo> openFileTable =
        omMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);

    try (
        TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> keyIter =
            openFileTable.iterator()) {
      keyIter.seek(subPath);
      while (keyIter.hasNext() && matchedKeys.size() < limit) {
        Table.KeyValue<String, OmKeyInfo> entry = keyIter.next();
        String dbKey = entry.getKey(); // Get the DB key
        if (!dbKey.startsWith(subPath)) {
          break; // Exit the loop if the key no longer matches the prefix
        }
        // Add the DB key and OmKeyInfo object to the map
        matchedKeys.put(dbKey, entry.getValue());
      }
    } catch (NullPointerException | IOException exception) {
      LOG.error("Error retrieving keys from openFileTable for path: {}",
          subPath, exception);
    }
    return matchedKeys;
  }


  /**
   * Finds all subdirectories under a parent directory in an FSO bucket. It builds
   * a list of paths for these subdirectories. These sub-directories are then used
   * to search for open files in the openFileTable.
   * <p>
   * How it works:
   * - Starts from a parent directory identified by parentId.
   * - Looks through all child directories of this parent.
   * - For each child, it creates a path that starts with volumeID/bucketID/parentId,
   * following our openFileTable format
   * - Adds these paths to a list and explores each child further for more subdirectories.
   *
   * @param parentId The ID of the directory we start exploring from.
   * @param subPaths A list where we collect paths to all subdirectories.
   * @param names    An array with at least two elements: the first is volumeID and the second is bucketID.
   *                 These are used to start each path.
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
  public String convertToObjectPath(String prevKeyPrefix) throws IOException {
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

  private long getParentId(BucketHandler handler, String[] names,
                           String bucketName, long bucketId)
      throws IOException {
    String parentName = names[names.length - 2];
    if (bucketName.equals(parentName) && names.length == 3) {
      return bucketId;
    }
    return handler.getDirObjectId(names, names.length - 1);
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
   * @param searchPrefix The search prefix that was used.
   * @return The response indicating that no keys matched the search prefix.
   */
  private Response noMatchedKeysResponse(String searchPrefix) {
    String message =
        "{\"message\": \"No keys exist for the specified search prefix";
    message += ".\"}";
    return Response.status(Response.Status.NOT_FOUND)
        .entity(message)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  /**
   * Utility method to create a bad request response with a custom message.
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

}
