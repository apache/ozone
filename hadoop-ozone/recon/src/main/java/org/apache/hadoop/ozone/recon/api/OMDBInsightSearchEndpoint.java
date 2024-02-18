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
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.api.handlers.BucketHandler;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.recon.api.handlers.BucketHandler.getBucketHandler;
import static org.apache.hadoop.ozone.recon.api.handlers.EntityHandler.parseRequestPath;

/**
 * REST endpoint for search implementation in OM DB Insight.
 */
@Path("/insights")
@Produces(MediaType.APPLICATION_JSON)
@AdminOnly
public class OMDBInsightSearchEndpoint {

  @Inject
  private ContainerEndpoint containerEndpoint;
  private OzoneStorageContainerManager reconSCM;
  @Inject
  private ReconContainerMetadataManager reconContainerMetadataManager;
  private final ReconOMMetadataManager omMetadataManager;
  private final ReconContainerManager containerManager;
  private static final Logger LOG =
      LoggerFactory.getLogger(OMDBInsightSearchEndpoint.class);
  private final GlobalStatsDao globalStatsDao;
  private ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager;


  @Inject
  public OMDBInsightSearchEndpoint(OzoneStorageContainerManager reconSCM,
                             ReconOMMetadataManager omMetadataManager,
                             GlobalStatsDao globalStatsDao,
                             ReconNamespaceSummaryManagerImpl
                                 reconNamespaceSummaryManager) {
    this.reconSCM = reconSCM;
    this.containerManager =
        (ReconContainerManager) reconSCM.getContainerManager();
    this.omMetadataManager = omMetadataManager;
    this.globalStatsDao = globalStatsDao;
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
      @QueryParam("searchPrefix") String searchPrefix,
      @DefaultValue("10") @QueryParam("limit") int limit)
      throws IOException {

    if (searchPrefix == null || searchPrefix.isEmpty()) {
      return createBadRequestResponse(
          "searchPrefix query parameter is required.");
    }
    List<OmKeyInfo> matchedKeys = new ArrayList<>();

    for (BucketLayout layout : Arrays.asList(
        BucketLayout.LEGACY, BucketLayout.FILE_SYSTEM_OPTIMIZED)) {

      Table<String, OmKeyInfo> openKeyTable =
          omMetadataManager.getOpenKeyTable(layout);
      try (
          TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
              keyIter = openKeyTable.iterator()) {
        if (layout == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
          searchPrefix = convertToObjectPath(searchPrefix);
        }
        keyIter.seek(searchPrefix);
        while (keyIter.hasNext() && matchedKeys.size() < limit) {
          Table.KeyValue<String, OmKeyInfo> entry = keyIter.next();
          String key = entry.getKey();
          // Break if the key no longer matches the prefix
          if (!key.startsWith(searchPrefix)) {
            break;
          }
          OmKeyInfo omKeyInfo = entry.getValue();
          // Add it to the list of matched keys.
          matchedKeys.add(omKeyInfo);
        }
      } catch (Exception e) {
        return noMatchedKeysResponse(searchPrefix);
      }
    }
    if (matchedKeys.isEmpty()) {
      return noMatchedKeysResponse(searchPrefix);
    } else {
      return Response.ok(matchedKeys).build();
    }
  }

  /**
   * Converts a given key prefix to an object path for FSO buckets.
   * The conversion is necessary because keys in FSO buckets are stored in a
   * object format in the OpenFileTable,
   * e.g., "/volumeId/bucketId/parentId/fileName/id -> KeyInfo".
   *
   * @param prevKeyPrefix The key prefix to convert.
   * @return The object path for the given key prefix.
   * @throws IOException if an error occurs during conversion.
   */
  public String convertToObjectPath(String prevKeyPrefix) throws IOException {
    if (prevKeyPrefix.isEmpty()) {
      return "";
    }

    // Fetch the volumeID
    try {
      String[] names = parseRequestPath(normalizePath(prevKeyPrefix));
      String volumeName = names[0];
      String volumeKey = omMetadataManager.getVolumeKey(volumeName);
      long volumeId =
          omMetadataManager.getVolumeTable().getSkipCache(volumeKey).getObjectID();

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

      // Fetch the intermediate parentID
      BucketHandler handler =
          getBucketHandler(reconNamespaceSummaryManager, omMetadataManager,
              reconSCM, bucketInfo);
      long parentId = getParentId(handler, names, bucketName, bucketId);
      String keyName = names[names.length - 1];
      return constructObjectPathWithPrefix(volumeId, bucketId, parentId) +
          OM_KEY_PREFIX + keyName;
    } catch (IOException e) {
      LOG.error("Error converting key prefix to object path: {}", prevKeyPrefix,
          e);
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

  private static String normalizePath(String path) {
    return OM_KEY_PREFIX + OmUtils.normalizeKey(path, false);
  }

}
