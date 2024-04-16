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

package org.apache.hadoop.ozone.recon.api;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.handlers.EntityHandler;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.Stats;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_FETCH_COUNT;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_KEY_SIZE;

/**
 * REST APIs for namespace metadata summary.
 */
@Path("/namespace")
@Produces(MediaType.APPLICATION_JSON)
@AdminOnly
public class NSSummaryEndpoint {
  private static final Logger LOG = LoggerFactory.getLogger(
      NSSummaryEndpoint.class);

  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;

  private final ReconOMMetadataManager omMetadataManager;

  private final OzoneStorageContainerManager reconSCM;
  @Inject
  public NSSummaryEndpoint(ReconNamespaceSummaryManager namespaceSummaryManager,
                           ReconOMMetadataManager omMetadataManager,
                           OzoneStorageContainerManager reconSCM) {
    this.reconNamespaceSummaryManager = namespaceSummaryManager;
    this.omMetadataManager = omMetadataManager;
    this.reconSCM = reconSCM;
  }

  /**
   * This endpoint will return the entity type and aggregate count of objects.
   * @param path the request path.
   * @return HTTP response with basic info: entity type, num of objects
   * @throws IOException IOE
   */
  @GET
  @Path("/summary")
  public Response getBasicInfo(
      @QueryParam("path") String path) throws IOException {

    if (path == null || path.length() == 0) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    NamespaceSummaryResponse namespaceSummaryResponse;
    if (!isInitializationComplete()) {
      namespaceSummaryResponse =
          NamespaceSummaryResponse.newBuilder()
              .setEntityType(EntityType.UNKNOWN)
              .setStatus(ResponseStatus.INITIALIZING)
              .build();
      return Response.ok(namespaceSummaryResponse).build();
    }

    EntityHandler handler = EntityHandler.getEntityHandler(
            reconNamespaceSummaryManager,
            omMetadataManager, reconSCM, path);

    namespaceSummaryResponse = handler.getSummaryResponse();

    return Response.ok(namespaceSummaryResponse).build();
  }

  /**
   * DU endpoint to return datasize for subdirectory (bucket for volume).
   * @param path request path
   * @param listFile show subpath/disk usage for each key
   * @param withReplica count actual DU with replication
   * @return DU response
   * @throws IOException
   */
  @GET
  @Path("/du")
  @SuppressWarnings("methodlength")
  public Response getDiskUsage(@QueryParam("path") String path,
                               @DefaultValue("false")
                               @QueryParam("files") boolean listFile,
                               @DefaultValue("false")
                               @QueryParam("replica") boolean withReplica)
      throws IOException {
    if (path == null || path.length() == 0) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    DUResponse duResponse = new DUResponse();
    if (!isInitializationComplete()) {
      duResponse.setStatus(ResponseStatus.INITIALIZING);
      return Response.ok(duResponse).build();
    }

    EntityHandler handler = EntityHandler.getEntityHandler(
            reconNamespaceSummaryManager,
            omMetadataManager, reconSCM, path);

    duResponse = handler.getDuResponse(
            listFile, withReplica, false, new Stats(-1));

    return Response.ok(duResponse).build();
  }

  /**
   * This API will list out limited 'count' number of keys after applying below filters in API parameters:
   * Default Values of API param filters:
   *    -- replicationType - empty string and filter will not be applied, so list out all keys irrespective of
   *       replication type.
   *    -- creationTime - empty string and filter will not be applied, so list out keys irrespective of age.
   *    -- keySize - 0 bytes, which means all keys greater than zero bytes will be listed, effectively all.
   *    -- startPrefix - /
   *    -- count - 1000
   *
   * @param replicationType Filter for RATIS or EC replication keys
   * @param creationDate Filter for keys created after creationDate in "MM-dd-yyyy HH:mm:ss" string format.
   * @param keySize Filter for Keys greater than keySize in bytes.
   * @param startPrefix Filter for startPrefix path.
   * @param limit Filter for limited count of keys.
   * @param recursive listing out keys recursively for FSO buckets.
   * @return the list of keys in below structured format:
   * Response For OBS Bucket keys:
   * ********************************************************
   * {
   *     "status": "OK",
   *     "path": "/volume1/obs-bucket/",
   *     "size": 73400320,
   *     "sizeWithReplica": 81788928,
   *     "subPathCount": 1,
   *     "totalKeyCount": 7,
   *     "lastKey": "/volume1/obs-bucket/key7",
   *     "subPaths": [
   *         {
   *             "key": true,
   *             "path": "key1",
   *             "size": 10485760,
   *             "sizeWithReplica": 18874368,
   *             "isKey": true,
   *             "replicationType": "RATIS",
   *             "creationTime": 1712321367060,
   *             "modificationTime": 1712321368190
   *         },
   *         {
   *             "key": true,
   *             "path": "key7",
   *             "size": 10485760,
   *             "sizeWithReplica": 18874368,
   *             "isKey": true,
   *             "replicationType": "EC",
   *             "creationTime": 1713261005555,
   *             "modificationTime": 1713261006728
   *         }
   *     ],
   *     "sizeDirectKey": 73400320
   * }
   * ********************************************************
   * @throws IOException
   */
  @GET
  @Path("/listKeys")
  @SuppressWarnings("methodlength")
  public Response listKeysWithDu(@QueryParam("replicationType") String replicationType,
                                 @QueryParam("creationDate") String creationDate,
                                 @DefaultValue(DEFAULT_KEY_SIZE) @QueryParam("keySize") long keySize,
                                 @DefaultValue(OM_KEY_PREFIX) @QueryParam("startPrefix") String startPrefix,
                                 @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam("count") long limit,
                                 @DefaultValue("false") @QueryParam("recursive") boolean recursive)
      throws IOException {

    if (startPrefix == null || startPrefix.length() == 0) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    DUResponse duResponse = new DUResponse();
    if (!isInitializationComplete()) {
      duResponse.setStatus(ResponseStatus.INITIALIZING);
      return Response.ok(duResponse).build();
    }
    EntityHandler handler = EntityHandler.getEntityHandler(
        reconNamespaceSummaryManager,
        omMetadataManager, reconSCM, startPrefix);

    Stats stats = new Stats(limit);

    duResponse = handler.getListKeysResponse(stats, recursive);

    List<DUResponse.DiskUsage> keyListWithDu = duResponse.getDuData();

    long epochMillis = ReconUtils.convertToEpochMillis(creationDate, "MM-dd-yyyy HH:mm:ss", TimeZone.getDefault());
    Predicate<DUResponse.DiskUsage> keyAgeFilter = keyData -> keyData.getCreationTime() >= epochMillis;
    Predicate<DUResponse.DiskUsage> keyReplicationFilter =
        keyData -> keyData.getReplicationType().equals(replicationType);
    Predicate<DUResponse.DiskUsage> keySizeFilter = keyData -> keyData.getSize() > keySize;
    Predicate<DUResponse.DiskUsage> keyFilter = keyData -> keyData.isKey();

    List<DUResponse.DiskUsage> filteredKeyList = keyListWithDu.stream()
        .filter(keyFilter)
        .filter(keyData -> !StringUtils.isEmpty(creationDate) ? keyAgeFilter.test(keyData) : true)
        .filter(keyData -> !StringUtils.isEmpty(replicationType) ? keyReplicationFilter.test(keyData) : true)
        .filter(keySizeFilter)
        .collect(Collectors.toList());

    duResponse.setDuData(filteredKeyList);
    duResponse.setCount(filteredKeyList.size());
    return Response.ok(duResponse).build();
  }

  /**
   * Quota usage endpoint that summarize the quota allowed and quota used in
   * bytes.
   * @param path request path
   * @return Quota Usage response
   * @throws IOException
   */
  @GET
  @Path("/quota")
  public Response getQuotaUsage(@QueryParam("path") String path)
      throws IOException {

    if (path == null || path.length() == 0) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
    if (!isInitializationComplete()) {
      quotaUsageResponse.setResponseCode(ResponseStatus.INITIALIZING);
      return Response.ok(quotaUsageResponse).build();
    }

    EntityHandler handler = EntityHandler.getEntityHandler(
            reconNamespaceSummaryManager,
            omMetadataManager, reconSCM, path);

    quotaUsageResponse = handler.getQuotaResponse();

    return Response.ok(quotaUsageResponse).build();
  }

  /**
   * Endpoint that returns aggregate file size distribution under a path.
   * @param path request path
   * @return File size distribution response
   * @throws IOException
   */
  @GET
  @Path("/dist")
  public Response getFileSizeDistribution(@QueryParam("path") String path)
      throws IOException {

    if (path == null || path.length() == 0) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    FileSizeDistributionResponse distResponse =
        new FileSizeDistributionResponse();
    if (!isInitializationComplete()) {
      distResponse.setStatus(ResponseStatus.INITIALIZING);
      return Response.ok(distResponse).build();
    }

    EntityHandler handler = EntityHandler.getEntityHandler(
            reconNamespaceSummaryManager,
            omMetadataManager, reconSCM, path);

    distResponse = handler.getDistResponse();

    return Response.ok(distResponse).build();
  }

  /**
   * Return if all OMDB tables that will be used are initialized.
   * @return if tables are initialized
   */
  private boolean isInitializationComplete() {
    if (omMetadataManager == null) {
      return false;
    }
    return omMetadataManager.getVolumeTable() != null
        && omMetadataManager.getBucketTable() != null
        && omMetadataManager.getDirectoryTable() != null
        && omMetadataManager.getFileTable() != null
        && omMetadataManager.getKeyTable(BucketLayout.LEGACY) != null;
  }

}
