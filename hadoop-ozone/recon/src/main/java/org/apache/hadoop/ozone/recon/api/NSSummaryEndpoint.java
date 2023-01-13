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

import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.recon.api.handlers.EntityHandler;
import org.apache.hadoop.ozone.recon.api.types.CountStats;
import org.apache.hadoop.ozone.recon.api.types.EntityInfoResponse;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.ObjectDBInfo;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * REST APIs for namespace metadata summary.
 */
@Path("/namespace")
@Produces(MediaType.APPLICATION_JSON)
@AdminOnly
public class NSSummaryEndpoint {

  public static final String COUNT = "count";
  public static final String SIZE = "size";
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

  @GET
  @Path("/entityinfo")
  public Response getEntityMetrics(@QueryParam("path") String path,
                                   @DefaultValue("createTime")
                                   @QueryParam("orderBy") String orderBy,
                                   @DefaultValue("10")
                                   @QueryParam("count") int count)
      throws IOException {
    if (path == null || path.length() == 0) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    EntityInfoResponse entityInfoResponse;
    if (!isInitializationComplete()) {
      entityInfoResponse =
          new EntityInfoResponse(EntityType.UNKNOWN);
      entityInfoResponse.setStatus(ResponseStatus.INITIALIZING);
    } else {
      NamespaceSummaryResponse nsSummaryData =
          getNamespaceSummaryResponse(path);
      DUResponse duResponse = getDuResponse(path, true, true);

      EntityType entityType = nsSummaryData.getEntityType();
      entityInfoResponse = new EntityInfoResponse(entityType);
      Map<String, List<DUResponse.DiskUsage>> childMetricsListMap =
          entityInfoResponse.getChildMetricsListMap();

      if (COUNT.equalsIgnoreCase(orderBy)) {
        switch (entityType) {
        case ROOT:
          childMetricsListMap.put(EntityType.VOLUME.name(),
              duResponse.getDuData()
              .stream().sorted(Comparator.comparingLong(
                  DUResponse.DiskUsage::getVolumeCount).reversed())
              .limit(count).collect(Collectors.toList()));
          break;
        case VOLUME:
          childMetricsListMap.put(EntityType.BUCKET.name(),
              duResponse.getDuData()
              .stream().sorted(Comparator.comparingLong(
                  DUResponse.DiskUsage::getBucketCount).reversed())
              .limit(count).collect(Collectors.toList()));
          break;
        case BUCKET:
        case DIRECTORY:
          List<DUResponse.DiskUsage> keys = getKeys(duResponse.getDuData());
          List<DUResponse.DiskUsage> dirs = getDirs(duResponse.getDuData());
          childMetricsListMap.put(EntityType.KEY.name(),
              keys.stream().sorted(Comparator.comparingLong(
                      DUResponse.DiskUsage::getKeyCount).reversed())
                  .limit(count).collect(Collectors.toList()));
          childMetricsListMap.put(EntityType.DIRECTORY.name(),
              dirs.stream().sorted(Comparator.comparingLong(
                      DUResponse.DiskUsage::getKeyCount).reversed())
                  .limit(count).collect(Collectors.toList()));
          break;
        default:
          childMetricsListMap.put(EntityType.UNKNOWN.name(),
              duResponse.getDuData()
              .stream().sorted(Comparator.comparingLong(
                  DUResponse.DiskUsage::getKeyCount).reversed())
              .limit(count).collect(Collectors.toList()));
          break;
        }
      } else if (SIZE.equalsIgnoreCase(orderBy)) {
        switch (entityType) {
        case ROOT:
          childMetricsListMap.put(EntityType.VOLUME.name(),
              duResponse.getDuData()
                  .stream().sorted(Comparator.comparingLong(
                      DUResponse.DiskUsage::getSizeWithReplica).reversed())
                  .limit(count).collect(Collectors.toList()));
          break;
        case VOLUME:
          childMetricsListMap.put(EntityType.BUCKET.name(),
              duResponse.getDuData()
                  .stream().sorted(Comparator.comparingLong(
                      DUResponse.DiskUsage::getSizeWithReplica).reversed())
                  .limit(count).collect(Collectors.toList()));
          break;
        case BUCKET:
        case DIRECTORY:
          List<DUResponse.DiskUsage> keys = getKeys(duResponse.getDuData());
          List<DUResponse.DiskUsage> dirs = getDirs(duResponse.getDuData());
          childMetricsListMap.put(EntityType.KEY.name(),
              keys.stream().sorted(Comparator.comparingLong(
                      DUResponse.DiskUsage::getSizeWithReplica).reversed())
                  .limit(count).collect(Collectors.toList()));
          childMetricsListMap.put(EntityType.DIRECTORY.name(),
              dirs.stream().sorted(Comparator.comparingLong(
                      DUResponse.DiskUsage::getSizeWithReplica).reversed())
                  .limit(count).collect(Collectors.toList()));
          break;
        default:
          childMetricsListMap.put(EntityType.UNKNOWN.name(),
              duResponse.getDuData()
              .stream().sorted(Comparator.comparingLong(
                  DUResponse.DiskUsage::getSizeWithReplica).reversed())
              .limit(count).collect(Collectors.toList()));
          break;
        }
      } else {
        switch (entityType) {
        case ROOT:
          childMetricsListMap.put(EntityType.VOLUME.name(),
              duResponse.getDuData()
                  .stream().sorted(Comparator.comparingLong(
                      DUResponse.DiskUsage::getCreateTime).reversed())
                  .limit(count).collect(Collectors.toList()));
          break;
        case VOLUME:
          childMetricsListMap.put(EntityType.BUCKET.name(),
              duResponse.getDuData()
                  .stream().sorted(Comparator.comparingLong(
                      DUResponse.DiskUsage::getCreateTime).reversed())
                  .limit(count).collect(Collectors.toList()));
          break;
        case BUCKET:
        case DIRECTORY:
          List<DUResponse.DiskUsage> keys = getKeys(duResponse.getDuData());
          List<DUResponse.DiskUsage> dirs = getDirs(duResponse.getDuData());
          childMetricsListMap.put(EntityType.KEY.name(),
              keys.stream().sorted(Comparator.comparingLong(
                      DUResponse.DiskUsage::getCreateTime).reversed())
                  .limit(count).collect(Collectors.toList()));
          childMetricsListMap.put(EntityType.DIRECTORY.name(),
              dirs.stream().sorted(Comparator.comparingLong(
                      DUResponse.DiskUsage::getCreateTime).reversed())
                  .limit(count).collect(Collectors.toList()));
          break;
        default:
          childMetricsListMap.put(EntityType.UNKNOWN.name(),
              duResponse.getDuData()
                  .stream().sorted(Comparator.comparingLong(
                      DUResponse.DiskUsage::getCreateTime).reversed())
                  .limit(count).collect(Collectors.toList()));
          break;
        }
      }
      CountStats countStats = nsSummaryData.getCountStats();
      entityInfoResponse.setNumVolume(countStats.getNumVolume());
      entityInfoResponse.setNumBucket(countStats.getNumBucket());
      entityInfoResponse.setNumTotalDir(countStats.getNumTotalDir());
      entityInfoResponse.setNumTotalKey(countStats.getNumTotalKey());
      entityInfoResponse.setCreateTime(countStats.getCreateTime());
      entityInfoResponse.setLastModified(countStats.getLastModified());
      entityInfoResponse.setChildMetricsListMap(childMetricsListMap);
    }
    return Response.ok(entityInfoResponse).build();
  }

  private List<DUResponse.DiskUsage> getDirs(
      List<DUResponse.DiskUsage> duData) {
    return duData.stream()
        .filter(diskUsage ->
            diskUsage.getEntityType().equals(EntityType.DIRECTORY))
        .collect(Collectors.toList());
  }

  private List<DUResponse.DiskUsage> getKeys(
      List<DUResponse.DiskUsage> duData) {
    return duData.stream()
        .filter(diskUsage ->
            diskUsage.getEntityType().equals(EntityType.KEY))
        .collect(Collectors.toList());
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
    return Response.ok(getNamespaceSummaryResponse(path)).build();
  }

  private NamespaceSummaryResponse getNamespaceSummaryResponse(String path)
      throws IOException {
    NamespaceSummaryResponse namespaceSummaryResponse;
    if (!isInitializationComplete()) {
      namespaceSummaryResponse =
          NamespaceSummaryResponse.newBuilder()
              .setEntityType(EntityType.UNKNOWN)
              .setStatus(ResponseStatus.INITIALIZING)
              .build();
    } else {
      EntityHandler handler = EntityHandler.getEntityHandler(
          reconNamespaceSummaryManager,
          omMetadataManager, reconSCM, path);

      namespaceSummaryResponse = handler.getSummaryResponse();
    }
    return namespaceSummaryResponse;
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
    return Response.ok(getDuResponse(path, listFile, withReplica)).build();
  }

  private DUResponse getDuResponse(String path, boolean listFile,
                                   boolean withReplica) throws IOException {
    AtomicBoolean isError = new AtomicBoolean(false);
    DUResponse duResponse = new DUResponse();
    if (!isInitializationComplete()) {
      duResponse.setStatus(ResponseStatus.INITIALIZING);
    } else {
      EntityHandler handler = EntityHandler.getEntityHandler(
          reconNamespaceSummaryManager,
          omMetadataManager, reconSCM, path);
      duResponse = handler.getDuResponse(
          listFile, withReplica);
      duResponse.getDuData().forEach(diskUsage -> {
        try {
          EntityHandler childEntityHandler = EntityHandler.getEntityHandler(
              reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, diskUsage.getSubpath());
          NamespaceSummaryResponse namespaceSummaryResponse =
              childEntityHandler.getSummaryResponse();
          CountStats countStats = namespaceSummaryResponse.getCountStats();
          ObjectDBInfo objectDBInfo =
              namespaceSummaryResponse.getObjectDBInfo();
          diskUsage.setEntityType(namespaceSummaryResponse.getEntityType());
          diskUsage.setKeyCount(countStats.getNumTotalKey());
          diskUsage.setBucketCount(countStats.getNumBucket());
          diskUsage.setVolumeCount(countStats.getNumVolume());
          diskUsage.setDirCount(countStats.getNumTotalDir());
          diskUsage.setCreateTime(countStats.getCreateTime());
          diskUsage.setLastModified(
              countStats.getLastModified());
          diskUsage.setObjectDBInfo(objectDBInfo);
        } catch (Exception exp) {
          isError.set(true);
        }
      });
    }
    if (isError.get()) {
      duResponse.setStatus(ResponseStatus.PATH_NOT_FOUND);
    }
    return duResponse;
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
