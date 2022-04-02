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
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.recon.api.types.*;
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

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * REST APIs for namespace metadata summary.
 */
@Path("/namespace")
@Produces(MediaType.APPLICATION_JSON)
@AdminOnly
public class NSSummaryEndpoint {

  @Inject
  private ReconOMMetadataManager omMetadataManager;

  @Inject
  public NSSummaryEndpoint(ReconOMMetadataManager omMetadataManager) {
    this.omMetadataManager = omMetadataManager;
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

    NamespaceSummaryResponse namespaceSummaryResponse = null;
    if (!isInitializationComplete()) {
      namespaceSummaryResponse =
          new NamespaceSummaryResponse(EntityType.UNKNOWN);
      namespaceSummaryResponse.setStatus(ResponseStatus.INITIALIZING);
      return Response.ok(namespaceSummaryResponse).build();
    }

    String normalizedPath = normalizePath(path);
    String[] names = parseRequestPath(normalizedPath);

    EntityType type = entityUtils.getEntityType(normalizedPath, names);

    namespaceSummaryResponse = type.getSummaryResponse(names, entityUtils);

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

    String normalizedPath = normalizePath(path);
    String[] names = parseRequestPath(normalizedPath);
    EntityType type = entityUtils.getEntityType(normalizedPath, names);

    duResponse.setPath(normalizedPath);

    duResponse = type.getDuResponse(normalizedPath, names, listFile, withReplica, entityUtils);

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

    String normalizedPath = normalizePath(path);
    String[] names = parseRequestPath(normalizedPath);
    EntityType type = entityUtils.getEntityType(normalizedPath, names);

    quotaUsageResponse = type.getQuotaResponse(names, entityUtils);

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

    String normalizedPath = normalizePath(path);
    String[] names = parseRequestPath(normalizedPath);
    EntityType type = entityUtils.getEntityType(normalizedPath, names);

    distResponse = type.getDistResponse(names, entityUtils);

    return Response.ok(distResponse).build();
  }

  static String[] parseRequestPath(String path) {
    if (path.startsWith(OM_KEY_PREFIX)) {
      path = path.substring(1);
    }
    String[] names = path.split(OM_KEY_PREFIX);
    return names;
  }

  /**
   * Return if all OMDB tables that will be used are initialized.
   * @return
   */
  private boolean isInitializationComplete() {
    if (omMetadataManager == null) {
      return false;
    }
    return omMetadataManager.getVolumeTable() != null
        && omMetadataManager.getBucketTable() != null
        && omMetadataManager.getDirectoryTable() != null
        && omMetadataManager.getFileTable() != null;
  }

  private static String normalizePath(String path) {
    return OM_KEY_PREFIX + OmUtils.normalizeKey(path, false);
  }
}
