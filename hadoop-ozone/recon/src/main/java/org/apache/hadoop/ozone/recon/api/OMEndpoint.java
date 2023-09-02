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
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.api.types.AclMetadata;
import org.apache.hadoop.ozone.recon.api.types.BucketMetadata;
import org.apache.hadoop.ozone.recon.api.types.BucketsResponse;
import org.apache.hadoop.ozone.recon.api.types.VolumeMetadata;
import org.apache.hadoop.ozone.recon.api.types.VolumesResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_FETCH_COUNT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_PREVKEY;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_VOLUME;

/**
 * Endpoint to fetch details about volume.
 */
@Path("/om")
@Produces(MediaType.APPLICATION_JSON)
@AdminOnly
public class OMEndpoint {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMEndpoint.class);

  @Inject
  private ReconOMMetadataManager omMetadataManager;

  @Inject
  public OMEndpoint(ReconOMMetadataManager omMetadataManager) {
    this.omMetadataManager = omMetadataManager;
  }

  @GET
  @Path("/volumes")
  public Response getVolumes(
      @DefaultValue(DEFAULT_FETCH_COUNT)
      @QueryParam(RECON_QUERY_LIMIT) int limit,
      @DefaultValue(StringUtils.EMPTY)
      @QueryParam(RECON_QUERY_PREVKEY) String prevKey
  ) throws IOException {
    List<OmVolumeArgs> volumes = omMetadataManager.listAllVolumes(
        prevKey, limit);
    List<VolumeMetadata> volumeMetadata = volumes.stream()
        .map(this::toVolumeMetadata).collect(Collectors.toList());
    VolumesResponse volumesResponse =
        new VolumesResponse(volumes.size(), volumeMetadata);
    return Response.ok(volumesResponse).build();
  }

  @GET
  @Path("/buckets")
  public Response getBuckets(
      @QueryParam(RECON_QUERY_VOLUME) String volume,
      @DefaultValue(DEFAULT_FETCH_COUNT)
      @QueryParam(RECON_QUERY_LIMIT) int limit,
      @DefaultValue(StringUtils.EMPTY)
      @QueryParam(RECON_QUERY_PREVKEY) String prevKey
  ) throws IOException {
    List<OmBucketInfo> buckets = omMetadataManager.listBucketsUnderVolume(
        volume, prevKey, limit);
    List<BucketMetadata> bucketMetadata = buckets
        .stream().map(this::toBucketMetadata).collect(Collectors.toList());
    BucketsResponse bucketsResponse =
        new BucketsResponse(buckets.size(), bucketMetadata);
    return Response.ok(bucketsResponse).build();
  }

  private VolumeMetadata toVolumeMetadata(OmVolumeArgs omVolumeArgs) {
    if (omVolumeArgs == null) {
      return null;
    }

    VolumeMetadata.Builder builder = VolumeMetadata.newBuilder();

    List<AclMetadata> acls = new ArrayList<>();
    if (omVolumeArgs.getAcls() != null) {
      acls = omVolumeArgs.getAcls().stream()
          .map(this::toAclMetadata).collect(Collectors.toList());
    }

    return builder.withVolume(omVolumeArgs.getVolume())
        .withOwner(omVolumeArgs.getOwnerName())
        .withAdmin(omVolumeArgs.getAdminName())
        .withCreationTime(omVolumeArgs.getCreationTime())
        .withModificationTime(omVolumeArgs.getModificationTime())
        .withQuotaInBytes(omVolumeArgs.getQuotaInBytes())
        .withQuotaInNamespace(
            omVolumeArgs.getQuotaInNamespace())
        .withUsedNamespace(omVolumeArgs.getUsedNamespace())
        .withAcls(acls)
        .build();
  }

  private AclMetadata toAclMetadata(OzoneAcl ozoneAcl) {
    if (ozoneAcl == null) {
      return null;
    }

    AclMetadata.Builder builder = AclMetadata.newBuilder();

    return builder.withType(ozoneAcl.getType().toString().toUpperCase())
        .withName(ozoneAcl.getName())
        .withScope(ozoneAcl.getAclScope().toString().toUpperCase())
        .withAclList(ozoneAcl.getAclList().stream().map(Enum::toString)
            .map(String::toUpperCase)
            .collect(Collectors.toList()))
        .build();
  }

  private BucketMetadata toBucketMetadata(OmBucketInfo omBucketInfo) {
    if (omBucketInfo == null) {
      return null;
    }

    BucketMetadata.Builder builder = BucketMetadata.newBuilder();

    List<AclMetadata> acls = new ArrayList<>();
    if (omBucketInfo.getAcls() != null) {
      acls = omBucketInfo.getAcls().stream()
          .map(this::toAclMetadata).collect(Collectors.toList());
    }

    builder.withVolumeName(omBucketInfo.getVolumeName())
        .withBucketName(omBucketInfo.getBucketName())
        .withAcls(acls)
        .withVersionEnabled(omBucketInfo.getIsVersionEnabled())
        .withStorageType(omBucketInfo.getStorageType().toString().toUpperCase())
        .withCreationTime(omBucketInfo.getCreationTime())
        .withModificationTime(omBucketInfo.getModificationTime())
        .withUsedBytes(omBucketInfo.getUsedBytes())
        .withUsedNamespace(omBucketInfo.getUsedNamespace())
        .withQuotaInBytes(omBucketInfo.getQuotaInBytes())
        .withQuotaInNamespace(omBucketInfo.getQuotaInNamespace())
        .withBucketLayout(
            omBucketInfo.getBucketLayout().toString().toUpperCase())
        .withOwner(omBucketInfo.getOwner());

    if (omBucketInfo.getSourceVolume() != null) {
      builder.withSourceVolume(omBucketInfo.getSourceVolume());
    }

    if (omBucketInfo.getSourceBucket() != null) {
      builder.withSourceBucket(omBucketInfo.getSourceBucket());
    }

    return builder.build();
  }

}
