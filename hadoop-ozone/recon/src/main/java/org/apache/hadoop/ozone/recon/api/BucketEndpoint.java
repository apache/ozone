package org.apache.hadoop.ozone.recon.api;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.recon.api.types.AclMetadata;
import org.apache.hadoop.ozone.recon.api.types.BucketMetadata;
import org.apache.hadoop.ozone.recon.api.types.BucketsResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;

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
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_STARTKEY;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_VOLUME;

/**
 * Endpoint to fetch details about buckets.
 */
@Path("/buckets")
@Produces(MediaType.APPLICATION_JSON)
@AdminOnly
public class BucketEndpoint {

  @Inject
  private ReconOMMetadataManager omMetadataManager;

  @Inject
  public BucketEndpoint(ReconOMMetadataManager omMetadataManager) {
    this.omMetadataManager = omMetadataManager;
  }

  @GET
  public Response getBuckets(
      @QueryParam(RECON_QUERY_VOLUME) String volume,
      @DefaultValue(DEFAULT_FETCH_COUNT)
      @QueryParam(RECON_QUERY_LIMIT) int limit,
      @DefaultValue(StringUtils.EMPTY)
      @QueryParam(RECON_QUERY_STARTKEY) String startKey
  ) throws IOException {
    List<OmBucketInfo> buckets = omMetadataManager.listBucketsUnderVolume(
        volume, startKey, limit);
    List<BucketMetadata> bucketMetadata = buckets
        .stream().map(this::toBucketMetadata).collect(Collectors.toList());
    BucketsResponse bucketsResponse =
        new BucketsResponse(buckets.size(), bucketMetadata);
    return Response.ok(bucketsResponse).build();
  }

  private BucketMetadata toBucketMetadata(OmBucketInfo omBucketInfo) {
    if (omBucketInfo == null) {
      return null;
    }

    BucketMetadata.Builder builder = BucketMetadata.newBuilder();

    List<AclMetadata> acls = new ArrayList<>();
    if (omBucketInfo.getAcls() != null) {
      acls = omBucketInfo.getAcls().stream()
          .map(AclMetadata::toAclMetadata).collect(Collectors.toList());
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
