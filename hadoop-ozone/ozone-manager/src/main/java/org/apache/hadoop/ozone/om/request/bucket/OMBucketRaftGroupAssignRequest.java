package org.apache.hadoop.ozone.om.request.bucket;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketRaftGroupAssignResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketRaftGroupAssignRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketRaftGroupAssignResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * Handles assignment of raft group to a bucket through Ratis consensus.
 * This ensures all OM instances agree on the bucket-to-raft-group mapping.
 */
public class OMBucketRaftGroupAssignRequest extends OMClientRequest {
  private static final Logger LOG = LoggerFactory.getLogger(OMBucketRaftGroupAssignRequest.class);

  public OMBucketRaftGroupAssignRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
                                                 ExecutionContext context) {
    try {
      BucketRaftGroupAssignRequest assignRequest = getOmRequest().getBucketRaftGroupAssignRequest();
      String bucketPath = assignRequest.getBucketPath();
      HddsProtos.UUID raftGroupProto = assignRequest.getRaftGroupId();
      UUID raftGroupUUID = new UUID(raftGroupProto.getMostSigBits(),
          raftGroupProto.getLeastSigBits());
      LOG.info("MULTIRAFT: assign raft group: {} to bucket: {}, trxId: {}", raftGroupUUID, bucketPath,
          context.getCacheEpoch());
      ozoneManager.getOmRaftGroupManager().defineAndGetRaftGroupForBucket(bucketPath, raftGroupUUID);
      OmBucketInfo bucketInfo = ozoneManager.getMetadataManager().getBucketTable().get(bucketPath);
      bucketInfo.setRaftGroup(raftGroupUUID);
      return makeSuccessResponse(bucketPath, raftGroupUUID, bucketInfo);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private OMClientResponse makeSuccessResponse(String bucketPath, UUID bucketRaftGroup, OmBucketInfo bucket) {
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    omResponse.setBucketRaftGroupAssignResponse(
        BucketRaftGroupAssignResponse.newBuilder()
            .setBucketPath(bucketPath)
            .setRaftGroupId(HddsProtos.UUID.newBuilder()
                .setMostSigBits(bucketRaftGroup.getMostSignificantBits())
                .setLeastSigBits(bucketRaftGroup.getLeastSignificantBits()).build()));
    return new OMBucketRaftGroupAssignResponse(omResponse.build(), bucket);
  }
}
