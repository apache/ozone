package org.apache.hadoop.ozone.om.request.group;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.group.OMCreateRaftGroupsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketRaftGroupsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketRaftGroupsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Handles create raft group request.
 */
public class OMCreateRaftGroupsRequest extends OMClientRequest {

  public static final Logger LOG = LoggerFactory.getLogger(OMCreateRaftGroupsRequest.class);

  public OMCreateRaftGroupsRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    OMRequest omRequest = getOmRequest();
    CreateBucketRaftGroupsRequest createBucketRaftGroupsRequest = omRequest.getCreateBucketRaftGroupsRequest();
    if (createBucketRaftGroupsRequest.getPurgeExistingRaftGroups()) {
      try {
        Iterable<RaftGroup> existingRaftGroups = ozoneManager.getOmRatisServer().getServer().getGroups();
        for (RaftGroup group : existingRaftGroups) {
          if (!group.getGroupId().equals(ozoneManager.getOmRatisServer().getCurrentRaftGroupId())) {
            ozoneManager.getOmRatisServer().removeBucketRaftGroup(group.getGroupId());
          }
        }
      } catch (IOException e) {
        LOG.warn("Something went wrong on deleting existing raft groups", e);
      }
    }

    createBucketRaftGroupsRequest.getGroupIdsList().forEach(groupId -> {
      ozoneManager.createRaftGroupForBucket(RaftGroupId.valueOf(HddsUtils.fromProtobuf(groupId)));
      ozoneManager.getOmRaftGroupManager().addGroupIdToRaftGroupCounter(HddsUtils.fromProtobuf(groupId));
    });
    final OMResponse.Builder omResponse =
            OmResponseUtil.getOMResponseBuilder(omRequest);
    CreateBucketRaftGroupsResponse createBucketRaftGroupsResponse =
            CreateBucketRaftGroupsResponse.newBuilder().build();
    omResponse.setCreateBucketRaftGroupsResponse(createBucketRaftGroupsResponse);
    return new OMCreateRaftGroupsResponse(omResponse.build());
  }
}
