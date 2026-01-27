package org.apache.hadoop.ozone.om.request.group;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.group.OMBucketRaftGroupsStateUpdateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;

import java.io.IOException;

/**
 * Handles Bucket Raft Groups State Update Request.
 */
public class OMBucketRaftGroupsStateUpdateRequest extends OMClientRequest {

  public OMBucketRaftGroupsStateUpdateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    OMRequest omRequest = getOmRequest();
    long stateChangedIndex = omRequest.getBucketRaftGroupsStateChangedRequest().getStateChangedIndex();
    try {
      ozoneManager.updateBucketRaftGroupsReconfigurationIndex(stateChangedIndex);
      ozoneManager.getMetadataManager().getStore().flushDB();
      final OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
          OmResponseUtil.getOMResponseBuilder(omRequest);
      OzoneManagerProtocolProtos.BucketRaftGroupsStateChangedResponse bucketRaftGroupsStateChangedResponse =
          OzoneManagerProtocolProtos.BucketRaftGroupsStateChangedResponse.newBuilder().build();
      return new OMBucketRaftGroupsStateUpdateResponse(omResponse.setBucketRaftGroupsStateChangedResponse(
          bucketRaftGroupsStateChangedResponse).build());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
