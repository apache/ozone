package org.apache.hadoop.ozone.om.request.bucket;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.DummyOMClientResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OMAcquireBucketRaftGroupAssignmentLockRequest extends OMClientRequest {

  private static final Logger LOG = LoggerFactory.getLogger(OMAcquireBucketRaftGroupAssignmentLockRequest.class);

  public OMAcquireBucketRaftGroupAssignmentLockRequest(OzoneManagerProtocolProtos.OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    omResponse.setSuccess(ozoneManager.getOmRaftGroupManager().acquireBucketRaftGroupAssignmentWriteLock());
    return new DummyOMClientResponse(omResponse.build());
  }

}
