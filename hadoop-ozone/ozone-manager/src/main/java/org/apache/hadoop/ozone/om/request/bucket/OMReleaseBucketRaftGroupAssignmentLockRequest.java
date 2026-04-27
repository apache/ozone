/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

/**
 * Handles the request to release the bucket raft group assignment write lock.
 */
public class OMReleaseBucketRaftGroupAssignmentLockRequest extends OMClientRequest {

  private static final Logger LOG = LoggerFactory.getLogger(OMReleaseBucketRaftGroupAssignmentLockRequest.class);

  public OMReleaseBucketRaftGroupAssignmentLockRequest(OzoneManagerProtocolProtos.OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    try {
      ozoneManager.getOmRaftGroupManager().releaseBucketRaftGroupAssignmentWriteLock();
    } catch (InterruptedException e) {
      LOG.error("Interrupted while release bucket raft group assignment write lock");
      throw new RuntimeException(e);
    }
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    return new DummyOMClientResponse(omResponse.build());
  }

}
