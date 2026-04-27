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

package org.apache.hadoop.ozone.om.request.group;

import java.io.IOException;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.group.OMBucketRaftGroupsStateUpdateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;

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
