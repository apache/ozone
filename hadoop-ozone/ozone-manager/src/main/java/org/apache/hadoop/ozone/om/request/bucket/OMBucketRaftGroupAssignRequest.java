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

import java.io.IOException;
import java.util.UUID;
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
