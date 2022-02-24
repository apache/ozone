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

package org.apache.hadoop.ozone.om.request.key;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMPathsPurgeResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import java.util.List;

/**
 * Handles purging of keys from OM DB.
 */
public class OMPathsPurgeRequestWithFSO extends OMKeyRequest {

  public OMPathsPurgeRequestWithFSO(OMRequest omRequest) {
    super(omRequest, BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    OzoneManagerProtocolProtos.PurgePathsRequest purgePathsRequest =
        getOmRequest().getPurgePathsRequest();

    List<String> deletedDirsList = purgePathsRequest.getDeletedDirsList();
    List<OzoneManagerProtocolProtos.KeyInfo> deletedSubFilesList =
        purgePathsRequest.getDeletedSubFilesList();
    List<OzoneManagerProtocolProtos.KeyInfo> markDeletedSubDirsList =
        purgePathsRequest.getMarkDeletedSubDirsList();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    OMClientResponse omClientResponse = new OMPathsPurgeResponseWithFSO(
        omResponse.build(), markDeletedSubDirsList, deletedSubFilesList,
        deletedDirsList, ozoneManager.isRatisEnabled(), getBucketLayout());
    addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
        omDoubleBufferHelper);

    return omClientResponse;
  }
}
