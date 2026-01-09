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

package org.apache.hadoop.ozone.om.request.s3.security;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3DeleteRevokedSTSTokensResponse;
import org.apache.hadoop.ozone.om.service.RevokedSTSTokenCleanupService;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteRevokedSTSTokensRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

/**
 * Handles DeleteRevokedSTSTokens requests submitted by {@link RevokedSTSTokenCleanupService}.
 */
public class S3DeleteRevokedSTSTokensRequest extends OMClientRequest {

  public S3DeleteRevokedSTSTokensRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final UserGroupInformation ugi;
    try {
      ugi = createUGI();
    } catch (AuthenticationException e) {
      throw new OMException(e, OMException.ResultCodes.PERMISSION_DENIED);
    }
    if (!ozoneManager.isAdmin(ugi) && !ozoneManager.isS3Admin(ugi)) {
      throw new OMException("Only admins can delete revoked STS tokens", OMException.ResultCodes.PERMISSION_DENIED);
    }

    return getOmRequest().toBuilder()
        .setUserInfo(getUserInfo())
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final DeleteRevokedSTSTokensRequest request = getOmRequest().getDeleteRevokedSTSTokensRequest();
    final OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());

    final List<String> sessionTokens = request.getSessionTokenList();
    return new S3DeleteRevokedSTSTokensResponse(sessionTokens, omResponse.build());
  }
}


