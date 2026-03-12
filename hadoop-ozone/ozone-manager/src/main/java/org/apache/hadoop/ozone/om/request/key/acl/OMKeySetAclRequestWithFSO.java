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

package org.apache.hadoop.ozone.om.request.key.acl;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.acl.OMKeyAclResponse;
import org.apache.hadoop.ozone.om.response.key.acl.OMKeyAclResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle set Acl request for bucket for prefix layout.
 */
public class OMKeySetAclRequestWithFSO extends OMKeyAclRequestWithFSO {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeySetAclRequestWithFSO.class);

  private String path;
  private List<OzoneAcl> ozoneAcls;
  private OzoneObj obj;

  @Override
  public OzoneManagerProtocolProtos.OMRequest preExecute(
      OzoneManager ozoneManager) throws IOException {
    long modificationTime = Time.now();
    OzoneManagerProtocolProtos.SetAclRequest.Builder setAclRequestBuilder =
        getOmRequest().getSetAclRequest().toBuilder()
            .setModificationTime(modificationTime);

    return getOmRequest().toBuilder().setSetAclRequest(setAclRequestBuilder)
        .setUserInfo(getUserInfo()).build();
  }

  public OMKeySetAclRequestWithFSO(
      OzoneManagerProtocolProtos.OMRequest omReq, BucketLayout bucketLayout) {
    super(omReq, bucketLayout);
    OzoneManagerProtocolProtos.SetAclRequest setAclRequest =
        getOmRequest().getSetAclRequest();
    obj = OzoneObjInfo.fromProtobuf(setAclRequest.getObj());
    path = obj.getPath();
    ozoneAcls = Lists
        .newArrayList(OzoneAclUtil.fromProtobuf(setAclRequest.getAclList()));
  }

  @Override
  String getPath() {
    return path;
  }

  @Override
  OzoneObj getObject() {
    return obj;
  }

  @Override
  OzoneManagerProtocolProtos.OMResponse.Builder onInit() {
    return OmResponseUtil.getOMResponseBuilder(getOmRequest());
  }

  @Override
  OMClientResponse onSuccess(
      OzoneManagerProtocolProtos.OMResponse.Builder omResponse,
      OmKeyInfo omKeyInfo, boolean operationResult) {
    omResponse.setSuccess(operationResult);
    omResponse.setSetAclResponse(
        OzoneManagerProtocolProtos.SetAclResponse.newBuilder()
            .setResponse(operationResult));
    return new OMKeyAclResponse(omResponse.build(), omKeyInfo);
  }

  @Override
  void onComplete(Result result, boolean operationResult,
      Exception exception, long trxnLogIndex, AuditLogger auditLogger,
      Map<String, String> auditMap) {
    switch (result) {
    case SUCCESS:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Set acl: {} to path: {} success!", ozoneAcls, path);
      }
      break;
    case FAILURE:
      LOG.error("Set acl {} to path {} failed!", ozoneAcls, path, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeySetAclRequest: {}",
          getOmRequest());
    }

    if (ozoneAcls != null) {
      auditMap.put(OzoneConsts.ACL, ozoneAcls.toString());
    }
    markForAudit(auditLogger,
        buildAuditMessage(OMAction.SET_ACL, auditMap, exception,
            getOmRequest().getUserInfo()));
  }

  @Override
  boolean apply(OmKeyInfo.Builder builder, long trxnLogIndex) {
    // No need to check not null here, this will be never called with null.
    return builder.acls().set(ozoneAcls);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    ozoneManager.getMetrics().incNumSetAcl();
    return super.validateAndUpdateCache(ozoneManager, context);
  }

  @Override
  OMClientResponse onSuccess(
      OzoneManagerProtocolProtos.OMResponse.Builder omResponse,
      OmKeyInfo omKeyInfo, boolean operationResult, boolean isDir,
      long volumeId, long bucketId) {
    omResponse.setSuccess(operationResult);
    omResponse.setSetAclResponse(
        OzoneManagerProtocolProtos.SetAclResponse.newBuilder()
            .setResponse(operationResult));
    return new OMKeyAclResponseWithFSO(omResponse.build(), omKeyInfo, isDir,
        getBucketLayout(), volumeId, bucketId);
  }
}
