/**
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
package org.apache.hadoop.ozone.om.request.key.acl;

import com.google.common.collect.Lists;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.acl.OMKeyAclResponse;
import org.apache.hadoop.ozone.om.response.key.acl.OMKeyAclResponseV1;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Handle add Acl request for bucket for layout version V1.
 */
public class OMKeyAddAclRequestV1 extends OMKeyAclRequestV1 {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyAddAclRequestV1.class);

  @Override public OzoneManagerProtocolProtos.OMRequest preExecute(
      OzoneManager ozoneManager) throws IOException {
    long modificationTime = Time.now();
    OzoneManagerProtocolProtos.AddAclRequest.Builder addAclRequestBuilder =
        getOmRequest().getAddAclRequest().toBuilder()
            .setModificationTime(modificationTime);

    return getOmRequest().toBuilder().setAddAclRequest(addAclRequestBuilder)
        .setUserInfo(getUserInfo()).build();
  }

  private String path;
  private List<OzoneAcl> ozoneAcls;
  private OzoneObj obj;

  public OMKeyAddAclRequestV1(OzoneManagerProtocolProtos.OMRequest omRequest) {
    super(omRequest);
    OzoneManagerProtocolProtos.AddAclRequest addAclRequest =
        getOmRequest().getAddAclRequest();
    obj = OzoneObjInfo.fromProtobuf(addAclRequest.getObj());
    path = obj.getPath();
    ozoneAcls =
        Lists.newArrayList(OzoneAcl.fromProtobuf(addAclRequest.getAcl()));
  }

  @Override String getPath() {
    return path;
  }

  @Override OzoneObj getObject() {
    return obj;
  }

  @Override OzoneManagerProtocolProtos.OMResponse.Builder onInit() {
    return OmResponseUtil.getOMResponseBuilder(getOmRequest());
  }

  @Override OMClientResponse onSuccess(
      OzoneManagerProtocolProtos.OMResponse.Builder omResponse,
      OmKeyInfo omKeyInfo, boolean operationResult) {
    omResponse.setSuccess(operationResult);
    omResponse.setAddAclResponse(
        OzoneManagerProtocolProtos.AddAclResponse.newBuilder()
            .setResponse(operationResult));
    return new OMKeyAclResponse(omResponse.build(), omKeyInfo);
  }

  @Override void onComplete(Result result, boolean operationResult,
      IOException exception, long trxnLogIndex, AuditLogger auditLogger,
      Map<String, String> auditMap) {
    switch (result) {
    case SUCCESS:
      if (LOG.isDebugEnabled()) {
        if (operationResult) {
          LOG.debug("Add acl: {} to path: {} success!", ozoneAcls, path);
        } else {
          LOG.debug("Acl {} already exists in path {}", ozoneAcls, path);
        }
      }
      break;
    case FAILURE:
      LOG.error("Add acl {} to path {} failed!", ozoneAcls, path, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyAddAclRequest: {}",
          getOmRequest());
    }

    if (ozoneAcls != null) {
      auditMap.put(OzoneConsts.ACL, ozoneAcls.toString());
    }
    auditLog(auditLogger,
        buildAuditMessage(OMAction.ADD_ACL, auditMap, exception,
            getOmRequest().getUserInfo()));
  }

  @Override boolean apply(OmKeyInfo omKeyInfo, long trxnLogIndex) {
    // No need to check not null here, this will be never called with null.
    return omKeyInfo.addAcl(ozoneAcls.get(0));
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    ozoneManager.getMetrics().incNumAddAcl();
    return super.validateAndUpdateCache(ozoneManager, trxnLogIndex,
        omDoubleBufferHelper);
  }

  @Override OMClientResponse onSuccess(
      OzoneManagerProtocolProtos.OMResponse.Builder omResponse,
      OmKeyInfo omKeyInfo, boolean operationResult, boolean isDirectory) {
    omResponse.setSuccess(operationResult);
    omResponse.setAddAclResponse(
        OzoneManagerProtocolProtos.AddAclResponse.newBuilder()
            .setResponse(operationResult));
    return new OMKeyAclResponseV1(omResponse.build(), omKeyInfo, isDirectory);
  }
}
