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

package org.apache.hadoop.ozone.om.request.volume.acl;

import static java.util.Collections.singletonList;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.util.AclOp;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeAclOpResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles volume add acl request.
 */
public class OMVolumeAddAclRequest extends OMVolumeAclRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMVolumeAddAclRequest.class);

  private static final AclOp VOLUME_ADD_ACL_OP =
      (acls, builder) -> builder.add(acls.get(0));

  private final List<OzoneAcl> ozoneAcls;
  private final String volumeName;
  private final OzoneObj obj;

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    long modificationTime = Time.now();
    OzoneManagerProtocolProtos.AddAclRequest.Builder addAclRequestBuilder =
        getOmRequest().getAddAclRequest().toBuilder()
            .setModificationTime(modificationTime);
    final OMRequest omRequest = getOmRequest()
        .toBuilder()
        .setAddAclRequest(addAclRequestBuilder)
        .setUserInfo(getUserInfo())
        .build();
    setOmRequest(omRequest);
    return super.preExecute(ozoneManager);
  }

  public OMVolumeAddAclRequest(OMRequest omRequest) {
    super(omRequest, VOLUME_ADD_ACL_OP);
    OzoneManagerProtocolProtos.AddAclRequest addAclRequest =
        getOmRequest().getAddAclRequest();
    Objects.requireNonNull(addAclRequest, "addAclRequest == null");
    ozoneAcls = singletonList(OzoneAcl.fromProtobuf(addAclRequest.getAcl()));
    obj = OzoneObjInfo.fromProtobuf(addAclRequest.getObj());
    volumeName = obj.getPath().substring(1);
  }

  @Override
  public List<OzoneAcl> getAcls() {
    return ozoneAcls;
  }

  @Override
  public String getVolumeName() {
    return volumeName;
  }

  private OzoneAcl getAcl() {
    return ozoneAcls.get(0);
  }

  @Override
  OzoneObj getObject() {
    return obj;
  }

  @Override
  OMResponse.Builder onInit() {
    return OmResponseUtil.getOMResponseBuilder(getOmRequest());
  }

  @Override
  OMClientResponse onSuccess(OMResponse.Builder omResponse,
      OmVolumeArgs omVolumeArgs, boolean aclApplied) {
    omResponse.setAddAclResponse(OzoneManagerProtocolProtos.AddAclResponse
        .newBuilder().setResponse(aclApplied).build());
    return new OMVolumeAclOpResponse(omResponse.build(), omVolumeArgs);
  }

  @Override
  OMClientResponse onFailure(OMResponse.Builder omResponse,
      Exception ex) {
    return new OMVolumeAclOpResponse(createErrorOMResponse(omResponse, ex));
  }

  @Override
  void onComplete(Result result, Exception ex, long trxnLogIndex,
      AuditLogger auditLogger, Map<String, String> auditMap) {
    switch (result) {
    case SUCCESS:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Add acl: {} to volume: {} success!", getAcl(),
            getVolumeName());
      }
      break;
    case FAILURE:
      LOG.error("Add acl {} to volume {} failed!", getAcl(), getVolumeName(),
          ex);
      break;
    default:
      LOG.error("Unrecognized Result for OMVolumeAddAclRequest: {}",
          getOmRequest());
    }

    markForAudit(auditLogger, buildAuditMessage(OMAction.ADD_ACL, auditMap,
        ex, getOmRequest().getUserInfo()));
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    ozoneManager.getMetrics().incNumAddAcl();
    return super.validateAndUpdateCache(ozoneManager, context);
  }
}
