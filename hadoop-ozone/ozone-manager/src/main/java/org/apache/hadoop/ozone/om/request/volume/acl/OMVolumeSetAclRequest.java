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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
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
 * Handles volume set acl request.
 */
public class OMVolumeSetAclRequest extends OMVolumeAclRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMVolumeSetAclRequest.class);

  private static final AclOp VOLUME_SET_ACL_OP =
      (acls, builder) -> builder.set(acls);

  private final List<OzoneAcl> ozoneAcls;
  private final String volumeName;
  private final OzoneObj obj;

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    long modificationTime = Time.now();
    OzoneManagerProtocolProtos.SetAclRequest.Builder setAclRequestBuilder =
        getOmRequest().getSetAclRequest().toBuilder()
            .setModificationTime(modificationTime);

    OMRequest omRequest = getOmRequest().toBuilder()
        .setSetAclRequest(setAclRequestBuilder)
        .setUserInfo(getUserInfo())
        .build();
    setOmRequest(omRequest);
    return super.preExecute(ozoneManager);
  }

  public OMVolumeSetAclRequest(OMRequest omRequest) {
    super(omRequest, VOLUME_SET_ACL_OP);
    OzoneManagerProtocolProtos.SetAclRequest setAclRequest =
        getOmRequest().getSetAclRequest();
    Objects.requireNonNull(setAclRequest, "setAclRequest == null");
    ozoneAcls = setAclRequest.getAclList().stream()
        .map(OzoneAcl::fromProtobuf)
        .collect(Collectors.toList());
    obj = OzoneObjInfo.fromProtobuf(setAclRequest.getObj());
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
    omResponse.setSetAclResponse(OzoneManagerProtocolProtos.SetAclResponse
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
        LOG.debug("Set acls: {} to volume: {} success!", getAcls(),
            getVolumeName());
      }
      break;
    case FAILURE:
      LOG.error("Set acls {} to volume {} failed!", getAcls(),
          getVolumeName(), ex);
      break;
    default:
      LOG.error("Unrecognized Result for OMVolumeSetAclRequest: {}",
          getOmRequest());
    }

    markForAudit(auditLogger, buildAuditMessage(OMAction.SET_ACL, auditMap,
        ex, getOmRequest().getUserInfo()));
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    ozoneManager.getMetrics().incNumSetAcl();
    return super.validateAndUpdateCache(ozoneManager, context);
  }
}
