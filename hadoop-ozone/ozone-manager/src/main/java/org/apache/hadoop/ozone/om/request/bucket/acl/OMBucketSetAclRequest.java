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

package org.apache.hadoop.ozone.om.request.bucket.acl;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.acl.OMBucketAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetAclResponse;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle setAcl request for bucket.
 */
public class OMBucketSetAclRequest extends OMBucketAclRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMBucketSetAclRequest.class);

  private final String path;
  private final List<OzoneAcl> ozoneAcls;
  private final OzoneObj obj;

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    super.preExecute(ozoneManager);
    long modificationTime = Time.now();
    OzoneManagerProtocolProtos.SetAclRequest.Builder setAclRequestBuilder =
        getOmRequest().getSetAclRequest().toBuilder()
            .setModificationTime(modificationTime);

    return getOmRequest().toBuilder()
        .setSetAclRequest(setAclRequestBuilder)
        .setUserInfo(getUserInfo())
        .build();
  }

  public OMBucketSetAclRequest(OMRequest omRequest) {
    super(omRequest, (acls, builder) -> builder.set(acls));
    OzoneManagerProtocolProtos.SetAclRequest setAclRequest =
        getOmRequest().getSetAclRequest();
    obj = OzoneObjInfo.fromProtobuf(setAclRequest.getObj());
    path = obj.getPath();
    ozoneAcls = setAclRequest.getAclList().stream()
        .map(OzoneAcl::fromProtobuf)
        .collect(Collectors.toList());
  }

  @Override
  List<OzoneAcl> getAcls() {
    return ozoneAcls;
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
  OMResponse.Builder onInit() {
    return OmResponseUtil.getOMResponseBuilder(getOmRequest());
  }

  @Override
  OMClientResponse onSuccess(OMResponse.Builder omResponse,
      OmBucketInfo omBucketInfo, boolean operationResult) {
    omResponse.setSuccess(operationResult);
    omResponse.setSetAclResponse(SetAclResponse.newBuilder()
        .setResponse(operationResult));
    return new OMBucketAclResponse(omResponse.build(), omBucketInfo);
  }

  @Override
  void onComplete(boolean operationResult, Exception exception,
      OMMetrics omMetrics, AuditLogger auditLogger,
      Map<String, String> auditMap) {
    markForAudit(auditLogger, buildAuditMessage(OMAction.SET_ACL, auditMap,
        exception, getOmRequest().getUserInfo()));

    if (operationResult) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Set acl: {} for path: {} success!", getAcls(), getPath());
      }
    } else {
      omMetrics.incNumBucketUpdateFails();
      if (exception == null) {
        LOG.error("Set acl {} for path {} failed", getAcls(), getPath());
      } else {
        LOG.error("Set acl {} for path {} failed!", getAcls(), getPath(),
            exception);
      }
    }
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    ozoneManager.getMetrics().incNumSetAcl();
    return super.validateAndUpdateCache(ozoneManager, context);
  }
}

