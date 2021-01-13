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

package org.apache.hadoop.ozone.om.request.bucket.acl;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.util.BooleanBiFunction;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.acl.OMBucketAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .AddAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;

/**
 * Handle add Acl request for bucket.
 */
public class OMBucketAddAclRequest extends OMBucketAclRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMBucketAddAclRequest.class);

  private static BooleanBiFunction<List<OzoneAcl>, OmBucketInfo> bucketAddAclOp;
  private String path;
  private List<OzoneAcl> ozoneAcls;
  private OzoneObj obj;

  static {
    bucketAddAclOp = (ozoneAcls, omBucketInfo) -> {
      return omBucketInfo.addAcl(ozoneAcls.get(0));
    };
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    long modificationTime = Time.now();
    OzoneManagerProtocolProtos.AddAclRequest.Builder addAclRequestBuilder =
        getOmRequest().getAddAclRequest().toBuilder()
            .setModificationTime(modificationTime);

    return getOmRequest().toBuilder()
        .setAddAclRequest(addAclRequestBuilder)
        .setUserInfo(getUserInfo())
        .build();
  }

  public OMBucketAddAclRequest(OMRequest omRequest) {
    super(omRequest, bucketAddAclOp);
    OzoneManagerProtocolProtos.AddAclRequest addAclRequest =
        getOmRequest().getAddAclRequest();
    obj = OzoneObjInfo.fromProtobuf(addAclRequest.getObj());
    path = obj.getPath();
    ozoneAcls = Lists.newArrayList(
        OzoneAcl.fromProtobuf(addAclRequest.getAcl()));
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
    omResponse.setAddAclResponse(AddAclResponse.newBuilder()
         .setResponse(operationResult));
    return new OMBucketAclResponse(omResponse.build(), omBucketInfo);
  }

  @Override
  OMClientResponse onFailure(OMResponse.Builder omResponse,
      IOException exception) {
    return new OMBucketAclResponse(
        createErrorOMResponse(omResponse, exception));
  }

  @Override
  void onComplete(boolean operationResult, IOException exception,
      OMMetrics omMetrics, AuditLogger auditLogger,
      Map<String, String> auditMap) {
    auditLog(auditLogger, buildAuditMessage(OMAction.ADD_ACL, auditMap,
        exception, getOmRequest().getUserInfo()));

    if (operationResult) {
      LOG.debug("Add acl: {} to path: {} success!", getAcls(), getPath());
    } else {
      omMetrics.incNumBucketUpdateFails();
      if (exception == null) {
        LOG.error("Add acl {} to path {} failed, because acl already exist",
            getAcls(), getPath());
      } else {
        LOG.error("Add acl {} to path {} failed!", getAcls(), getPath(),
            exception);
      }
    }
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    ozoneManager.getMetrics().incNumAddAcl();
    return super.validateAndUpdateCache(ozoneManager, trxnLogIndex,
        omDoubleBufferHelper);
  }
}

