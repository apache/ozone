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
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.S3ManagedAccessKeyInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.ManagedS3AccessKeyDeleteResponse;
import org.apache.hadoop.ozone.om.upgrade.BelongsToLayoutVersion;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteManagedS3AccessKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Handles delete managed S3 access-key requests.
 */
@BelongsToLayoutVersion(OMLayoutFeature.MANAGED_LOCAL_S3_ACCESS_KEYS)
public class DeleteManagedS3AccessKeyRequest extends OMClientRequest {

  public DeleteManagedS3AccessKeyRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    OMRequest omRequest = super.preExecute(ozoneManager);
    ManagedS3AccessKeyRequestHelper.checkEnabled(ozoneManager);
    ManagedS3AccessKeyRequestHelper.checkLayoutFinalized(ozoneManager);
    ManagedS3AccessKeyRequestHelper.checkAdmin(ozoneManager,
        omRequest.getUserInfo());
    return omRequest;
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      ExecutionContext context) {
    OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    String accessKeyId = getOmRequest().getDeleteManagedS3AccessKeyRequest()
        .getAccessKeyId();
    Exception exception = null;
    try {
      ManagedS3AccessKeyRequestHelper.checkEnabled(ozoneManager);
      ManagedS3AccessKeyRequestHelper.checkLayoutFinalized(ozoneManager);
      S3ManagedAccessKeyInfo existing = ozoneManager.getMetadataManager()
          .getS3ManagedAccessKeyTable().get(accessKeyId);
      if (existing == null) {
        throw new OMException("Managed S3 access key '" + accessKeyId +
            "' not found", ResultCodes.MANAGED_S3_ACCESS_KEY_NOT_FOUND);
      }
      ozoneManager.getMetadataManager().getS3ManagedAccessKeyTable()
          .addCacheEntry(new CacheKey<>(accessKeyId),
              CacheValue.get(context.getIndex()));
      DeleteManagedS3AccessKeyResponse response =
          DeleteManagedS3AccessKeyResponse.newBuilder()
              .setAccessKeyId(accessKeyId)
              .build();
      markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
          OMAction.DELETE_MANAGED_S3_ACCESS_KEY,
          ManagedS3AccessKeyRequestHelper.auditMap(existing), null,
          getOmRequest().getUserInfo()));
      return new ManagedS3AccessKeyDeleteResponse(accessKeyId,
          omResponse.setDeleteManagedS3AccessKeyResponse(response).build());
    } catch (Exception ex) {
      exception = ex;
      markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
          OMAction.DELETE_MANAGED_S3_ACCESS_KEY,
          ManagedS3AccessKeyRequestHelper.auditMap(accessKeyId), exception,
          getOmRequest().getUserInfo()));
      return new ManagedS3AccessKeyDeleteResponse(null,
          createErrorOMResponse(omResponse, exception));
    }
  }
}
