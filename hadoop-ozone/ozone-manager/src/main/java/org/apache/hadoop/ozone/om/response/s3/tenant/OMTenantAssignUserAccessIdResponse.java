/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om.response.s3.tenant;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBUserPrincipalInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.PRINCIPAL_TO_ACCESS_IDS_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.S3_SECRET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.TENANT_ACCESS_ID_TABLE;

/**
 * Response for OMAssignUserToTenantRequest.
 */
@CleanupTableInfo(cleanupTables = {
    S3_SECRET_TABLE,
    TENANT_ACCESS_ID_TABLE,
    PRINCIPAL_TO_ACCESS_IDS_TABLE
})
public class OMTenantAssignUserAccessIdResponse extends OMClientResponse {

  private S3SecretValue s3SecretValue;
  private String principal, accessId;
  private OmDBAccessIdInfo omDBAccessIdInfo;
  private OmDBUserPrincipalInfo omDBUserPrincipalInfo;

  @SuppressWarnings("checkstyle:parameternumber")
  public OMTenantAssignUserAccessIdResponse(@Nonnull OMResponse omResponse,
      @Nonnull S3SecretValue s3SecretValue,
      @Nonnull String principal,
      @Nonnull String accessId,
      @Nonnull OmDBAccessIdInfo omDBAccessIdInfo,
      @Nonnull OmDBUserPrincipalInfo omDBUserPrincipalInfo
  ) {
    super(omResponse);
    this.s3SecretValue = s3SecretValue;
    this.principal = principal;
    this.accessId = accessId;
    this.omDBAccessIdInfo = omDBAccessIdInfo;
    this.omDBUserPrincipalInfo = omDBUserPrincipalInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMTenantAssignUserAccessIdResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    if (s3SecretValue != null &&
        getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      // Add S3SecretTable entry
      omMetadataManager.getS3SecretTable().putWithBatch(batchOperation,
          accessId, s3SecretValue);
    }

    omMetadataManager.getTenantAccessIdTable().putWithBatch(
        batchOperation, accessId, omDBAccessIdInfo);
    omMetadataManager.getPrincipalToAccessIdsTable().putWithBatch(
        batchOperation, principal, omDBUserPrincipalInfo);
  }

  @VisibleForTesting
  public OmDBAccessIdInfo getOmDBAccessIdInfo() {
    return omDBAccessIdInfo;
  }

  @VisibleForTesting
  public S3SecretValue getS3Secret() {
    return s3SecretValue;
  }
}
