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

package org.apache.hadoop.ozone.om.response.s3.tenant;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.PRINCIPAL_TO_ACCESS_IDS_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.S3_SECRET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.TENANT_ACCESS_ID_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.S3SecretManager;
import org.apache.hadoop.ozone.om.helpers.OmDBUserPrincipalInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for OMTenantRevokeUserAccessIdRequest.
 */
@CleanupTableInfo(cleanupTables = {
    S3_SECRET_TABLE,
    TENANT_ACCESS_ID_TABLE,
    PRINCIPAL_TO_ACCESS_IDS_TABLE
})
public class OMTenantRevokeUserAccessIdResponse extends OMClientResponse {

  private String principal, accessId;
  private OmDBUserPrincipalInfo omDBUserPrincipalInfo;
  private S3SecretManager s3SecretManager;

  @SuppressWarnings("checkstyle:parameternumber")
  public OMTenantRevokeUserAccessIdResponse(@Nonnull OMResponse omResponse,
      @Nonnull String accessId,
      @Nonnull String principal,
      @Nonnull OmDBUserPrincipalInfo omDBUserPrincipalInfo,
      @Nonnull S3SecretManager s3SecretManager
  ) {
    super(omResponse);
    this.principal = principal;
    this.accessId = accessId;
    this.omDBUserPrincipalInfo = omDBUserPrincipalInfo;
    this.s3SecretManager = s3SecretManager;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMTenantRevokeUserAccessIdResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    assert (accessId != null);
    // TODO: redundant check? Is status always OK when addToDBBatch is called
    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      if (s3SecretManager.isBatchSupported()) {
        s3SecretManager.batcher().deleteWithBatch(batchOperation, accessId);
      } else {
        s3SecretManager.revokeSecret(accessId);
      }
    }

    omMetadataManager.getTenantAccessIdTable().deleteWithBatch(
        batchOperation, accessId);

    if (!omDBUserPrincipalInfo.getAccessIds().isEmpty()) {
      omMetadataManager.getPrincipalToAccessIdsTable().putWithBatch(
          batchOperation, principal, omDBUserPrincipalInfo);
    } else {
      // Remove entry from DB if accessId set is empty
      omMetadataManager.getPrincipalToAccessIdsTable().deleteWithBatch(
          batchOperation, principal);
    }
  }
}
