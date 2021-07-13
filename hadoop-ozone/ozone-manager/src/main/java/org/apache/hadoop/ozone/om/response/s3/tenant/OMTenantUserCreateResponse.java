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

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.S3_SECRET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.TENANT_GROUP_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.TENANT_ROLE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.TENANT_USER_TABLE;

/**
 * Response for OMTenantUserCreate request.
 */
@CleanupTableInfo(cleanupTables = {
    S3_SECRET_TABLE,
    TENANT_USER_TABLE,
    TENANT_GROUP_TABLE,
    TENANT_ROLE_TABLE
})
public class OMTenantUserCreateResponse extends OMClientResponse {

  private S3SecretValue s3SecretValue;
  private String principal, tenantName, groupName, roleName;

  public OMTenantUserCreateResponse(@Nonnull OMResponse omResponse,
      @Nullable S3SecretValue s3SecretValue,
      @Nullable String principal,
      @Nullable String tenantName,
      @Nullable String groupName,
      @Nullable String roleName
  ) {
    super(omResponse);
    this.s3SecretValue = s3SecretValue;
    this.principal = principal;
    this.tenantName = tenantName;
    this.groupName = groupName;
    this.roleName = roleName;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMTenantUserCreateResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    if (s3SecretValue != null &&
        getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      omMetadataManager.getS3SecretTable().putWithBatch(batchOperation,
          s3SecretValue.getKerberosID(), s3SecretValue);
    }

    omMetadataManager.getTenantUserTable().putWithBatch(
        batchOperation, principal, tenantName);
    omMetadataManager.getTenantGroupTable().putWithBatch(
        batchOperation, principal, groupName);
    omMetadataManager.getTenantRoleTable().putWithBatch(
        batchOperation, principal, roleName);
  }
}
