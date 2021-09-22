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
import org.apache.hadoop.ozone.om.helpers.OmDBTenantInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.TENANT_POLICY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.TENANT_STATE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.VOLUME_TABLE;

/**
 * Response for OMTenantCreate request.
 */
@CleanupTableInfo(cleanupTables = {
    TENANT_STATE_TABLE,
    TENANT_POLICY_TABLE,
    VOLUME_TABLE
})
public class OMTenantCreateResponse extends OMClientResponse {

  private OzoneManagerStorageProtos.PersistedUserVolumeInfo userVolumeInfo;
  private OmVolumeArgs omVolumeArgs;

  private OmDBTenantInfo omTenantInfo;
  private String userPolicyId, bucketPolicyId;

  public OMTenantCreateResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmVolumeArgs omVolumeArgs,
      @Nonnull OzoneManagerStorageProtos.PersistedUserVolumeInfo userVolumeInfo,
      @Nonnull OmDBTenantInfo omTenantInfo,
      @Nonnull String userPolicyId,
      @Nonnull String bucketPolicyId
  ) {
    super(omResponse);
    this.omVolumeArgs = omVolumeArgs;
    this.userVolumeInfo = userVolumeInfo;
    this.omTenantInfo = omTenantInfo;
    this.userPolicyId = userPolicyId;
    this.bucketPolicyId = bucketPolicyId;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMTenantCreateResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    final String tenantName = omTenantInfo.getTenantName();
    omMetadataManager.getTenantStateTable().putWithBatch(
        batchOperation, tenantName, omTenantInfo);

    final String userPolicyGroupName =
        omTenantInfo.getUserPolicyGroupName();
    omMetadataManager.getTenantPolicyTable().putWithBatch(
        batchOperation, userPolicyGroupName, userPolicyId);

    final String bucketPolicyGroupName =
        omTenantInfo.getBucketPolicyGroupName();
    omMetadataManager.getTenantPolicyTable().putWithBatch(
        batchOperation, bucketPolicyGroupName, bucketPolicyId);

    // From OMVolumeCreateResponse
    String dbVolumeKey =
        omMetadataManager.getVolumeKey(omVolumeArgs.getVolume());
    String dbUserKey =
        omMetadataManager.getUserKey(omVolumeArgs.getOwnerName());

    omMetadataManager.getVolumeTable().putWithBatch(batchOperation,
        dbVolumeKey, omVolumeArgs);
    omMetadataManager.getUserTable().putWithBatch(batchOperation, dbUserKey,
        userVolumeInfo);
  }

  @VisibleForTesting
  public OmDBTenantInfo getOmDBTenantInfo() {
    return omTenantInfo;
  }
}
