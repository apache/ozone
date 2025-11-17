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

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.TENANT_STATE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VOLUME_TABLE;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserVolumeInfo;

/**
 * Response for OMTenantCreate request.
 */
@CleanupTableInfo(cleanupTables = {
    TENANT_STATE_TABLE,
    VOLUME_TABLE
})
public class OMTenantCreateResponse extends OMClientResponse {

  private PersistedUserVolumeInfo userVolumeInfo;
  private OmVolumeArgs omVolumeArgs;
  private OmDBTenantState omTenantState;

  public OMTenantCreateResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmVolumeArgs omVolumeArgs,
      PersistedUserVolumeInfo userVolumeInfo,
      @Nonnull OmDBTenantState omTenantState
  ) {
    super(omResponse);
    this.omVolumeArgs = omVolumeArgs;
    this.userVolumeInfo = userVolumeInfo;
    this.omTenantState = omTenantState;
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

    final String tenantId = omTenantState.getTenantId();
    omMetadataManager.getTenantStateTable().putWithBatch(
        batchOperation, tenantId, omTenantState);

    // From OMVolumeCreateResponse
    String dbVolumeKey =
        omMetadataManager.getVolumeKey(omVolumeArgs.getVolume());
    omMetadataManager.getVolumeTable().putWithBatch(batchOperation,
        dbVolumeKey, omVolumeArgs);

    // userVolumeInfo can be null when skipped volume creation
    if (userVolumeInfo != null) {
      String dbUserKey =
          omMetadataManager.getUserKey(omVolumeArgs.getOwnerName());
      omMetadataManager.getUserTable().putWithBatch(batchOperation, dbUserKey,
          userVolumeInfo);
    }
  }

  @VisibleForTesting
  public OmDBTenantState getOmDBTenantState() {
    return omTenantState;
  }
}
