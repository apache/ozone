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

import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for DeleteTenant request.
 */
@CleanupTableInfo(cleanupTables = {
    TENANT_STATE_TABLE,
    VOLUME_TABLE
})
public class OMTenantDeleteResponse extends OMClientResponse {

  private String volumeName;
  private OmVolumeArgs omVolumeArgs;
  private String tenantId;

  public OMTenantDeleteResponse(@Nonnull OMResponse omResponse,
                                @Nonnull String volumeName,
                                @Nullable OmVolumeArgs omVolumeArgs,
                                @Nonnull String tenantId) {
    super(omResponse);
    this.volumeName = volumeName;
    this.omVolumeArgs = omVolumeArgs;
    this.tenantId = tenantId;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMTenantDeleteResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    omMetadataManager.getTenantStateTable().deleteWithBatch(
        batchOperation, tenantId);

    if (!volumeName.isEmpty()) {
      Objects.requireNonNull(omVolumeArgs, "omVolumeArgs == null");
      Preconditions.checkState(omVolumeArgs.getVolume().equals(volumeName));

      final String dbVolumeKey = omMetadataManager.getVolumeKey(volumeName);
      omMetadataManager.getVolumeTable().putWithBatch(batchOperation,
          dbVolumeKey, omVolumeArgs);
    }
  }
}
