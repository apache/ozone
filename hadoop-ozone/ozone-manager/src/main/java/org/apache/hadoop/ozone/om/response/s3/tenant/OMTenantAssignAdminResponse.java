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

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.TENANT_ACCESS_ID_TABLE;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for OMTenantAssignAdminRequest.
 */
@CleanupTableInfo(cleanupTables = {
    TENANT_ACCESS_ID_TABLE
})
public class OMTenantAssignAdminResponse extends OMClientResponse {

  private String accessId;
  private OmDBAccessIdInfo omDBAccessIdInfo;

  public OMTenantAssignAdminResponse(@Nonnull OMResponse omResponse,
      @Nonnull String accessId,
      @Nonnull OmDBAccessIdInfo omDBAccessIdInfo
  ) {
    super(omResponse);
    this.accessId = accessId;
    this.omDBAccessIdInfo = omDBAccessIdInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMTenantAssignAdminResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    omMetadataManager.getTenantAccessIdTable().putWithBatch(
        batchOperation, accessId, omDBAccessIdInfo);
  }

  @VisibleForTesting
  public OmDBAccessIdInfo getOmDBAccessIdInfo() {
    return omDBAccessIdInfo;
  }
}
