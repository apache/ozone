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

package org.apache.hadoop.ozone.om.response.s3.security;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.TENANT_ACCESS_ID_TABLE;

/**
 * Response for SetSecret request.
 */
@CleanupTableInfo(cleanupTables = {TENANT_ACCESS_ID_TABLE})
public class OMSetSecretResponse extends OMClientResponse {

  private String accessId;
  private OmDBAccessIdInfo dbAccessIdInfo;

  public OMSetSecretResponse(@Nullable String accessId,
                             @Nullable OmDBAccessIdInfo dbAccessIdInfo,
                             @Nonnull OMResponse omResponse) {
    super(omResponse);
    this.accessId = accessId;
    this.dbAccessIdInfo = dbAccessIdInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMSetSecretResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    assert(getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK);
    omMetadataManager.getTenantAccessIdTable().putWithBatch(batchOperation,
            accessId, dbAccessIdInfo);
  }
}
