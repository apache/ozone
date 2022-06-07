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
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.S3_SECRET_TABLE;

/**
 * Response for SetSecret request.
 */
@CleanupTableInfo(cleanupTables = {S3_SECRET_TABLE})
public class OMSetSecretResponse extends OMClientResponse {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMSetSecretResponse.class);

  private String accessId;
  private S3SecretValue s3SecretValue;

  public OMSetSecretResponse(@Nullable String accessId,
                             @Nullable S3SecretValue s3SecretValue,
                             @Nonnull OMResponse omResponse) {
    super(omResponse);
    this.accessId = accessId;
    this.s3SecretValue = s3SecretValue;
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

    assert (getOMResponse().getStatus()
        == OzoneManagerProtocolProtos.Status.OK);

    if (s3SecretValue != null) {
      LOG.debug("Updating TenantAccessIdTable");
      omMetadataManager.getS3SecretTable().putWithBatch(batchOperation,
          accessId, s3SecretValue);
    }
  }
}
