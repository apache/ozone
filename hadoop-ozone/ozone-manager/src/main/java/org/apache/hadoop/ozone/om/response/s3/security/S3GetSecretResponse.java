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

package org.apache.hadoop.ozone.om.response.s3.security;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.S3_SECRET_TABLE;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.S3SecretManager;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for GetS3Secret request.
 */
@CleanupTableInfo(cleanupTables = {S3_SECRET_TABLE})
public class S3GetSecretResponse extends OMClientResponse {

  private final S3SecretValue s3SecretValue;
  private final S3SecretManager s3SecretManager;

  public S3GetSecretResponse(@Nullable S3SecretValue s3SecretValue,
      @Nonnull S3SecretManager secretManager,
      @Nonnull OMResponse omResponse) {
    super(omResponse);
    this.s3SecretManager = secretManager;
    this.s3SecretValue = s3SecretValue;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    boolean isOk
        = getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK;
    if (s3SecretValue != null && isOk) {
      if (s3SecretManager.isBatchSupported()) {
        s3SecretManager.batcher()
            .addWithBatch(batchOperation, s3SecretValue.getKerberosID(), s3SecretValue);
      } // else - the secret has already been stored in S3GetSecretRequest.
    }
  }

  @VisibleForTesting
  public S3SecretValue getS3SecretValue() {
    return s3SecretValue;
  }
}
