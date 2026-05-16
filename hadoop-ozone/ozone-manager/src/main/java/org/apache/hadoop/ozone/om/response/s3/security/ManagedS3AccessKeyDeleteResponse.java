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

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.S3_MANAGED_ACCESS_KEY_TABLE;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response that deletes a managed S3 access-key row.
 */
@CleanupTableInfo(cleanupTables = {S3_MANAGED_ACCESS_KEY_TABLE})
public class ManagedS3AccessKeyDeleteResponse extends OMClientResponse {

  private final String accessKeyId;

  public ManagedS3AccessKeyDeleteResponse(@Nullable String accessKeyId,
      @Nonnull OMResponse omResponse) {
    super(omResponse);
    this.accessKeyId = accessKeyId;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    if (accessKeyId != null && getOMResponse().getStatus()
        == OzoneManagerProtocolProtos.Status.OK) {
      omMetadataManager.getS3ManagedAccessKeyTable().deleteWithBatch(
          batchOperation, accessKeyId);
    }
  }
}
