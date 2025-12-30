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

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.S3_REVOKED_STS_TOKEN_TABLE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.time.Clock;
import java.time.ZoneOffset;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for RevokeSTSToken request.
 */
@CleanupTableInfo(cleanupTables = {S3_REVOKED_STS_TOKEN_TABLE})
public class S3RevokeSTSTokenResponse extends OMClientResponse {

  private static final Clock CLOCK = Clock.system(ZoneOffset.UTC);

  private final String sessionToken;

  public S3RevokeSTSTokenResponse(String sessionToken, @Nonnull OMResponse omResponse) {
    super(omResponse);
    this.sessionToken = sessionToken;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager, BatchOperation batchOperation) throws IOException {
    if (sessionToken != null && getOMResponse().hasStatus() &&  getOMResponse().getStatus() == OK) {
      final Table<String, Long> table = omMetadataManager.getS3RevokedStsTokenTable();
      if (table != null) {
        // Store insertionTimeMillis as value
        table.putWithBatch(batchOperation, sessionToken, CLOCK.millis());
      }
    }
  }
}
