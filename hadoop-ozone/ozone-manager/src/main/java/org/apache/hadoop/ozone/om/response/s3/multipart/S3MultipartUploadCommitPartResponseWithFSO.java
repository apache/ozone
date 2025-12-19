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

package org.apache.hadoop.ozone.om.response.s3.multipart;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.MULTIPART_INFO_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_FILE_TABLE;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for S3MultipartUploadCommitPartWithFSO request.
 */
@CleanupTableInfo(cleanupTables = {OPEN_FILE_TABLE, DELETED_TABLE,
    MULTIPART_INFO_TABLE, BUCKET_TABLE})
public class S3MultipartUploadCommitPartResponseWithFSO
        extends S3MultipartUploadCommitPartResponse {

  /**
   * Regular response.
   * 1. Update MultipartKey in MultipartInfoTable with new PartKeyInfo
   * 2. Delete openKey from OpenKeyTable
   * 3. If old key or uncommitted (pseudo) key exists, put it in DeletedTable
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public S3MultipartUploadCommitPartResponseWithFSO(
      @Nonnull OMResponse omResponse, String multipartKey, String openKey,
      @Nullable OmMultipartKeyInfo omMultipartKeyInfo,
      @Nullable Map<String, RepeatedOmKeyInfo> keyToDeleteMap,
      @Nullable OmKeyInfo openPartKeyInfoToBeDeleted,
      @Nonnull OmBucketInfo omBucketInfo, long bucketId, @Nonnull BucketLayout bucketLayout) {

    super(omResponse, multipartKey, openKey, omMultipartKeyInfo,
            keyToDeleteMap, openPartKeyInfoToBeDeleted,
            omBucketInfo, bucketId, bucketLayout);
  }
}
