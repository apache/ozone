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

package org.apache.hadoop.ozone.om.response.s3.multipart;

import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.MULTIPARTFILEINFO_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_FILE_TABLE;

/**
 * Response for Multipart Abort Request layout version V1.
 */
@CleanupTableInfo(cleanupTables = {OPEN_FILE_TABLE, DELETED_TABLE,
    MULTIPARTFILEINFO_TABLE})
public class S3MultipartUploadAbortResponseV1
    extends S3MultipartUploadAbortResponse {

  public S3MultipartUploadAbortResponseV1(@Nonnull OMResponse omResponse,
      String multipartKey, @Nonnull OmMultipartKeyInfo omMultipartKeyInfo,
      boolean isRatisEnabled, @Nonnull OmBucketInfo omBucketInfo) {

    super(omResponse, multipartKey, omMultipartKeyInfo, isRatisEnabled,
        omBucketInfo);
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public S3MultipartUploadAbortResponseV1(@Nonnull OMResponse omResponse) {
    super(omResponse);
  }
}
