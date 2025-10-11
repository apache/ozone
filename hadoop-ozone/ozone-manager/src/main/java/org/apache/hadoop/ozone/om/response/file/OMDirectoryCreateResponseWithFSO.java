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

package org.apache.hadoop.ozone.om.response.file;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.request.file.OMDirectoryCreateRequest.Result;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.key.OmKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Response for create directory request.
 */
@CleanupTableInfo(cleanupTables = {DIRECTORY_TABLE})
public class OMDirectoryCreateResponseWithFSO extends OmKeyResponse {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDirectoryCreateResponseWithFSO.class);

  private OmDirectoryInfo dirInfo;
  private List<OmDirectoryInfo> parentDirInfos;
  private Result result;
  private long volumeId;
  private long bucketId;
  private OmBucketInfo bucketInfo;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public OMDirectoryCreateResponseWithFSO(@Nonnull OMResponse omResponse,
      @Nonnull long volumeId, @Nonnull long bucketId,
      @Nonnull OmDirectoryInfo dirInfo,
      @Nonnull List<OmDirectoryInfo> pDirInfos, @Nonnull Result result,
      @Nonnull BucketLayout bucketLayout, @Nonnull OmBucketInfo bucketInfo) {
    super(omResponse, bucketLayout);
    this.dirInfo = dirInfo;
    this.parentDirInfos = pDirInfos;
    this.result = result;
    this.volumeId = volumeId;
    this.bucketId = bucketId;
    this.bucketInfo = bucketInfo;
  }

  /**
   * For when the request is not successful or the directory already exists.
   */
  public OMDirectoryCreateResponseWithFSO(@Nonnull OMResponse omResponse,
                                     @Nonnull Result result) {
    super(omResponse);
    this.result = result;
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
                              BatchOperation batchOperation)
          throws IOException {
    addToDirectoryTable(omMetadataManager, batchOperation);
  }

  private void addToDirectoryTable(OMMetadataManager omMetadataManager,
                                BatchOperation batchOperation)
          throws IOException {
    if (dirInfo != null) {
      if (parentDirInfos != null) {
        for (OmDirectoryInfo parentDirInfo : parentDirInfos) {
          String parentKey = omMetadataManager
                  .getOzonePathKey(volumeId, bucketId,
                          parentDirInfo.getParentObjectID(),
                          parentDirInfo.getName());
          LOG.debug("putWithBatch parent : dir {} info : {}", parentKey,
                  parentDirInfo);
          omMetadataManager.getDirectoryTable()
                  .putWithBatch(batchOperation, parentKey, parentDirInfo);
        }
      }

      String dirKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
              dirInfo.getParentObjectID(), dirInfo.getName());
      omMetadataManager.getDirectoryTable().putWithBatch(batchOperation, dirKey,
              dirInfo);
      String bucketKey = omMetadataManager.getBucketKey(
          bucketInfo.getVolumeName(), bucketInfo.getBucketName());
      omMetadataManager.getBucketTable().putWithBatch(batchOperation,
          bucketKey, bucketInfo);
    } else {
      // When directory already exists, we don't add it to cache. And it is
      // not an error, in this case dirKeyInfo will be null.
      LOG.debug("Response Status is OK, dirKeyInfo is null in " +
              "OMDirectoryCreateResponseWithFSO");
    }
  }
}
