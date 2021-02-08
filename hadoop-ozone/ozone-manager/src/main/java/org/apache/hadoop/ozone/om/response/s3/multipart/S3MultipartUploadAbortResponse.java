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

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .PartKeyInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.MULTIPARTINFO_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;

/**
 * Response for Multipart Abort Request.
 */
@CleanupTableInfo(cleanupTables = {OPEN_KEY_TABLE, DELETED_TABLE,
    MULTIPARTINFO_TABLE})
public class S3MultipartUploadAbortResponse extends OMClientResponse {

  private String multipartKey;
  private OmMultipartKeyInfo omMultipartKeyInfo;
  private boolean isRatisEnabled;
  private OmBucketInfo omBucketInfo;

  public S3MultipartUploadAbortResponse(@Nonnull OMResponse omResponse,
      String multipartKey, @Nonnull OmMultipartKeyInfo omMultipartKeyInfo,
      boolean isRatisEnabled, @Nonnull OmBucketInfo omBucketInfo) {
    super(omResponse);
    this.multipartKey = multipartKey;
    this.omMultipartKeyInfo = omMultipartKeyInfo;
    this.isRatisEnabled = isRatisEnabled;
    this.omBucketInfo = omBucketInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public S3MultipartUploadAbortResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    // Delete from openKey table and multipart info table.
    omMetadataManager.getOpenKeyTable().deleteWithBatch(batchOperation,
        multipartKey);
    omMetadataManager.getMultipartInfoTable().deleteWithBatch(batchOperation,
        multipartKey);

    // Move all the parts to delete table
    TreeMap<Integer, PartKeyInfo > partKeyInfoMap =
        omMultipartKeyInfo.getPartKeyInfoMap();
    for (Map.Entry<Integer, PartKeyInfo > partKeyInfoEntry :
        partKeyInfoMap.entrySet()) {
      PartKeyInfo partKeyInfo = partKeyInfoEntry.getValue();
      OmKeyInfo currentKeyPartInfo =
          OmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());

      RepeatedOmKeyInfo repeatedOmKeyInfo =
          omMetadataManager.getDeletedTable().get(partKeyInfo.getPartName());

      repeatedOmKeyInfo = OmUtils.prepareKeyForDelete(currentKeyPartInfo,
          repeatedOmKeyInfo, omMultipartKeyInfo.getUpdateID(), isRatisEnabled);

      omMetadataManager.getDeletedTable().putWithBatch(batchOperation,
          partKeyInfo.getPartName(), repeatedOmKeyInfo);

      // update bucket usedBytes.
      omMetadataManager.getBucketTable().putWithBatch(batchOperation,
          omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
              omBucketInfo.getBucketName()), omBucketInfo);
    }
  }
}
