/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.response.s3.multipart;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartAbortInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.key.OmKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.MULTIPARTINFO_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;

/**
 * Base class for responses that need to move multipart info part keys to the
 * deleted table.
 */
@CleanupTableInfo(cleanupTables = {OPEN_KEY_TABLE, OPEN_FILE_TABLE,
    DELETED_TABLE, MULTIPARTINFO_TABLE, BUCKET_TABLE})
public abstract class AbstractS3MultipartAbortResponse extends OmKeyResponse {

  private boolean isRatisEnabled;

  public AbstractS3MultipartAbortResponse(
      @Nonnull OMResponse omResponse, boolean isRatisEnabled) {
    super(omResponse);
    this.isRatisEnabled = isRatisEnabled;
  }

  public AbstractS3MultipartAbortResponse(
      @Nonnull OMResponse omResponse, boolean isRatisEnabled,
      BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    this.isRatisEnabled =  isRatisEnabled;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public AbstractS3MultipartAbortResponse(@Nonnull OMResponse omResponse,
        BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    checkStatusNotOK();
  }

  /**
   * Adds the operation of aborting a list of multipart uploads under the
   * same bucket.
   * @param omMetadataManager
   * @param batchOperation
   * @param omBucketInfo
   * @param multipartAbortInfo
   * @throws IOException
   */
  protected void addAbortToBatch(
      OMMetadataManager omMetadataManager,
      BatchOperation batchOperation,
      OmBucketInfo omBucketInfo,
      List<OmMultipartAbortInfo> multipartAbortInfo
  ) throws IOException {
    for (OmMultipartAbortInfo abortInfo: multipartAbortInfo) {
      // Delete from openKey table and multipart info table.
      omMetadataManager.getOpenKeyTable(abortInfo.getBucketLayout())
          .deleteWithBatch(batchOperation, abortInfo.getMultipartOpenKey());
      omMetadataManager.getMultipartInfoTable().deleteWithBatch(batchOperation,
          abortInfo.getMultipartKey());

      OmMultipartKeyInfo omMultipartKeyInfo = abortInfo
          .getOmMultipartKeyInfo();
      // Move all the parts to delete table
      for (PartKeyInfo partKeyInfo: omMultipartKeyInfo.getPartKeyInfoMap()) {
        OmKeyInfo currentKeyPartInfo =
            OmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());

        // TODO: Similar to open key deletion response, we can check if the
        //  MPU part actually contains blocks, and only move the to
        //  deletedTable if it does.

        RepeatedOmKeyInfo repeatedOmKeyInfo = OmUtils.prepareKeyForDelete(
            currentKeyPartInfo, omMultipartKeyInfo.getUpdateID(),
            isRatisEnabled);

        // multi-part key format is volumeName/bucketName/keyName/uploadId
        String deleteKey = omMetadataManager.getOzoneDeletePathKey(
            currentKeyPartInfo.getObjectID(), abortInfo.getMultipartKey());

        omMetadataManager.getDeletedTable().putWithBatch(batchOperation,
            deleteKey, repeatedOmKeyInfo);
      }
    }
    // update bucket usedBytes.
    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
            omBucketInfo.getBucketName()), omBucketInfo);
  }

  /**
   * Adds the operation of aborting a multipart upload to the batch operation.
   * Both LEGACY/OBS and FSO have similar abort logic. The only difference
   * is the multipartOpenKey used in the openKeyTable and openFileTable.
   */
  protected void addAbortToBatch(
      OMMetadataManager omMetadataManager,
      BatchOperation batchOperation,
      String multipartKey,
      String multipartOpenKey,
      OmMultipartKeyInfo omMultipartKeyInfo,
      OmBucketInfo omBucketInfo,
      BucketLayout bucketLayout) throws IOException {
    OmMultipartAbortInfo omMultipartAbortInfo =
        new OmMultipartAbortInfo.Builder()
            .setMultipartKey(multipartKey)
            .setMultipartOpenKey(multipartOpenKey)
            .setMultipartKeyInfo(omMultipartKeyInfo)
            .setBucketLayout(bucketLayout)
            .build();
    addAbortToBatch(omMetadataManager, batchOperation, omBucketInfo,
        Collections.singletonList(omMultipartAbortInfo));
  }

}
