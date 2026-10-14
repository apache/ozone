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

package org.apache.hadoop.ozone.om.response.key;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for CommitKey request - prefix layout1.
 */
public class OMKeyCommitResponseWithFSO extends OMKeyCommitResponse {

  private long volumeId;
  private Long replacedObjectId;
  private OmDirectoryInfo nfsParentDirectoryInfo;
  private String nfsFileLeaseKey;
  private OmNfsLeaseInfo nfsFileLease;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public OMKeyCommitResponseWithFSO(
      @Nonnull OMResponse omResponse,
      @Nonnull OmKeyInfo omKeyInfo,
      String ozoneKeyName, String openKeyName,
      @Nonnull OmBucketInfo omBucketInfo,
      Map<String, RepeatedOmKeyInfo> deleteKeyMap, long volumeId,
      boolean isHSync,
      OmKeyInfo newOpenKeyInfo, String openKeyNameToUpdate, OmKeyInfo openKeyToUpdate) {
    this(omResponse, omKeyInfo, ozoneKeyName, openKeyName, omBucketInfo, deleteKeyMap, volumeId,
        isHSync, newOpenKeyInfo, openKeyNameToUpdate, openKeyToUpdate, null);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public OMKeyCommitResponseWithFSO(
      @Nonnull OMResponse omResponse, @Nonnull OmKeyInfo omKeyInfo,
      String ozoneKeyName, String openKeyName, @Nonnull OmBucketInfo omBucketInfo,
      Map<String, RepeatedOmKeyInfo> deleteKeyMap, long volumeId, boolean isHSync,
      OmKeyInfo newOpenKeyInfo, String openKeyNameToUpdate, OmKeyInfo openKeyToUpdate, Long replacedObjectId) {
    this(omResponse, omKeyInfo, ozoneKeyName, openKeyName, omBucketInfo, deleteKeyMap, volumeId, isHSync,
        newOpenKeyInfo, openKeyNameToUpdate, openKeyToUpdate, replacedObjectId, null);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public OMKeyCommitResponseWithFSO(
      @Nonnull OMResponse omResponse, @Nonnull OmKeyInfo omKeyInfo,
      String ozoneKeyName, String openKeyName, @Nonnull OmBucketInfo omBucketInfo,
      Map<String, RepeatedOmKeyInfo> deleteKeyMap, long volumeId, boolean isHSync,
      OmKeyInfo newOpenKeyInfo, String openKeyNameToUpdate, OmKeyInfo openKeyToUpdate, Long replacedObjectId,
      OmDirectoryInfo nfsParentDirectoryInfo) {
    this(omResponse, omKeyInfo, ozoneKeyName, openKeyName, omBucketInfo, deleteKeyMap, volumeId, isHSync,
        newOpenKeyInfo, openKeyNameToUpdate, openKeyToUpdate, replacedObjectId, nfsParentDirectoryInfo, null, null);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public OMKeyCommitResponseWithFSO(
      @Nonnull OMResponse omResponse, @Nonnull OmKeyInfo omKeyInfo,
      String ozoneKeyName, String openKeyName, @Nonnull OmBucketInfo omBucketInfo,
      Map<String, RepeatedOmKeyInfo> deleteKeyMap, long volumeId, boolean isHSync,
      OmKeyInfo newOpenKeyInfo, String openKeyNameToUpdate, OmKeyInfo openKeyToUpdate, Long replacedObjectId,
      OmDirectoryInfo nfsParentDirectoryInfo, String nfsFileLeaseKey, OmNfsLeaseInfo nfsFileLease) {
    super(omResponse, omKeyInfo, ozoneKeyName, openKeyName,
        omBucketInfo, deleteKeyMap, isHSync, newOpenKeyInfo, openKeyNameToUpdate, openKeyToUpdate);
    this.volumeId = volumeId;
    this.replacedObjectId = replacedObjectId;
    this.nfsParentDirectoryInfo = nfsParentDirectoryInfo;
    this.nfsFileLeaseKey = nfsFileLeaseKey;
    this.nfsFileLease = nfsFileLease;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMKeyCommitResponseWithFSO(@Nonnull OMResponse omResponse,
                                    @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
                           BatchOperation batchOperation) throws IOException {

    // Delete from OpenKey table if commit
    if (!this.isHSync()) {
      omMetadataManager.getOpenKeyTable(getBucketLayout())
          .deleteWithBatch(batchOperation, getOpenKeyName());
    } else if (getNewOpenKeyInfo() != null) {
      omMetadataManager.getOpenKeyTable(getBucketLayout()).putWithBatch(
          batchOperation, getOpenKeyName(), getNewOpenKeyInfo());
    }

    if (replacedObjectId != null && replacedObjectId != getOmKeyInfo().getObjectID()) {
      OMFileRequest.deleteNfsObjectFromDB(omMetadataManager, batchOperation,
          volumeId, getOmBucketInfo().getObjectID(), replacedObjectId);
    }
    OMFileRequest.addToFileTable(omMetadataManager, batchOperation,
        getOmKeyInfo(), volumeId, getOmBucketInfo().getObjectID());
    if (nfsParentDirectoryInfo != null) {
      OMFileRequest.addToDirectoryTable(omMetadataManager, batchOperation, nfsParentDirectoryInfo,
          volumeId, getOmBucketInfo().getObjectID());
    }
    if (nfsFileLease != null) {
      omMetadataManager.getNfsFileLeaseTable().putWithBatch(batchOperation, nfsFileLeaseKey, nfsFileLease);
    }

    updateDeletedTable(omMetadataManager, batchOperation);
    handleOpenKeyToUpdate(omMetadataManager, batchOperation);

    // update bucket usedBytes.
    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        omMetadataManager.getBucketKey(getOmBucketInfo().getVolumeName(),
            getOmBucketInfo().getBucketName()), getOmBucketInfo());
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
