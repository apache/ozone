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
package org.apache.hadoop.ozone.om.request.key;

import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.ErrorInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeysDeleteResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.PARTIAL_DELETE;

/**
 * Handles DeleteKeys request for recursive bucket deletion.
 */
public class OmKeysDeleteRequestWithFSO extends OMKeysDeleteRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OmKeysDeleteRequestWithFSO.class);

  public OmKeysDeleteRequestWithFSO(
      OzoneManagerProtocolProtos.OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex) {
    return super.validateAndUpdateCache(ozoneManager, trxnLogIndex);
  }

  @Override
  protected OmKeyInfo getOmKeyInfo(
      OzoneManager ozoneManager, OMMetadataManager omMetadataManager,
      String volumeName, String bucketName, String keyName)
      throws IOException {
    OzoneFileStatus keyStatus = getOzoneKeyStatus(
        ozoneManager, omMetadataManager, volumeName, bucketName, keyName);
    return keyStatus != null ? keyStatus.getKeyInfo() : null;
  }

  @Override
  protected void addKeyToAppropriateList(List<OmKeyInfo> omKeyInfoList,
      OmKeyInfo omKeyInfo, List<OmKeyInfo> dirList, OzoneFileStatus keyStatus) {
    if (keyStatus.isDirectory()) {
      dirList.add(omKeyInfo);
    } else {
      omKeyInfoList.add(omKeyInfo);
    }
  }

  @Override
  protected OzoneFileStatus getOzoneKeyStatus(
      OzoneManager ozoneManager, OMMetadataManager omMetadataManager,
      String volumeName, String bucketName, String keyName) throws IOException {
    return OMFileRequest.getOMKeyInfoIfExists(omMetadataManager,
        volumeName, bucketName, keyName, 0,
        ozoneManager.getDefaultReplicationConfig());
  }

  @Override
  protected long markKeysAsDeletedInCache(
          OzoneManager ozoneManager, long trxnLogIndex,
          List<OmKeyInfo> omKeyInfoList,
          List<OmKeyInfo> dirList, OMMetadataManager omMetadataManager,
          long quotaReleased) throws IOException {

    // Mark all keys which can be deleted, in cache as deleted.
    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      final long volumeId = omMetadataManager.getVolumeId(
              omKeyInfo.getVolumeName());
      final long bucketId = omMetadataManager.getBucketId(
              omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());
      omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
          new CacheKey<>(omMetadataManager
              .getOzonePathKey(volumeId, bucketId,
                      omKeyInfo.getParentObjectID(),
                      omKeyInfo.getFileName())),
          CacheValue.get(trxnLogIndex));

      omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());
      quotaReleased += sumBlockLengths(omKeyInfo);
    }
    // Mark directory keys.
    for (OmKeyInfo omKeyInfo : dirList) {
      final long volumeId = omMetadataManager.getVolumeId(
              omKeyInfo.getVolumeName());
      final long bucketId = omMetadataManager.getBucketId(
              omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());
      omMetadataManager.getDirectoryTable().addCacheEntry(new CacheKey<>(
              omMetadataManager.getOzonePathKey(volumeId, bucketId,
                      omKeyInfo.getParentObjectID(),
                  omKeyInfo.getFileName())),
          CacheValue.get(trxnLogIndex));

      omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());
      quotaReleased += sumBlockLengths(omKeyInfo);
    }
    return quotaReleased;
  }

  @NotNull @Override
  protected OMClientResponse getOmClientResponse(OzoneManager ozoneManager,
      List<OmKeyInfo> omKeyInfoList, List<OmKeyInfo> dirList,
      OzoneManagerProtocolProtos.OMResponse.Builder omResponse,
      OzoneManagerProtocolProtos.DeleteKeyArgs.Builder unDeletedKeys,
      Map<String, ErrorInfo> keyToErrors,
      boolean deleteStatus, OmBucketInfo omBucketInfo, long volumeId) {
    OMClientResponse omClientResponse;
    List<OzoneManagerProtocolProtos.DeleteKeyError> deleteKeyErrors = new ArrayList<>();
    for (Map.Entry<String, ErrorInfo>  key : keyToErrors.entrySet()) {
      deleteKeyErrors.add(OzoneManagerProtocolProtos.DeleteKeyError.newBuilder()
          .setKey(key.getKey()).setErrorCode(key.getValue().getCode())
          .setErrorMsg(key.getValue().getMessage()).build());
    }
    omClientResponse = new OMKeysDeleteResponseWithFSO(omResponse
        .setDeleteKeysResponse(
            OzoneManagerProtocolProtos.DeleteKeysResponse.newBuilder()
                .setStatus(deleteStatus).setUnDeletedKeys(unDeletedKeys).addAllErrors(deleteKeyErrors))
        .setStatus(deleteStatus ? OK : PARTIAL_DELETE).setSuccess(deleteStatus)
        .build(), omKeyInfoList, dirList, ozoneManager.isRatisEnabled(),
        omBucketInfo.copyObject(), volumeId);
    return omClientResponse;

  }
}
