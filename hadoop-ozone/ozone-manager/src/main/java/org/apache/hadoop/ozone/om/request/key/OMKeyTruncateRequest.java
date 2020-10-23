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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.key.OMKeyTruncateResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TruncateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TruncateKeyResponse;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_TRUNCATE_NEW_LENGTH;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles TruncateKey request.
 */
public class OMKeyTruncateRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyTruncateRequest.class);

  public OMKeyTruncateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    TruncateKeyRequest truncateKeyRequest =
        getOmRequest().getTruncateKeyRequest();
    Preconditions.checkNotNull(truncateKeyRequest);

    OzoneManagerProtocolProtos.KeyArgs keyArgs =
        truncateKeyRequest.getKeyArgs();

    OzoneManagerProtocolProtos.KeyArgs.Builder newKeyArgs =
        keyArgs.toBuilder().setModificationTime(Time.now())
            .setKeyName(validateAndNormalizeKey(
                ozoneManager.getEnableFileSystemPaths(), keyArgs.getKeyName()));

    return getOmRequest().toBuilder()
        .setTruncateKeyRequest(truncateKeyRequest.toBuilder()
            .setKeyArgs(newKeyArgs)).setUserInfo(getUserInfo()).build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    TruncateKeyRequest truncateKeyRequest =
        getOmRequest().getTruncateKeyRequest();
    OzoneManagerProtocolProtos.KeyArgs keyArgs =
        truncateKeyRequest.getKeyArgs();
    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();
    long newLength = keyArgs.getNewLength();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyTruncates();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    IOException exception = null;
    Result result = null;
    OmVolumeArgs omVolumeArgs = null;
    OmBucketInfo omBucketInfo = null;
    try {
      keyArgs = resolveBucketLink(ozoneManager, keyArgs, auditMap);
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();

      // check Acls to see if user has access to perform truncate operation on
      // the key
      checkKeyAcls(ozoneManager, volumeName, bucketName, keyName,
          IAccessAuthorizer.ACLType.WRITE, OzoneObj.ResourceType.KEY);

      acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
          volumeName, bucketName);

      // Validate bucket and volume exists or not.
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      // Check if the key exists
      String objectKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
          keyName);

      // keyName should exist
      OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(objectKey);
      if (omKeyInfo == null) {
        throw new OMException("Key not found " + keyName, KEY_NOT_FOUND);
      }

      // newLength should be less than the existed length
      if (newLength >= omKeyInfo.getDataSize()) {
        throw new OMException("Invalid truncate new length " + keyName,
            INVALID_TRUNCATE_NEW_LENGTH);
      }

      omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());
      List<OmKeyLocationInfo> locationInfos =
          omKeyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();

      omKeyInfo.addNewVersion(new ArrayList<>(), true);

      List<OmKeyLocationInfo> newLocationInfos = new ArrayList<>();
      long len = 0;
      for (OmKeyLocationInfo locationInfo : locationInfos) {
        long blockLen = locationInfo.getLength();
        if (len + blockLen > newLength) {
          blockLen = newLength - len;
        }

        newLocationInfos.add(new OmKeyLocationInfo.Builder()
              .setPipeline(locationInfo.getPipeline())
              .setBlockID(locationInfo.getBlockID())
              .setLength(blockLen)
              .setOffset(locationInfo.getOffset())
              .setToken(locationInfo.getToken())
              .build());

        len += locationInfo.getLength();
        if (len >= newLength) {
          break;
        }
      }

      long quotaReleased = omKeyInfo.getDataSize() - newLength;

      omKeyInfo.appendNewBlocks(newLocationInfos, true);
      omKeyInfo.setDataSize(newLength);

      // Add to cache and override the existed entry
      Table<String, OmKeyInfo> keyTable = omMetadataManager.getKeyTable();

      keyTable.addCacheEntry(new CacheKey<>(objectKey),
          new CacheValue<>(Optional.of(omKeyInfo), trxnLogIndex));

      omVolumeArgs = getVolumeInfo(omMetadataManager, volumeName);
      omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);

      // update usedBytes atomically.
      omVolumeArgs.getUsedBytes().add(-quotaReleased);
      omBucketInfo.getUsedBytes().add(-quotaReleased);

      omClientResponse = new OMKeyTruncateResponse(omResponse
          .setTruncateKeyResponse(TruncateKeyResponse.newBuilder()).build(),
          omKeyInfo, omVolumeArgs, omBucketInfo);

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyTruncateResponse(createErrorOMResponse(
          omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
            omDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    auditLog(auditLogger, buildAuditMessage(OMAction.TRUNCATE_KEY, auditMap,
        exception, getOmRequest().getUserInfo()));

    switch (result) {
    case SUCCESS:
      LOG.debug("Truncate Key is successfully completed for" +
          "volume:{} bucket:{} key:{}. ",
          volumeName, bucketName, keyName);
      break;
    case FAILURE:
      ozoneManager.getMetrics().incNumKeyTruncateFails();
      LOG.error("Truncate key failed for volume:{} bucket:{} key: {}.",
          volumeName, bucketName, keyName);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyRenameRequest: {}",
          truncateKeyRequest);
    }
    return omClientResponse;
  }
}
