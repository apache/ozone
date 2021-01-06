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
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles CommitKey request.
 */
public class OMKeyCommitRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyCommitRequest.class);

  public OMKeyCommitRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    CommitKeyRequest commitKeyRequest = getOmRequest().getCommitKeyRequest();
    Preconditions.checkNotNull(commitKeyRequest);

    KeyArgs keyArgs = commitKeyRequest.getKeyArgs();

    // Verify key name
    final boolean checkKeyNameEnabled = ozoneManager.getConfiguration()
         .getBoolean(OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_KEY,
                 OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_DEFAULT);
    if(checkKeyNameEnabled){
      OmUtils.validateKeyName(StringUtils.removeEnd(keyArgs.getKeyName(),
              OzoneConsts.FS_FILE_COPYING_TEMP_SUFFIX));
    }

    KeyArgs.Builder newKeyArgs =
        keyArgs.toBuilder().setModificationTime(Time.now())
            .setKeyName(validateAndNormalizeKey(
                ozoneManager.getEnableFileSystemPaths(), keyArgs.getKeyName()));

    return getOmRequest().toBuilder()
        .setCommitKeyRequest(commitKeyRequest.toBuilder()
            .setKeyArgs(newKeyArgs)).setUserInfo(getUserInfo()).build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    CommitKeyRequest commitKeyRequest = getOmRequest().getCommitKeyRequest();

    KeyArgs commitKeyArgs = commitKeyRequest.getKeyArgs();

    String volumeName = commitKeyArgs.getVolumeName();
    String bucketName = commitKeyArgs.getBucketName();
    String keyName = commitKeyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyCommits();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    Map<String, String> auditMap = buildKeyArgsAuditMap(commitKeyArgs);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    IOException exception = null;
    OmKeyInfo omKeyInfo = null;
    OmBucketInfo omBucketInfo = null;
    OMClientResponse omClientResponse = null;
    boolean bucketLockAcquired = false;
    Result result;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    try {
      commitKeyArgs = resolveBucketLink(ozoneManager, commitKeyArgs, auditMap);
      volumeName = commitKeyArgs.getVolumeName();
      bucketName = commitKeyArgs.getBucketName();

      // check Acl
      checkKeyAclsInOpenKeyTable(ozoneManager, volumeName, bucketName,
          keyName, IAccessAuthorizer.ACLType.WRITE,
          commitKeyRequest.getClientID());

      String dbOzoneKey =
          omMetadataManager.getOzoneKey(volumeName, bucketName,
              keyName);
      String dbOpenKey = omMetadataManager.getOpenKey(volumeName, bucketName,
          keyName, commitKeyRequest.getClientID());

      List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
      for (KeyLocation keyLocation : commitKeyArgs.getKeyLocationsList()) {
        locationInfoList.add(OmKeyLocationInfo.getFromProtobuf(keyLocation));
      }

      bucketLockAcquired =
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
              volumeName, bucketName);

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      // Check for directory exists with same name, if it exists throw error.
      if (ozoneManager.getEnableFileSystemPaths()) {
        if (checkDirectoryAlreadyExists(volumeName, bucketName, keyName,
            omMetadataManager)) {
          throw new OMException("Can not create file: " + keyName +
              " as there is already directory in the given path", NOT_A_FILE);
        }
        // Ensure the parent exist.
        if (!"".equals(OzoneFSUtils.getParent(keyName))
            && !checkDirectoryAlreadyExists(volumeName, bucketName,
            OzoneFSUtils.getParent(keyName), omMetadataManager)) {
          throw new OMException("Cannot create file : " + keyName
              + " as parent directory doesn't exist",
              OMException.ResultCodes.DIRECTORY_NOT_FOUND);
        }
      }

      omKeyInfo = omMetadataManager.getOpenKeyTable().get(dbOpenKey);
      if (omKeyInfo == null) {
        throw new OMException("Failed to commit key, as " + dbOpenKey +
            "entry is not found in the OpenKey table", KEY_NOT_FOUND);
      }
      omKeyInfo.setDataSize(commitKeyArgs.getDataSize());

      omKeyInfo.setModificationTime(commitKeyArgs.getModificationTime());

      // Update the block length for each block
      omKeyInfo.updateLocationInfoList(locationInfoList);

      // Set the UpdateID to current transactionLogIndex
      omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      // Add to cache of open key table and key table.
      omMetadataManager.getOpenKeyTable().addCacheEntry(
          new CacheKey<>(dbOpenKey),
          new CacheValue<>(Optional.absent(), trxnLogIndex));

      omMetadataManager.getKeyTable().addCacheEntry(
          new CacheKey<>(dbOzoneKey),
          new CacheValue<>(Optional.of(omKeyInfo), trxnLogIndex));

      long scmBlockSize = ozoneManager.getScmBlockSize();
      int factor = omKeyInfo.getFactor().getNumber();
      omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);
      // Block was pre-requested and UsedBytes updated when createKey and
      // AllocatedBlock. The space occupied by the Key shall be based on
      // the actual Key size, and the total Block size applied before should
      // be subtracted.
      long correctedSpace = omKeyInfo.getDataSize() * factor -
          locationInfoList.size() * scmBlockSize * factor;
      omBucketInfo.incrUsedBytes(correctedSpace);

      omClientResponse = new OMKeyCommitResponse(omResponse.build(),
          omKeyInfo, dbOzoneKey, dbOpenKey, omBucketInfo.copyObject());

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyCommitResponse(createErrorOMResponse(
          omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);

      if(bucketLockAcquired) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    auditLog(auditLogger, buildAuditMessage(OMAction.COMMIT_KEY, auditMap,
          exception, getOmRequest().getUserInfo()));

    switch (result) {
    case SUCCESS:
      // As when we commit the key, then it is visible in ozone, so we should
      // increment here.
      // As key also can have multiple versions, we need to increment keys
      // only if version is 0. Currently we have not complete support of
      // versioning of keys. So, this can be revisited later.
      if (omKeyInfo.getKeyLocationVersions().size() == 1) {
        omMetrics.incNumKeys();
      }
      LOG.debug("Key committed. Volume:{}, Bucket:{}, Key:{}", volumeName,
          bucketName, keyName);
      break;
    case FAILURE:
      LOG.error("Key commit failed. Volume:{}, Bucket:{}, Key:{}.",
          volumeName, bucketName, keyName, exception);
      omMetrics.incNumKeyCommitFails();
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyCommitRequest: {}",
          commitKeyRequest);
    }

    return omClientResponse;
  }
}
