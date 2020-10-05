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

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.*;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponseV1;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.*;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles CommitKey request.
 */
public class OMKeyCommitRequestV1 extends OMKeyCommitRequest {

  private static final Logger LOG =
          LoggerFactory.getLogger(OMKeyCommitRequestV1.class);

  public OMKeyCommitRequestV1(OMRequest omRequest) {
    super(omRequest);
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
    OmVolumeArgs omVolumeArgs = null;
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


      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
      Iterator<Path> pathComponents = Paths.get(keyName).iterator();
      String dbOpenFileKey = null;

      List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
      for (KeyLocation keyLocation : commitKeyArgs.getKeyLocationsList()) {
        locationInfoList.add(OmKeyLocationInfo.getFromProtobuf(keyLocation));
      }

      bucketLockAcquired =
              omMetadataManager.getLock().acquireLock(BUCKET_LOCK,
                      volumeName, bucketName);

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      String fileName = OzoneFSUtils.getFileName(keyName);
      omBucketInfo = omMetadataManager.getBucketTable().get(bucketKey);
      long bucketId = omBucketInfo.getObjectID();
      long parentID = getParentID(bucketId, pathComponents, keyName,
              omMetadataManager);
      String dbFileKey = omMetadataManager.getOzonePathKey(parentID, fileName);
      dbOpenFileKey = omMetadataManager.getOpenFileName(parentID, fileName,
              commitKeyRequest.getClientID());

      omKeyInfo = omMetadataManager.getOpenKeyTable().get(dbOpenFileKey);
      if (omKeyInfo == null) {
        throw new OMException("Failed to commit key, as " + dbOpenFileKey +
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
              new CacheKey<>(dbFileKey),
              new CacheValue<>(Optional.absent(), trxnLogIndex));

      omMetadataManager.getKeyTable().addCacheEntry(
              new CacheKey<>(dbFileKey),
              new CacheValue<>(Optional.of(omKeyInfo), trxnLogIndex));

      long scmBlockSize = ozoneManager.getScmBlockSize();
      int factor = omKeyInfo.getFactor().getNumber();
      omVolumeArgs = getVolumeInfo(omMetadataManager, volumeName);
      // update usedBytes atomically.
      // Block was pre-requested and UsedBytes updated when createKey and
      // AllocatedBlock. The space occupied by the Key shall be based on
      // the actual Key size, and the total Block size applied before should
      // be subtracted.
      long correctedSpace = omKeyInfo.getDataSize() * factor -
              locationInfoList.size() * scmBlockSize * factor;
      omVolumeArgs.getUsedBytes().add(correctedSpace);
      omBucketInfo.getUsedBytes().add(correctedSpace);

      omClientResponse = new OMKeyCommitResponse(omResponse.build(),
              omKeyInfo, dbFileKey, dbOpenFileKey, omVolumeArgs, omBucketInfo);

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyCommitResponseV1(createErrorOMResponse(
              omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
              omDoubleBufferHelper);

      if(bucketLockAcquired) {
        omMetadataManager.getLock().releaseLock(BUCKET_LOCK, volumeName,
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
      LOG.error("Key commit failed. Volume:{}, Bucket:{}, Key:{}. Exception:{}",
              volumeName, bucketName, keyName, exception);
      omMetrics.incNumKeyCommitFails();
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyCommitRequest: {}",
              commitKeyRequest);
    }

    return omClientResponse;
  }

  /**
   * Get parent id for the user given path.
   *
   * @param bucketId          bucket id
   * @param pathComponents    fie path elements
   * @param keyName           user given key name
   * @param omMetadataManager metadata manager
   * @return lastKnownParentID
   * @throws IOException DB failure or parent not exists in DirectoryTable
   */
  private long getParentID(long bucketId, Iterator<Path> pathComponents,
                                        String keyName,
                                        OMMetadataManager omMetadataManager)
          throws IOException {

    long lastKnownParentId = bucketId;
    boolean parentFound = true; // default bucketID as parent
    OmDirectoryInfo omDirectoryInfo = null;
    while (pathComponents.hasNext()) {
      String nodeName = pathComponents.next().toString();
      // Reached last component, which would be a file. Returns its parentID.
      if (!pathComponents.hasNext()) {
        return lastKnownParentId;
      }
      String dbNodeName = lastKnownParentId + "/" + nodeName;
      omDirectoryInfo = omMetadataManager.
              getDirectoryTable().get(dbNodeName);
      if (omDirectoryInfo != null) {
        lastKnownParentId = omDirectoryInfo.getObjectID();
      } else {
        parentFound = false;
        break;
      }
    }

    if (!parentFound) {
      throw new OMException("Failed to commit key, as parent directory of "
              + keyName + " entry is not found in DirectoryTable",
              KEY_NOT_FOUND);
    }
    return lastKnownParentId;
  }
}
