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

package org.apache.hadoop.ozone.om.request.file;

import com.google.common.base.Preconditions;

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.file.OMRecoverLeaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .RecoverLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .RecoverLeaseResponse;

import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .Type.RecoverLease;

/**
 * Perform actions for RecoverLease requests.
 */
public class OMRecoverLeaseRequest extends OMKeyRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMRecoverLeaseRequest.class);

  private String volumeName;
  private String bucketName;
  private String keyName;
  private OmKeyInfo keyInfo;
  private String dbFileKey;

  private OMMetadataManager omMetadataManager;

  public OMRecoverLeaseRequest(OMRequest omRequest) {
    super(omRequest, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    RecoverLeaseRequest recoverLeaseRequest = getOmRequest()
        .getRecoverLeaseRequest();

    Preconditions.checkNotNull(recoverLeaseRequest);
    volumeName = recoverLeaseRequest.getVolumeName();
    bucketName = recoverLeaseRequest.getBucketName();
    keyName = recoverLeaseRequest.getKeyName();
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    RecoverLeaseRequest recoverLeaseRequest = getOmRequest()
        .getRecoverLeaseRequest();

    String keyPath = recoverLeaseRequest.getKeyName();
    String normalizedKeyPath =
        validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(),
            keyPath, getBucketLayout());

    return getOmRequest().toBuilder()
        .setRecoverLeaseRequest(
            recoverLeaseRequest.toBuilder()
                .setKeyName(normalizedKeyPath))
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    RecoverLeaseRequest recoverLeaseRequest = getOmRequest()
        .getRecoverLeaseRequest();
    Preconditions.checkNotNull(recoverLeaseRequest);

    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, volumeName);
    auditMap.put(OzoneConsts.BUCKET, bucketName);
    auditMap.put(OzoneConsts.KEY, keyName);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    omMetadataManager = ozoneManager.getMetadataManager();
    OMClientResponse omClientResponse = null;
    IOException exception = null;
    // increment metric
    OMMetrics omMetrics = ozoneManager.getMetrics();

    boolean acquiredLock = false;
    try {
      // check ACL
      checkKeyAcls(ozoneManager, volumeName, bucketName, keyName,
          IAccessAuthorizer.ACLType.WRITE, OzoneObj.ResourceType.KEY);

      // acquire lock
      acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
          volumeName, bucketName);

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      String openKeyEntryName = doWork(ozoneManager, transactionLogIndex);

      // Prepare response
      boolean responseCode = true;
      omResponse
          .setRecoverLeaseResponse(
              RecoverLeaseResponse.newBuilder()
                  .setResponse(responseCode)
                  .build())
          .setCmdType(RecoverLease);
      omClientResponse =
          new OMRecoverLeaseResponse(omResponse.build(), getBucketLayout(),
              keyInfo, dbFileKey, openKeyEntryName);
      omMetrics.incNumRecoverLease();
      LOG.debug("Key recovered. Volume:{}, Bucket:{}, Key:{}", volumeName,
          bucketName, keyName);
    } catch (IOException ex) {
      LOG.error("Fail for recovering lease. Volume:{}, Bucket:{}, Key:{}",
          volumeName, bucketName, keyName, ex);
      exception = ex;
      omMetrics.incNumRecoverLeaseFails();
      omResponse.setCmdType(RecoverLease);
      omClientResponse = new OMRecoverLeaseResponse(
          createErrorOMResponse(omResponse, ex), getBucketLayout());
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    // Audit Log outside the lock
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.RECOVER_LEASE, auditMap, exception,
        getOmRequest().getUserInfo()));

    return omClientResponse;
  }

  private String doWork(OzoneManager ozoneManager, long transactionLogIndex)
      throws IOException {

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(
        volumeName, bucketName);
    Iterator<Path> pathComponents = Paths.get(keyName).iterator();
    long parentID = OMFileRequest.getParentID(volumeId, bucketId,
        pathComponents, keyName, omMetadataManager,
        "Cannot recover file : " + keyName
            + " as parent directory doesn't exist");
    String fileName = OzoneFSUtils.getFileName(keyName);
    dbFileKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
        parentID, fileName);

    keyInfo = getKey(dbFileKey);
    if (keyInfo == null) {
      throw new OMException("Key:" + keyName + " not found", KEY_NOT_FOUND);
    }
    final String clientId = keyInfo.getMetadata().remove(
        OzoneConsts.HSYNC_CLIENT_ID);
    if (clientId == null) {
      // if file is closed, do nothing and return right away.
      LOG.warn("Key:" + keyName + " is already closed");
      return null;
    }
    String openFileDBKey = omMetadataManager.getOpenFileName(
            volumeId, bucketId, parentID, fileName, Long.parseLong(clientId));
    if (openFileDBKey != null) {
      commitKey(dbFileKey, keyInfo, fileName, ozoneManager,
          transactionLogIndex);
      removeOpenKey(openFileDBKey, fileName, transactionLogIndex);
    }

    return openFileDBKey;
  }

  private OmKeyInfo getKey(String dbOzoneKey) throws IOException {
    return omMetadataManager.getKeyTable(getBucketLayout()).get(dbOzoneKey);
  }

  private void commitKey(String dbOzoneKey, OmKeyInfo omKeyInfo,
      String fileName, OzoneManager ozoneManager,
      long transactionLogIndex) throws IOException {
    omKeyInfo.setModificationTime(Time.now());
    omKeyInfo.setUpdateID(transactionLogIndex, ozoneManager.isRatisEnabled());

    OMFileRequest.addFileTableCacheEntry(omMetadataManager, dbOzoneKey,
        omKeyInfo, fileName, transactionLogIndex);
  }

  private void removeOpenKey(String openKeyName, String fileName,
      long transactionLogIndex) {
    OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager,
        openKeyName, null, fileName, transactionLogIndex);
  }
}
