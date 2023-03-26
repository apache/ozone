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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_ALREADY_EXISTS;
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

      String openKeyEntryName = doWork(ozoneManager, recoverLeaseRequest,
          transactionLogIndex);

      // Prepare response
      boolean responseCode = true;
      omResponse.setRecoverLeaseResponse(RecoverLeaseResponse.newBuilder()
              .setResponse(responseCode).build())
          .setCmdType(RecoverLease);
      omClientResponse =
          new OMRecoverLeaseResponse(omResponse.build(), getBucketLayout(),
              openKeyEntryName);
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
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
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

  private String doWork(OzoneManager ozoneManager,
      RecoverLeaseRequest recoverLeaseRequest, long transactionLogIndex)
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
    String dbFileKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
        parentID, fileName);

    OmKeyInfo keyInfo = getKey(dbFileKey);
    if (keyInfo == null) {
      throw new OMException("Key:" + keyName + " not found", KEY_NOT_FOUND);
    }
    final String clientId = keyInfo.getMetadata().get(
        OzoneConsts.HSYNC_CLIENT_ID);
    if (clientId == null) {
      throw new OMException("Key:" + keyName + " is closed",
          KEY_ALREADY_EXISTS);
    }
    String openKeyEntryName = getOpenFileEntryName(volumeId, bucketId,
        parentID, fileName);
    if (openKeyEntryName != null) {
      checkFileState(keyInfo, openKeyEntryName);
      commitKey(dbFileKey, keyInfo, ozoneManager, transactionLogIndex);
      removeOpenKey(openKeyEntryName, transactionLogIndex);
    }

    return openKeyEntryName;
  }

  private OmKeyInfo getKey(String dbOzoneKey) throws IOException {
    return omMetadataManager.getKeyTable(getBucketLayout()).get(dbOzoneKey);
  }

  private String getOpenFileEntryName(long volumeId, long bucketId,
      long parentObjectId, String fileName) throws IOException {
    String openFileEntryName = null;
    String dbOpenKeyPrefix =
        omMetadataManager.getOpenFileNamePrefix(volumeId, bucketId,
            parentObjectId, fileName);
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        iter = omMetadataManager.getOpenKeyTable(getBucketLayout())
        .iterator(dbOpenKeyPrefix)) {

      while (iter.hasNext()) {
        if (openFileEntryName != null) {
          throw new IOException("Found more than two keys in the" +
            " open key table for file" + fileName);
        }
        openFileEntryName = iter.next().getKey();
      }
    }
    /*if (openFileEntryName == null) {
      throw new IOException("Unable to find the corresponding key in " +
        "the open key table for file " + fileName);
    }*/
    return openFileEntryName;
  }

  private void checkFileState(OmKeyInfo keyInfo, String openKeyEntryName)
      throws IOException {

    // if file is closed, do nothing and return right away.
    if (openKeyEntryName.isEmpty()) {
      return; // ("File is closed");
    }
    // if file is open, no sync, fail right away.
    if (keyInfo == null) {
      throw new IOException("file is open but not yet sync'ed");
    }
  }

  private void commitKey(String dbOzoneKey, OmKeyInfo omKeyInfo,
      OzoneManager ozoneManager, long transactionLogIndex) throws IOException {
    omKeyInfo.setModificationTime(Time.now());
    omKeyInfo.setUpdateID(transactionLogIndex, ozoneManager.isRatisEnabled());

    omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
      new CacheKey<>(dbOzoneKey),
      new CacheValue<>(Optional.of(omKeyInfo), transactionLogIndex));
  }

  private void removeOpenKey(String openKeyName, long transactionLogIndex)
      throws IOException {
    omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
      new CacheKey<>(openKeyName),
      new CacheValue<>(Optional.absent(), transactionLogIndex));
  }
}
