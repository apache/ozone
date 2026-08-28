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

package org.apache.hadoop.ozone.om.request.key;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.request.OMClientRequestUtils;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCopyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CopyKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CopyKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles a server side key copy: the destination key is created as an
 * independent key that reuses the source key's committed block locations, so no
 * data is read or written. Both keys are tagged with a shared block group id
 * and the group's sharer count is tracked in the sharedBlockGroupTable, which
 * {@link org.apache.hadoop.ozone.om.service.KeyDeletingService} consults so the
 * blocks are only released once the last sharer is reclaimed.
 *
 * <p>This is the proof-of-concept scope. A copy is rejected, and the caller is
 * expected to fall back to reading and rewriting the data, when it would need
 * to cross a bucket, overwrite an existing key, or touch encrypted, GDPR or
 * hsync-active keys.
 */
public class OMKeyCopyRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyCopyRequest.class);

  public OMKeyCopyRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    CopyKeyRequest copyKeyRequest =
        super.preExecute(ozoneManager).getCopyKeyRequest();
    Objects.requireNonNull(copyKeyRequest, "copyKeyRequest == null");

    KeyArgs sourceKeyArgs = copyKeyRequest.getSourceKeyArgs();
    KeyArgs destinationKeyArgs = copyKeyRequest.getDestinationKeyArgs();

    if (!sourceKeyArgs.getVolumeName().equals(destinationKeyArgs.getVolumeName())
        || !sourceKeyArgs.getBucketName().equals(destinationKeyArgs.getBucketName())) {
      throw new OMException("Server side copy across buckets is not supported yet",
          NOT_SUPPORTED_OPERATION);
    }
    if (sourceKeyArgs.getKeyName().equals(destinationKeyArgs.getKeyName())) {
      throw new OMException("Server side copy onto the source key is not supported",
          NOT_SUPPORTED_OPERATION);
    }

    KeyArgs.Builder normalizedSource = sourceKeyArgs.toBuilder()
        .setKeyName(validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(),
            sourceKeyArgs.getKeyName(), getBucketLayout()));
    KeyArgs.Builder normalizedDestination = destinationKeyArgs.toBuilder()
        .setKeyName(validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(),
            destinationKeyArgs.getKeyName(), getBucketLayout()))
        .setModificationTime(Time.now());

    KeyArgs resolvedSource = resolveBucketAndCheckKeyAcls(normalizedSource.build(),
        ozoneManager, ACLType.READ);
    KeyArgs resolvedDestination = resolveBucketAndCheckKeyAcls(normalizedDestination.build(),
        ozoneManager, ACLType.CREATE);

    return getOmRequest().toBuilder()
        .setUserInfo(getUserInfo())
        .setCopyKeyRequest(copyKeyRequest.toBuilder()
            .setSourceKeyArgs(resolvedSource)
            .setDestinationKeyArgs(resolvedDestination))
        .build();
  }

  @Override
  @SuppressWarnings("checkstyle:methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    CopyKeyRequest copyKeyRequest = getOmRequest().getCopyKeyRequest();
    KeyArgs sourceKeyArgs = copyKeyRequest.getSourceKeyArgs();
    KeyArgs destinationKeyArgs = copyKeyRequest.getDestinationKeyArgs();

    String volumeName = destinationKeyArgs.getVolumeName();
    String bucketName = destinationKeyArgs.getBucketName();
    String sourceKeyName = sourceKeyArgs.getKeyName();
    String destinationKeyName = destinationKeyArgs.getKeyName();

    Map<String, String> auditMap = buildKeyArgsAuditMap(destinationKeyArgs);
    auditMap.put(OzoneConsts.SRC_KEY, sourceKeyName);
    auditMap.put(OzoneConsts.DST_KEY, destinationKeyName);

    OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    IOException exception = null;
    Result result = null;
    try {
      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      String sourceDbKey =
          omMetadataManager.getOzoneKey(volumeName, bucketName, sourceKeyName);
      String destinationDbKey =
          omMetadataManager.getOzoneKey(volumeName, bucketName, destinationKeyName);

      OmKeyInfo sourceKeyInfo =
          omMetadataManager.getKeyTable(getBucketLayout()).get(sourceDbKey);
      if (sourceKeyInfo == null) {
        throw new OMException("Source key not found: " + sourceKeyName, KEY_NOT_FOUND);
      }
      if (omMetadataManager.getKeyTable(getBucketLayout()).isExist(destinationDbKey)) {
        throw new OMException("Destination key already exists: " + destinationKeyName,
            KEY_ALREADY_EXISTS);
      }
      rejectIneligibleSource(sourceKeyInfo, sourceKeyName);

      // The lineage root's objectID names the group, so copies of copies join
      // the group the source already belongs to instead of starting a new one.
      final long sharedBlockGroupId = sourceKeyInfo.hasSharedBlocks()
          ? sourceKeyInfo.getSharedBlockGroupId() : sourceKeyInfo.getObjectID();

      Map<String, String> destinationMetadata = new HashMap<>(sourceKeyInfo.getMetadata());
      destinationMetadata.putAll(
          KeyValueUtil.getFromProtobuf(destinationKeyArgs.getMetadataList()));
      // The ETag describes the content, which a copy reproduces exactly, so it
      // survives whatever the caller asked to change about the metadata.
      String sourceETag = sourceKeyInfo.getMetadata().get(OzoneConsts.ETAG);
      if (sourceETag != null) {
        destinationMetadata.put(OzoneConsts.ETAG, sourceETag);
      }

      // Built from a fresh builder rather than the source's: an objectID is
      // immutable once assigned, and the copy is a new object that merely
      // points at the same blocks.
      OmKeyInfo destinationKeyInfo = new OmKeyInfo.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName(destinationKeyName)
          .setOmKeyLocationInfos(copyLocationVersions(sourceKeyInfo))
          .setDataSize(sourceKeyInfo.getDataSize())
          .setReplicationConfig(sourceKeyInfo.getReplicationConfig())
          .setFileEncryptionInfo(sourceKeyInfo.getFileEncryptionInfo())
          .setAcls(sourceKeyInfo.getAcls())
          .setOwnerName(sourceKeyInfo.getOwnerName())
          .setFile(sourceKeyInfo.isFile())
          .setTags(sourceKeyInfo.getTags())
          .addAllMetadata(destinationMetadata)
          .setCreationTime(destinationKeyArgs.getModificationTime())
          .setModificationTime(destinationKeyArgs.getModificationTime())
          .setObjectID(ozoneManager.getObjectIdFromTxId(trxnLogIndex))
          .setUpdateID(trxnLogIndex)
          .setSharedBlockGroupId(sharedBlockGroupId)
          .build();

      // Tag the source on its first copy so that its own deletion consults the
      // sharer count as well.
      OmKeyInfo updatedSourceKeyInfo = null;
      if (!sourceKeyInfo.hasSharedBlocks()) {
        updatedSourceKeyInfo = sourceKeyInfo.toBuilder()
            .setSharedBlockGroupId(sharedBlockGroupId)
            .setUpdateID(trxnLogIndex)
            .build();
      }

      // An absent row means the source is still the exclusive owner, so the
      // first copy takes the count straight to two.
      Long currentSharerCount =
          omMetadataManager.getSharedBlockGroupTable().get(sharedBlockGroupId);
      long newSharerCount =
          currentSharerCount == null ? 2L : currentSharerCount + 1L;

      OmBucketInfo omBucketInfo =
          getBucketInfo(omMetadataManager, volumeName, bucketName);
      // The copy is charged its full logical size: the physical sharing is
      // invisible to quota, which stays consistent because deleting either key
      // gives the same amount back.
      checkBucketQuotaInNamespace(omBucketInfo, 1L);
      checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
          destinationKeyInfo.getReplicatedSize());
      omBucketInfo.incrUsedNamespace(1L);
      omBucketInfo.incrUsedBytes(destinationKeyInfo.getReplicatedSize());

      omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
          new CacheKey<>(destinationDbKey),
          CacheValue.get(trxnLogIndex, destinationKeyInfo));
      if (updatedSourceKeyInfo != null) {
        omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
            new CacheKey<>(sourceDbKey),
            CacheValue.get(trxnLogIndex, updatedSourceKeyInfo));
      }
      omMetadataManager.getSharedBlockGroupTable().addCacheEntry(
          new CacheKey<>(sharedBlockGroupId),
          CacheValue.get(trxnLogIndex, newSharerCount));

      omClientResponse = new OMKeyCopyResponse(
          omResponse.setCopyKeyResponse(CopyKeyResponse.newBuilder()
              .setKeyInfo(destinationKeyInfo.getProtobuf(getOmRequest().getVersion()))).build(),
          destinationKeyInfo, destinationDbKey, updatedSourceKeyInfo, sourceDbKey,
          sharedBlockGroupId, newSharerCount, omBucketInfo.copyObject(), getBucketLayout());

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyCopyResponse(
          createErrorOMResponse(omResponse, exception), getBucketLayout());
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.COPY_KEY, auditMap, exception, getOmRequest().getUserInfo()));

    switch (result) {
    case SUCCESS:
      LOG.debug("Copy key success. Volume:{}, Bucket:{}, Source:{}, Destination:{}.",
          volumeName, bucketName, sourceKeyName, destinationKeyName);
      break;
    case FAILURE:
      if (OMClientRequestUtils.shouldLogClientRequestFailure(exception)) {
        LOG.error("Copy key failed. Volume:{}, Bucket:{}, Source:{}, Destination:{}.",
            volumeName, bucketName, sourceKeyName, destinationKeyName, exception);
      }
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyCopyRequest: {}", copyKeyRequest);
    }

    return omClientResponse;
  }

  /**
   * Copies the source's committed block locations for the destination key. The
   * groups are rebuilt rather than shared so the two keys do not alias one
   * another's in-memory location lists; the blocks they name are the same.
   */
  private static List<OmKeyLocationInfoGroup> copyLocationVersions(OmKeyInfo sourceKeyInfo) {
    return sourceKeyInfo.getKeyLocationVersions().stream()
        .map(version -> new OmKeyLocationInfoGroup(version.getVersion(),
            version.createLocationList(), version.isMultipartKey()))
        .collect(Collectors.toList());
  }

  /**
   * Rejects sources whose blocks cannot be shared as they are. The caller is
   * expected to fall back to a read and rewrite copy for these.
   */
  private void rejectIneligibleSource(OmKeyInfo sourceKeyInfo, String sourceKeyName)
      throws OMException {
    // Block data is ciphertext under the source key's own DEK and IV, so a
    // shared copy would have to carry the source's FileEncryptionInfo verbatim.
    if (sourceKeyInfo.getFileEncryptionInfo() != null) {
      throw new OMException("Server side copy of an encrypted key is not supported: "
          + sourceKeyName, NOT_SUPPORTED_OPERATION);
    }
    // Erasure relies on destroying the key's own secret, which duplicating it
    // into a second key sharing the same blocks would defeat.
    if (sourceKeyInfo.getMetadata().containsKey(OzoneConsts.GDPR_FLAG)) {
      throw new OMException("Server side copy of a GDPR enforced key is not supported: "
          + sourceKeyName, NOT_SUPPORTED_OPERATION);
    }
    // An hsync'ed key still has a writer, so its block list can still change.
    if (sourceKeyInfo.getMetadata().containsKey(OzoneConsts.HSYNC_CLIENT_ID)) {
      throw new OMException("Server side copy of a key being written with hsync is not "
          + "supported: " + sourceKeyName, NOT_SUPPORTED_OPERATION);
    }
  }
}
