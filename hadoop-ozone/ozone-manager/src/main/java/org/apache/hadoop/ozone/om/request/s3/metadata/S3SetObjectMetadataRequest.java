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

package org.apache.hadoop.ozone.om.request.s3.metadata;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.SET_OBJECT_METADATA;

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequestUtils;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.metadata.S3SetObjectMetadataResponse;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetObjectMetadataRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetObjectMetadataResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles set object metadata request: replaces the custom metadata and tags
 * of an existing key without touching its data. Used by the S3 gateway for a
 * CopyObject onto itself with metadata directive REPLACE.
 */
public class S3SetObjectMetadataRequest extends OMKeyRequest {

  /**
   * System-managed metadata entries that a SetObjectMetadata request may never
   * change: they are always carried over from the existing key. The ETag only
   * reflects the object content, and dropping the GDPR entries would make a
   * GDPR key unreadable.
   */
  public static final Set<String> RESERVED_METADATA_KEYS = ImmutableSet.of(
      OzoneConsts.ETAG,
      OzoneConsts.GDPR_FLAG,
      OzoneConsts.GDPR_SECRET,
      OzoneConsts.GDPR_ALGORITHM,
      OzoneConsts.HSYNC_CLIENT_ID);

  private static final Logger LOG =
      LoggerFactory.getLogger(S3SetObjectMetadataRequest.class);

  public S3SetObjectMetadataRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  /**
   * Builds the replacement metadata map from the request metadata, with the
   * reserved system entries taken from the existing key instead of the request.
   */
  public static Map<String, String> replaceMetadata(Map<String, String> existingMetadata,
      Map<String, String> requestMetadata) {
    Map<String, String> newMetadata = new HashMap<>();
    for (Map.Entry<String, String> entry : requestMetadata.entrySet()) {
      if (!RESERVED_METADATA_KEYS.contains(entry.getKey())) {
        newMetadata.put(entry.getKey(), entry.getValue());
      }
    }
    for (String reservedKey : RESERVED_METADATA_KEYS) {
      String reservedValue = existingMetadata.get(reservedKey);
      if (reservedValue != null) {
        newMetadata.put(reservedKey, reservedValue);
      }
    }
    return newMetadata;
  }

  @Override
  @DisallowedUntilLayoutVersion(SET_OBJECT_METADATA)
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    SetObjectMetadataRequest setObjectMetadataRequest =
        super.preExecute(ozoneManager).getSetObjectMetadataRequest();
    Objects.requireNonNull(setObjectMetadataRequest, "setObjectMetadataRequest == null");

    KeyArgs keyArgs = setObjectMetadataRequest.getKeyArgs();

    String keyPath = keyArgs.getKeyName();
    keyPath = validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(),
        keyPath, getBucketLayout());

    KeyArgs.Builder newKeyArgs =
        keyArgs.toBuilder()
            .setKeyName(keyPath)
            .setModificationTime(Time.now());

    KeyArgs resolvedArgs = resolveBucketAndCheckKeyAcls(newKeyArgs.build(),
        ozoneManager, ACLType.WRITE);
    return getOmRequest().toBuilder()
        .setUserInfo(getUserInfo())
        .setSetObjectMetadataRequest(
            setObjectMetadataRequest.toBuilder().setKeyArgs(resolvedArgs))
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    SetObjectMetadataRequest setObjectMetadataRequest = getOmRequest().getSetObjectMetadataRequest();

    KeyArgs keyArgs = setObjectMetadataRequest.getKeyArgs();
    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumSetObjectMetadata();

    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    IOException exception = null;
    Result result = null;
    try {
      mergeOmLockDetails(
          omMetadataManager.getLock()
              .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName)
      );
      acquiredLock = getOmLockDetails().isLockAcquired();

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      String dbOzoneKey =
          omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);

      OmKeyInfo omKeyInfo =
          omMetadataManager.getKeyTable(getBucketLayout()).get(dbOzoneKey);
      if (omKeyInfo == null) {
        throw new OMException("Key not found", KEY_NOT_FOUND);
      }

      omKeyInfo = omKeyInfo.toBuilder()
          .setMetadata(replaceMetadata(omKeyInfo.getMetadata(),
              KeyValueUtil.getFromProtobuf(keyArgs.getMetadataList())))
          .setTags(KeyValueUtil.getFromProtobuf(keyArgs.getTagsList()))
          .setModificationTime(keyArgs.getModificationTime())
          .setUpdateID(trxnLogIndex)
          .build();

      // Note: unlike object tagging, the key modification time is updated
      // because AWS CopyObject updates the object's LastModified.

      // Update table cache
      omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
          new CacheKey<>(dbOzoneKey),
          CacheValue.get(trxnLogIndex, omKeyInfo)
      );

      omClientResponse = new S3SetObjectMetadataResponse(
          omResponse.setSetObjectMetadataResponse(SetObjectMetadataResponse.newBuilder()).build(),
          omKeyInfo
      );

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new S3SetObjectMetadataResponse(
          createErrorOMResponse(omResponse, exception),
          getBucketLayout()
      );
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
        OMAction.SET_OBJECT_METADATA, auditMap, exception, getOmRequest().getUserInfo()
    ));

    switch (result) {
    case SUCCESS:
      LOG.debug("Set object metadata success. Volume:{}, Bucket:{}, Key:{}.", volumeName,
          bucketName, keyName);
      break;
    case FAILURE:
      omMetrics.incNumSetObjectMetadataFails();
      if (OMClientRequestUtils.shouldLogClientRequestFailure(exception)) {
        LOG.error("Set object metadata failed. Volume:{}, Bucket:{}, Key:{}.", volumeName,
            bucketName, keyName, exception);
      }
      break;
    default:
      LOG.error("Unrecognized Result for S3SetObjectMetadataRequest: {}",
          setObjectMetadataRequest);
    }

    return omClientResponse;
  }
}
