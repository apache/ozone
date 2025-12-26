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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_UNDER_LEASE_RECOVERY;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMAllocateBlockResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles allocate block request.
 */
public class OMAllocateBlockRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMAllocateBlockRequest.class);

  public OMAllocateBlockRequest(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    AllocateBlockRequest allocateBlockRequest =
        super.preExecute(ozoneManager).getAllocateBlockRequest();

    Objects.requireNonNull(allocateBlockRequest, "allocateBlockRequest == null");

    KeyArgs keyArgs = allocateBlockRequest.getKeyArgs();
    String keyPath = keyArgs.getKeyName();
    keyPath = validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(),
        keyPath, getBucketLayout());

    ExcludeList excludeList = new ExcludeList();
    if (allocateBlockRequest.hasExcludeList()) {
      excludeList =
          ExcludeList.getFromProtoBuf(allocateBlockRequest.getExcludeList());
    }

    // TODO: Here we are allocating block with out any check for key exist in
    //  open table or not and also with out any authorization checks.
    //  Assumption here is that allocateBlocks with out openKey will be less.
    //  There is a chance some one can misuse this api to flood allocateBlock
    //  calls. But currently allocateBlock is internally called from
    //  BlockOutputStreamEntryPool, so we are fine for now. But if one some
    //  one uses direct omclient we might be in trouble.

    UserInfo userInfo = getUserIfNotExists(ozoneManager);
    ReplicationConfig repConfig = ReplicationConfig.fromProto(keyArgs.getType(),
        keyArgs.getFactor(), keyArgs.getEcReplicationConfig());
    // To allocate atleast one block passing requested size and scmBlockSize
    // as same value. When allocating block requested size is same as
    // scmBlockSize.
    List<OmKeyLocationInfo> omKeyLocationInfoList =
        allocateBlock(ozoneManager.getScmClient(),
            ozoneManager.getBlockTokenSecretManager(), repConfig, excludeList,
            ozoneManager.getScmBlockSize(), ozoneManager.getScmBlockSize(),
            ozoneManager.getPreallocateBlocksMax(),
            ozoneManager.isGrpcBlockTokenEnabled(),
            ozoneManager.getOMServiceId(), ozoneManager.getMetrics(),
            keyArgs.getSortDatanodes(), userInfo);

    // Set modification time and normalize key if required.
    KeyArgs.Builder newKeyArgs =
        keyArgs.toBuilder().setModificationTime(Time.now()).setKeyName(keyPath);

    KeyArgs resolvedKeyArgs =
        resolveBucketAndCheckOpenKeyAcls(newKeyArgs.build(), ozoneManager,
            ACLType.WRITE, allocateBlockRequest.getClientID());

    AllocateBlockRequest.Builder newAllocatedBlockRequest =
        AllocateBlockRequest.newBuilder()
            .setClientID(allocateBlockRequest.getClientID())
            .setKeyArgs(resolvedKeyArgs);

    if (allocateBlockRequest.hasExcludeList()) {
      newAllocatedBlockRequest.setExcludeList(
          allocateBlockRequest.getExcludeList());
    }

    // Add allocated block info.
    newAllocatedBlockRequest.setKeyLocation(
        omKeyLocationInfoList.get(0).getProtobuf(getOmRequest().getVersion()));

    return getOmRequest().toBuilder().setUserInfo(userInfo)
        .setAllocateBlockRequest(newAllocatedBlockRequest).build();

  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    OzoneManagerProtocolProtos.AllocateBlockRequest allocateBlockRequest =
        getOmRequest().getAllocateBlockRequest();

    OzoneManagerProtocolProtos.KeyArgs keyArgs =
        allocateBlockRequest.getKeyArgs();

    OzoneManagerProtocolProtos.KeyLocation blockLocation =
        allocateBlockRequest.getKeyLocation();
    Objects.requireNonNull(blockLocation, "blockLocation == null");

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();
    long clientID = allocateBlockRequest.getClientID();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumBlockAllocateCalls();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
    auditMap.put(OzoneConsts.CLIENT_ID, String.valueOf(clientID));

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    String openKeyName = null;

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;

    OmKeyInfo openKeyInfo = null;
    Exception exception = null;
    OmBucketInfo omBucketInfo = null;
    boolean acquiredLock = false;

    try {
      validateBucketAndVolume(omMetadataManager, volumeName,
          bucketName);

      // Here we don't acquire bucket/volume lock because for a single client
      // allocateBlock is called in serial fashion.

      openKeyName = omMetadataManager
          .getOpenKey(volumeName, bucketName, keyName, clientID);
      openKeyInfo =
          omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKeyName);
      if (openKeyInfo == null) {
        throw new OMException("Open Key not found " + openKeyName,
            KEY_NOT_FOUND);
      }

      if (openKeyInfo.getMetadata().containsKey(OzoneConsts.LEASE_RECOVERY)) {
        throw new OMException("Open Key " + openKeyName + " is under lease recovery",
            KEY_UNDER_LEASE_RECOVERY);
      }
      if (openKeyInfo.getMetadata().containsKey(OzoneConsts.DELETED_HSYNC_KEY) ||
          openKeyInfo.getMetadata().containsKey(OzoneConsts.OVERWRITTEN_HSYNC_KEY)) {
        throw new OMException("Open Key " + openKeyName + " is already deleted/overwritten",
            KEY_NOT_FOUND);
      }
      List<OmKeyLocationInfo> newLocationList = Collections.singletonList(
          OmKeyLocationInfo.getFromProtobuf(blockLocation));

      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();
      omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);
      // check bucket and volume quota
      long preAllocatedKeySize = newLocationList.size()
          * ozoneManager.getScmBlockSize();
      long hadAllocatedKeySize =
          openKeyInfo.getLatestVersionLocations().getLocationList().size()
              * ozoneManager.getScmBlockSize();
      ReplicationConfig repConfig = openKeyInfo.getReplicationConfig();
      long totalAllocatedSpace = QuotaUtil.getReplicatedSize(
          preAllocatedKeySize, repConfig) + QuotaUtil.getReplicatedSize(
          hadAllocatedKeySize, repConfig);
      checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
          totalAllocatedSpace);
      // Append new block
      openKeyInfo.appendNewBlocks(newLocationList, false);

      // Set modification time.
      openKeyInfo.setModificationTime(keyArgs.getModificationTime());

      // Set the UpdateID to current transactionLogIndex
      openKeyInfo = openKeyInfo.toBuilder()
          .setUpdateID(trxnLogIndex)
          .build();

      // Add to cache.
      omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
          new CacheKey<>(openKeyName),
          CacheValue.get(trxnLogIndex, openKeyInfo));

      omResponse.setAllocateBlockResponse(AllocateBlockResponse.newBuilder()
          .setKeyLocation(blockLocation).build());
      omClientResponse = new OMAllocateBlockResponse(omResponse.build(),
          openKeyInfo, clientID, getBucketLayout());

      LOG.debug("Allocated block for Volume:{}, Bucket:{}, OpenKey:{}",
          volumeName, bucketName, openKeyName);
    } catch (IOException | InvalidPathException ex) {
      omMetrics.incNumBlockAllocateCallFails();
      exception = ex;
      omClientResponse = new OMAllocateBlockResponse(createErrorOMResponse(
          omResponse, exception), getBucketLayout());
      LOG.error("Allocate Block failed. Volume:{}, Bucket:{}, OpenKey:{}. " +
          "Exception:{}", volumeName, bucketName, openKeyName, exception);
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(
            omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK,
                volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    markForAudit(auditLogger, buildAuditMessage(OMAction.ALLOCATE_BLOCK, auditMap,
        exception, getOmRequest().getUserInfo()));

    return omClientResponse;
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.CLUSTER_NEEDS_FINALIZATION,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.AllocateBlock
  )
  public static OMRequest disallowAllocateBlockWithECReplicationConfig(
      OMRequest req, ValidationContext ctx) throws OMException {
    if (!ctx.versionManager()
        .isAllowed(OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT)) {
      if (req.getAllocateBlockRequest().getKeyArgs().hasEcReplicationConfig()) {
        throw new OMException("Cluster does not have the Erasure Coded"
            + " Storage support feature finalized yet, but the request contains"
            + " an Erasure Coded replication type. Rejecting the request,"
            + " please finalize the cluster upgrade and then try again.",
            OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
      }
    }
    return req;
  }

  /**
   * Validates block allocation requests.
   * We do not want to allow older clients to create block allocation requests
   * for keys that are present in buckets which use non LEGACY layouts.
   *
   * @param req - the request to validate
   * @param ctx - the validation context
   * @return the validated request
   * @throws OMException if the request is invalid
   */
  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.AllocateBlock
  )
  public static OMRequest blockAllocateBlockWithBucketLayoutFromOldClient(
      OMRequest req, ValidationContext ctx) throws IOException {
    if (req.getAllocateBlockRequest().hasKeyArgs()) {
      KeyArgs keyArgs = req.getAllocateBlockRequest().getKeyArgs();

      if (keyArgs.hasVolumeName() && keyArgs.hasBucketName()) {
        BucketLayout bucketLayout = ctx.getBucketLayout(
            keyArgs.getVolumeName(), keyArgs.getBucketName());
        bucketLayout.validateSupportedOperation();
      }
    }
    return req;
  }
}
