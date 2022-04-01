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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMAllocateBlockResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

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
        getOmRequest().getAllocateBlockRequest();

    Preconditions.checkNotNull(allocateBlockRequest);

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
            ozoneManager.isGrpcBlockTokenEnabled(), ozoneManager.getOMNodeId());

    // Set modification time and normalize key if required.
    KeyArgs.Builder newKeyArgs =
        keyArgs.toBuilder().setModificationTime(Time.now()).setKeyName(keyPath);

    AllocateBlockRequest.Builder newAllocatedBlockRequest =
        AllocateBlockRequest.newBuilder()
            .setClientID(allocateBlockRequest.getClientID())
            .setKeyArgs(newKeyArgs);

    if (allocateBlockRequest.hasExcludeList()) {
      newAllocatedBlockRequest.setExcludeList(
          allocateBlockRequest.getExcludeList());
    }

    // Add allocated block info.
    newAllocatedBlockRequest.setKeyLocation(
        omKeyLocationInfoList.get(0).getProtobuf(getOmRequest().getVersion()));

    return getOmRequest().toBuilder().setUserInfo(getUserInfo())
        .setAllocateBlockRequest(newAllocatedBlockRequest).build();

  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    OzoneManagerProtocolProtos.AllocateBlockRequest allocateBlockRequest =
        getOmRequest().getAllocateBlockRequest();

    OzoneManagerProtocolProtos.KeyArgs keyArgs =
        allocateBlockRequest.getKeyArgs();

    OzoneManagerProtocolProtos.KeyLocation blockLocation =
        allocateBlockRequest.getKeyLocation();
    Preconditions.checkNotNull(blockLocation);

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
    IOException exception = null;
    OmBucketInfo omBucketInfo = null;
    boolean acquiredLock = false;

    try {
      keyArgs = resolveBucketLink(ozoneManager, keyArgs, auditMap);
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();

      // check Acl
      checkKeyAclsInOpenKeyTable(ozoneManager, volumeName, bucketName, keyName,
          IAccessAuthorizer.ACLType.WRITE, allocateBlockRequest.getClientID());

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

      List<OmKeyLocationInfo> newLocationList = Collections.singletonList(
          OmKeyLocationInfo.getFromProtobuf(blockLocation));

      acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
          volumeName, bucketName);
      omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);
      // check bucket and volume quota
      long preAllocatedSpace = newLocationList.size()
          * ozoneManager.getScmBlockSize()
          * openKeyInfo.getReplicationConfig().getRequiredNodes();
      checkBucketQuotaInBytes(omBucketInfo, preAllocatedSpace);
      // Append new block
      openKeyInfo.appendNewBlocks(newLocationList, false);

      // Set modification time.
      openKeyInfo.setModificationTime(keyArgs.getModificationTime());

      // Set the UpdateID to current transactionLogIndex
      openKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      // Add to cache.
      omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
          new CacheKey<>(openKeyName),
          new CacheValue<>(Optional.of(openKeyInfo), trxnLogIndex));

      omBucketInfo.incrUsedBytes(preAllocatedSpace);
      omResponse.setAllocateBlockResponse(AllocateBlockResponse.newBuilder()
          .setKeyLocation(blockLocation).build());
      OmBucketInfo shortBucketInfo = omBucketInfo.copyObject();
      omClientResponse = new OMAllocateBlockResponse(omResponse.build(),
          openKeyInfo, clientID, shortBucketInfo, getBucketLayout());

      LOG.debug("Allocated block for Volume:{}, Bucket:{}, OpenKey:{}",
          volumeName, bucketName, openKeyName);
    } catch (IOException ex) {
      omMetrics.incNumBlockAllocateCallFails();
      exception = ex;
      omClientResponse = new OMAllocateBlockResponse(createErrorOMResponse(
          omResponse, exception), getBucketLayout());
      LOG.error("Allocate Block failed. Volume:{}, Bucket:{}, OpenKey:{}. " +
            "Exception:{}", volumeName, bucketName, openKeyName, exception);
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    auditLog(auditLogger, buildAuditMessage(OMAction.ALLOCATE_BLOCK, auditMap,
        exception, getOmRequest().getUserInfo()));

    return omClientResponse;
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.CLUSTER_NEEDS_FINALIZATION,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.AllocateBlock
  )
  public static OMRequest disallowAllocateBlockWithECReplicationConfig(
      OMRequest req, ValidationContext ctx) throws ServiceException {
    if (ctx.versionManager().getMetadataLayoutVersion()
        < OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion()) {
      if (req.getAllocateBlockRequest().getKeyArgs().hasEcReplicationConfig()) {
        throw new ServiceException("Cluster does not have the Erasure Coded"
            + " Storage support feature finalized yet, but the request contains"
            + " an Erasure Coded replication type. Rejecting the request,"
            + " please finalize the cluster upgrade and then try again.");
      }
    }
    return req;
  }
}
