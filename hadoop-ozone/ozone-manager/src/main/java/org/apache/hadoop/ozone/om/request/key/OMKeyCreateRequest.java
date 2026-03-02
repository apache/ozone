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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.utils.UniqueId;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OzoneConfigUtil;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.lock.OzoneLockStrategy;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCreateResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles CreateKey request.
 */

public class OMKeyCreateRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyCreateRequest.class);

  public OMKeyCreateRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    CreateKeyRequest createKeyRequest = super.preExecute(ozoneManager)
        .getCreateKeyRequest();
    Objects.requireNonNull(createKeyRequest, "createKeyRequest == null");

    KeyArgs keyArgs = createKeyRequest.getKeyArgs();

    final OMPerformanceMetrics perfMetrics = ozoneManager.getPerfMetrics();

    if (keyArgs.hasExpectedDataGeneration()) {
      ozoneManager.checkFeatureEnabled(OzoneManagerVersion.ATOMIC_REWRITE_KEY);
    }

    OmUtils.verifyKeyNameWithSnapshotReservedWord(keyArgs.getKeyName());
    if (ozoneManager.getConfig().isKeyNameCharacterCheckEnabled()) {
      OmUtils.validateKeyName(keyArgs.getKeyName());
    }


    String keyPath = keyArgs.getKeyName();
    keyPath = validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(),
        keyPath, getBucketLayout());

    // We cannot allocate block for multipart upload part when
    // createMultipartKey is called, as we will not know type and factor with
    // which initiateMultipartUpload has started for this key. When
    // allocateBlock call happen's we shall know type and factor, as we set
    // the type and factor read from multipart table, and set the KeyInfo in
    // validateAndUpdateCache and return to the client. TODO: See if we can fix
    //  this. We do not call allocateBlock in openKey for multipart upload.

    CreateKeyRequest.Builder newCreateKeyRequest = null;
    KeyArgs.Builder newKeyArgs = null;
    UserInfo userInfo = getUserInfo();
    if (!keyArgs.getIsMultipartKey()) {

      long scmBlockSize = ozoneManager.getScmBlockSize();

      // NOTE size of a key is not a hard limit on anything, it is a value that
      // client should expect, in terms of current size of key. If client sets
      // a value, then this value is used, otherwise, we allocate a single
      // block which is the current size, if read by the client.
      final long requestedSize = keyArgs.getDataSize() > 0 ?
          keyArgs.getDataSize() : scmBlockSize;

      HddsProtos.ReplicationFactor factor = keyArgs.getFactor();
      HddsProtos.ReplicationType type = keyArgs.getType();

      final OmBucketInfo bucketInfo = ozoneManager
          .getBucketInfo(keyArgs.getVolumeName(), keyArgs.getBucketName());
      final ReplicationConfig repConfig = OzoneConfigUtil
          .resolveReplicationConfigPreference(type, factor,
              keyArgs.getEcReplicationConfig(),
              bucketInfo.getDefaultReplicationConfig(),
              ozoneManager);

      final StorageType storageType = resolveEffectiveStoragePolicy(
          bucketInfo, ozoneManager).getPrimaryStorageType();

      // TODO: Here we are allocating block with out any check for
      //  bucket/key/volume or not and also with out any authorization checks.
      //  As for a client for the first time this can be executed on any OM,
      //  till leader is identified.

      List<OmKeyLocationInfo> omKeyLocationInfoList;
      final long effectiveDataSize;
      // Skip block allocation if dataSize <= 0. We also consider unspecified dataSize as
      // empty key since the client will not set dataSize if the key is empty (i.e. dataSize <= 0),
      if (!keyArgs.hasDataSize() || keyArgs.getDataSize() <= 0) {
        omKeyLocationInfoList = Collections.emptyList();
        effectiveDataSize = 0;
      } else {
        omKeyLocationInfoList = captureLatencyNs(perfMetrics.getCreateKeyAllocateBlockLatencyNs(),
            () -> allocateBlock(ozoneManager.getScmClient(),
                ozoneManager.getBlockTokenSecretManager(), repConfig,
                new ExcludeList(), requestedSize, scmBlockSize,
                ozoneManager.getPreallocateBlocksMax(),
                ozoneManager.isGrpcBlockTokenEnabled(),
                ozoneManager.getOMServiceId(),
                ozoneManager.getMetrics(),
                keyArgs.getSortDatanodes(),
                userInfo,
                storageType));
        effectiveDataSize = requestedSize;
      }

      newKeyArgs = keyArgs.toBuilder().setModificationTime(Time.now())
              .setType(type).setFactor(factor)
              .setDataSize(effectiveDataSize);

      newKeyArgs.addAllKeyLocations(omKeyLocationInfoList.stream()
          .map(info -> info.getProtobuf(false,
              getOmRequest().getVersion()))
          .collect(Collectors.toList()));
    } else {
      newKeyArgs = keyArgs.toBuilder().setModificationTime(Time.now());
    }

    newKeyArgs.setKeyName(keyPath);

    if (keyArgs.getIsMultipartKey()) {
      getFileEncryptionInfoForMpuKey(keyArgs, newKeyArgs, ozoneManager);
    } else {
      generateRequiredEncryptionInfo(keyArgs, newKeyArgs, ozoneManager);
    }

    KeyArgs.Builder finalNewKeyArgs = newKeyArgs;
    KeyArgs resolvedKeyArgs =
        captureLatencyNs(perfMetrics.getCreateKeyResolveBucketAndAclCheckLatencyNs(), 
            () -> resolveBucketAndCheckKeyAcls(finalNewKeyArgs.build(), ozoneManager,
            IAccessAuthorizer.ACLType.CREATE));
    newCreateKeyRequest =
        createKeyRequest.toBuilder().setKeyArgs(resolvedKeyArgs)
            .setClientID(UniqueId.next());

    return getOmRequest().toBuilder()
        .setCreateKeyRequest(newCreateKeyRequest).setUserInfo(userInfo)
        .build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();
    CreateKeyRequest createKeyRequest = getOmRequest().getCreateKeyRequest();

    KeyArgs keyArgs = createKeyRequest.getKeyArgs();
    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyAllocates();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OzoneLockStrategy ozoneLockStrategy = getOzoneLockStrategy(ozoneManager);
    OmKeyInfo omKeyInfo = null;
    final List< OmKeyLocationInfo > locations = new ArrayList<>();

    boolean acquireLock = false;
    OMClientResponse omClientResponse = null;
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    Exception exception = null;
    Result result = null;
    List<OmKeyInfo> missingParentInfos = null;
    int numMissingParents = 0;
    final OMPerformanceMetrics perfMetrics = ozoneManager.getPerfMetrics();
    long createKeyStartTime = Time.monotonicNowNanos();
    try {

      mergeOmLockDetails(
          ozoneLockStrategy.acquireWriteLock(omMetadataManager, volumeName,
              bucketName, keyName));
      acquireLock = getOmLockDetails().isLockAcquired();
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);
      //TODO: We can optimize this get here, if getKmsProvider is null, then
      // bucket encryptionInfo will be not set. If this assumption holds
      // true, we can avoid get from bucket table.

      // Check if Key already exists
      String dbKeyName = omMetadataManager.getOzoneKey(volumeName, bucketName,
          keyName);
      OmKeyInfo dbKeyInfo = omMetadataManager.getKeyTable(getBucketLayout())
          .getIfExist(dbKeyName);
      validateAtomicRewrite(dbKeyInfo, keyArgs);

      OmBucketInfo bucketInfo =
          getBucketInfo(omMetadataManager, volumeName, bucketName);

      // If FILE_EXISTS we just override like how we used to do for Key Create.
      if (LOG.isDebugEnabled()) {
        LOG.debug("BucketName: {}, BucketLayout: {}",
            bucketInfo.getBucketName(), bucketInfo.getBucketLayout());
      }

      OMFileRequest.OMPathInfo pathInfo = null;

      if (bucketInfo.getBucketLayout()
          .shouldNormalizePaths(ozoneManager.getEnableFileSystemPaths())) {
        pathInfo = OMFileRequest.verifyFilesInPath(omMetadataManager,
            volumeName, bucketName, keyName, Paths.get(keyName));
        OMFileRequest.OMDirectoryResult omDirectoryResult =
            pathInfo.getDirectoryResult();

        // Check if a file or directory exists with same key name.
        if (omDirectoryResult == DIRECTORY_EXISTS) {
          throw new OMException("Cannot write to " +
              "directory. createIntermediateDirs behavior is enabled and " +
              "hence / has special interpretation: " + keyName, NOT_A_FILE);
        } else
          if (omDirectoryResult == FILE_EXISTS_IN_GIVENPATH) {
            throw new OMException("Can not create file: " + keyName +
                " as there is already file in the given path", NOT_A_FILE);
          }

        missingParentInfos = getAllParentInfo(ozoneManager, keyArgs,
            pathInfo.getMissingParents(), bucketInfo,
                pathInfo, trxnLogIndex);

        numMissingParents = missingParentInfos.size();
      }

      ReplicationConfig replicationConfig = OzoneConfigUtil
          .resolveReplicationConfigPreference(keyArgs.getType(),
              keyArgs.getFactor(), keyArgs.getEcReplicationConfig(),
              bucketInfo.getDefaultReplicationConfig(),
              ozoneManager);

      omKeyInfo = prepareKeyInfo(omMetadataManager, keyArgs, dbKeyInfo,
          keyArgs.getDataSize(), locations, getFileEncryptionInfo(keyArgs),
          ozoneManager.getPrefixManager(), bucketInfo, pathInfo, trxnLogIndex,
          ozoneManager.getObjectIdFromTxId(trxnLogIndex),
          replicationConfig, ozoneManager.getConfig());

      validateEncryptionKeyInfo(bucketInfo, keyArgs);

      long openVersion = omKeyInfo.getLatestVersionLocations().getVersion();
      long clientID = createKeyRequest.getClientID();
      String dbOpenKeyName = omMetadataManager.getOpenKey(volumeName,
          bucketName, keyName, clientID);

      // Append new blocks
      List<OmKeyLocationInfo> newLocationList = keyArgs.getKeyLocationsList()
          .stream().map(OmKeyLocationInfo::getFromProtobuf)
          .collect(Collectors.toList());
      omKeyInfo.appendNewBlocks(newLocationList, false);

      // Here we refer to the implementation of HDFS:
      // If the key size is 600MB, when createKey, keyLocationInfo in
      // keyLocationList is 3, and  the every pre-allocated block length is
      // 256MB. If the number of factor  is 3, the total pre-allocated block
      // ize is 256MB * 3 * 3. We will allocate more 256MB * 3 * 3 - 600mb * 3
      // = 504MB in advance, and we  will subtract this part when we finally
      // commitKey.
      long preAllocatedSpace = newLocationList.size()
          * ozoneManager.getScmBlockSize()
          * replicationConfig.getRequiredNodes();
      // check bucket and volume quota
      long quotaCheckStartTime = Time.monotonicNowNanos();
      checkBucketQuotaInBytes(omMetadataManager, bucketInfo,
          preAllocatedSpace);
      checkBucketQuotaInNamespace(bucketInfo, numMissingParents + 1L);
      perfMetrics.addCreateKeyQuotaCheckLatencyNs(Time.monotonicNowNanos() - quotaCheckStartTime);
      bucketInfo.incrUsedNamespace(numMissingParents);

      if (numMissingParents > 0) {
        // Add cache entries for the prefix directories.
        // Skip adding for the file key itself, until Key Commit.
        OMFileRequest.addKeyTableCacheEntries(omMetadataManager, volumeName,
            bucketName, bucketInfo.getBucketLayout(),
            null, missingParentInfos, trxnLogIndex);
      }

      // Add to cache entry can be done outside of lock for this openKey.
      // Even if bucket gets deleted, when commitKey we shall identify if
      // bucket gets deleted.
      omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
          dbOpenKeyName, omKeyInfo, trxnLogIndex);

      // Prepare response
      omResponse.setCreateKeyResponse(CreateKeyResponse.newBuilder()
          .setKeyInfo(omKeyInfo.getNetworkProtobuf(getOmRequest().getVersion(),
              keyArgs.getLatestVersionLocation()))
          .setID(clientID)
          .setOpenVersion(openVersion).build())
          .setCmdType(Type.CreateKey);
      omClientResponse = new OMKeyCreateResponse(omResponse.build(),
          omKeyInfo, missingParentInfos, clientID, bucketInfo.copyObject());

      result = Result.SUCCESS;
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omMetrics.incNumKeyAllocateFails();
      omResponse.setCmdType(Type.CreateKey);
      omClientResponse = new OMKeyCreateResponse(
          createErrorOMResponse(omResponse, exception), getBucketLayout());
    } finally {
      long createKeyLatency = Time.monotonicNowNanos() - createKeyStartTime;

      if (Result.SUCCESS.equals(result)) {
        perfMetrics.addCreateKeySuccessLatencyNs(createKeyLatency);
      } else {
        perfMetrics.addCreateKeyFailureLatencyNs(createKeyLatency);
      }
      
      if (acquireLock) {
        mergeOmLockDetails(ozoneLockStrategy
            .releaseWriteLock(omMetadataManager, volumeName,
                bucketName, keyName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    // Audit Log outside the lock
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.ALLOCATE_KEY, auditMap, exception,
        getOmRequest().getUserInfo()));

    logResult(createKeyRequest, omMetrics, exception, result,
            numMissingParents);

    return omClientResponse;
  }

  protected void logResult(CreateKeyRequest createKeyRequest,
      OMMetrics omMetrics, Exception exception, Result result,
       int numMissingParents) {
    switch (result) {
    case SUCCESS:
      // Missing directories are created immediately, counting that here.
      // The metric for the key is incremented as part of the key commit.
      omMetrics.incNumKeys(numMissingParents);
      LOG.debug("Key created. Volume:{}, Bucket:{}, Key:{}",
              createKeyRequest.getKeyArgs().getVolumeName(),
              createKeyRequest.getKeyArgs().getBucketName(),
              createKeyRequest.getKeyArgs().getKeyName());
      break;
    case FAILURE:
      if (createKeyRequest.getKeyArgs().hasEcReplicationConfig()) {
        omMetrics.incEcKeyCreateFailsTotal();
      }
      LOG.error("Key creation failed. Volume:{}, Bucket:{}, Key:{}. ",
              createKeyRequest.getKeyArgs().getVolumeName(),
              createKeyRequest.getKeyArgs().getBucketName(),
              createKeyRequest.getKeyArgs().getKeyName(), exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyCreateRequest: {}",
          createKeyRequest);
    }
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.CLUSTER_NEEDS_FINALIZATION,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CreateKey
  )
  public static OMRequest disallowCreateKeyWithECReplicationConfig(
      OMRequest req, ValidationContext ctx) throws OMException {
    if (!ctx.versionManager()
        .isAllowed(OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT)) {
      if (req.getCreateKeyRequest().getKeyArgs().hasEcReplicationConfig()) {
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
   * Validates key create requests.
   * We do not want to allow older clients to create keys in buckets which use
   * non LEGACY layouts.
   *
   * @param req - the request to validate
   * @param ctx - the validation context
   * @return the validated request
   * @throws OMException if the request is invalid
   */
  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CreateKey
  )
  public static OMRequest blockCreateKeyWithBucketLayoutFromOldClient(
      OMRequest req, ValidationContext ctx) throws IOException {
    if (req.getCreateKeyRequest().hasKeyArgs()) {
      KeyArgs keyArgs = req.getCreateKeyRequest().getKeyArgs();

      if (keyArgs.hasVolumeName() && keyArgs.hasBucketName()) {
        BucketLayout bucketLayout = ctx.getBucketLayout(
            keyArgs.getVolumeName(), keyArgs.getBucketName());
        bucketLayout.validateSupportedOperation();
      }
    }
    return req;
  }

  protected void validateAtomicRewrite(OmKeyInfo dbKeyInfo, KeyArgs keyArgs)
      throws OMException {
    if (keyArgs.hasExpectedDataGeneration()) {
      // If a key does not exist, or if it exists but the updateID do not match, then fail this request.
      if (dbKeyInfo == null) {
        throw new OMException("Key not found during expected rewrite", OMException.ResultCodes.KEY_NOT_FOUND);
      }
      if (dbKeyInfo.getUpdateID() != keyArgs.getExpectedDataGeneration()) {
        throw new OMException("Generation mismatch during expected rewrite", OMException.ResultCodes.KEY_NOT_FOUND);
      }
    }
  }
}
