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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneConfigUtil;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.lock.OzoneLockStrategy;
import org.apache.hadoop.ozone.om.request.file.OMDirectoryCreateRequest;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.response.key.OMKeyCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.UniqueId;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH;

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
    CreateKeyRequest createKeyRequest = getOmRequest().getCreateKeyRequest();
    Preconditions.checkNotNull(createKeyRequest);

    KeyArgs keyArgs = createKeyRequest.getKeyArgs();

    // Verify key name
    final boolean checkKeyNameEnabled = ozoneManager.getConfiguration()
         .getBoolean(OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_KEY,
                 OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_DEFAULT);
    if (checkKeyNameEnabled) {
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
              ozoneManager.getDefaultReplicationConfig());

      // TODO: Here we are allocating block with out any check for
      //  bucket/key/volume or not and also with out any authorization checks.
      //  As for a client for the first time this can be executed on any OM,
      //  till leader is identified.

      List<OmKeyLocationInfo> omKeyLocationInfoList =
          allocateBlock(ozoneManager.getScmClient(),
              ozoneManager.getBlockTokenSecretManager(), repConfig,
              new ExcludeList(), requestedSize, scmBlockSize,
              ozoneManager.getPreallocateBlocksMax(),
              ozoneManager.isGrpcBlockTokenEnabled(),
              ozoneManager.getOMNodeId());

      newKeyArgs = keyArgs.toBuilder().setModificationTime(Time.now())
              .setType(type).setFactor(factor)
              .setDataSize(requestedSize);

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

    newCreateKeyRequest =
        createKeyRequest.toBuilder().setKeyArgs(newKeyArgs)
            .setClientID(UniqueId.next());

    return getOmRequest().toBuilder()
        .setCreateKeyRequest(newCreateKeyRequest).setUserInfo(getUserInfo())
        .build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
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
    OmBucketInfo omBucketInfo = null;
    final List< OmKeyLocationInfo > locations = new ArrayList<>();

    boolean acquireLock = false;
    OMClientResponse omClientResponse = null;
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    IOException exception = null;
    Result result = null;
    List<OmKeyInfo> missingParentInfos = null;
    int numMissingParents = 0;
    try {
      keyArgs = resolveBucketLink(ozoneManager, keyArgs, auditMap);
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();

      // check Acl
      checkKeyAcls(ozoneManager, volumeName, bucketName, keyName,
          IAccessAuthorizer.ACLType.CREATE, OzoneObj.ResourceType.KEY);

      acquireLock = ozoneLockStrategy.acquireWriteLock(omMetadataManager,
          volumeName, bucketName, keyName);
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);
      //TODO: We can optimize this get here, if getKmsProvider is null, then
      // bucket encryptionInfo will be not set. If this assumption holds
      // true, we can avoid get from bucket table.

      // Check if Key already exists
      String dbKeyName = omMetadataManager.getOzoneKey(volumeName, bucketName,
          keyName);
      OmKeyInfo dbKeyInfo = omMetadataManager.getKeyTable(getBucketLayout())
          .getIfExist(dbKeyName);

      OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(
          omMetadataManager.getBucketKey(volumeName, bucketName));

      // If FILE_EXISTS we just override like how we used to do for Key Create.
      List< OzoneAcl > inheritAcls;
      if (LOG.isDebugEnabled()) {
        LOG.debug("BucketName: {}, BucketLayout: {}",
            bucketInfo.getBucketName(), bucketInfo.getBucketLayout());
      }
      if (bucketInfo.getBucketLayout()
          .shouldNormalizePaths(ozoneManager.getEnableFileSystemPaths())) {
        OMFileRequest.OMPathInfo pathInfo =
            OMFileRequest.verifyFilesInPath(omMetadataManager, volumeName,
                bucketName, keyName, Paths.get(keyName));
        OMFileRequest.OMDirectoryResult omDirectoryResult =
            pathInfo.getDirectoryResult();
        inheritAcls = pathInfo.getAcls();

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

        missingParentInfos = OMDirectoryCreateRequest
            .getAllParentInfo(ozoneManager, keyArgs,
                pathInfo.getMissingParents(), inheritAcls, trxnLogIndex);

        // Add cache entries for the prefix directories.
        // Skip adding for the file key itself, until Key Commit.
        OMFileRequest.addKeyTableCacheEntries(omMetadataManager, volumeName,
            bucketName, Optional.absent(), Optional.of(missingParentInfos),
            trxnLogIndex);
        numMissingParents = missingParentInfos.size();
      }

      ReplicationConfig replicationConfig = OzoneConfigUtil
          .resolveReplicationConfigPreference(keyArgs.getType(),
              keyArgs.getFactor(), keyArgs.getEcReplicationConfig(),
              bucketInfo.getDefaultReplicationConfig(),
              ozoneManager.getDefaultReplicationConfig());

      omKeyInfo = prepareKeyInfo(omMetadataManager, keyArgs, dbKeyInfo,
          keyArgs.getDataSize(), locations, getFileEncryptionInfo(keyArgs),
          ozoneManager.getPrefixManager(), bucketInfo, trxnLogIndex,
          ozoneManager.getObjectIdFromTxId(trxnLogIndex),
          ozoneManager.isRatisEnabled(), replicationConfig);

      long openVersion = omKeyInfo.getLatestVersionLocations().getVersion();
      long clientID = createKeyRequest.getClientID();
      String dbOpenKeyName = omMetadataManager.getOpenKey(volumeName,
          bucketName, keyName, clientID);

      // Append new blocks
      List<OmKeyLocationInfo> newLocationList = keyArgs.getKeyLocationsList()
          .stream().map(OmKeyLocationInfo::getFromProtobuf)
          .collect(Collectors.toList());
      omKeyInfo.appendNewBlocks(newLocationList, false);

      omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);
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
      checkBucketQuotaInBytes(omBucketInfo, preAllocatedSpace);
      checkBucketQuotaInNamespace(omBucketInfo, 1L);

      // Add to cache entry can be done outside of lock for this openKey.
      // Even if bucket gets deleted, when commitKey we shall identify if
      // bucket gets deleted.
      omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
          new CacheKey<>(dbOpenKeyName),
          new CacheValue<>(Optional.of(omKeyInfo), trxnLogIndex));

      // Prepare response
      omResponse.setCreateKeyResponse(CreateKeyResponse.newBuilder()
          .setKeyInfo(omKeyInfo.getNetworkProtobuf(getOmRequest().getVersion(),
              keyArgs.getLatestVersionLocation()))
          .setID(clientID)
          .setOpenVersion(openVersion).build())
          .setCmdType(Type.CreateKey);
      omClientResponse = new OMKeyCreateResponse(omResponse.build(),
          omKeyInfo, missingParentInfos, clientID, omBucketInfo.copyObject());

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omMetrics.incNumKeyAllocateFails();
      omResponse.setCmdType(Type.CreateKey);
      omClientResponse = new OMKeyCreateResponse(
          createErrorOMResponse(omResponse, exception), getBucketLayout());
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (acquireLock) {
        ozoneLockStrategy.releaseWriteLock(omMetadataManager, volumeName,
            bucketName, keyName);
      }
    }

    // Audit Log outside the lock
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.ALLOCATE_KEY, auditMap, exception,
        getOmRequest().getUserInfo()));

    logResult(createKeyRequest, omMetrics, exception, result,
            numMissingParents);

    return omClientResponse;
  }

  protected void logResult(CreateKeyRequest createKeyRequest,
      OMMetrics omMetrics, IOException exception, Result result,
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
}
