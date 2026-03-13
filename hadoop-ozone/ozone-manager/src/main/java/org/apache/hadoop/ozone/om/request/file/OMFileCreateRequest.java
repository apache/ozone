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

package org.apache.hadoop.ozone.om.request.file;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateFile;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.utils.UniqueId;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneConfigUtil;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.OMClientVersionValidator;
import org.apache.hadoop.ozone.om.request.validation.OMLayoutVersionValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.file.OMFileCreateResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateFileResponse;
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
 * Handles create file request.
 */
public class OMFileCreateRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMFileCreateRequest.class);

  public OMFileCreateRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    CreateFileRequest createFileRequest = super.preExecute(ozoneManager)
        .getCreateFileRequest();
    Objects.requireNonNull(createFileRequest, "createFileRequest == null");

    KeyArgs keyArgs = createFileRequest.getKeyArgs();

    OmUtils.verifyKeyNameWithSnapshotReservedWord(keyArgs.getKeyName());
    if (ozoneManager.getConfig().isKeyNameCharacterCheckEnabled()) {
      OmUtils.validateKeyName(keyArgs.getKeyName());
    }

    UserInfo userInfo = getUserInfo();
    if (keyArgs.getKeyName().isEmpty()) {
      // Check if this is the root of the filesystem.
      // Not throwing exception here, as need to throw exception after
      // checking volume/bucket exists.
      return getOmRequest().toBuilder().setUserInfo(userInfo).build();
    }

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

    // TODO: Here we are allocating block with out any check for
    //  bucket/key/volume or not and also with out any authorization checks.
    // We also allocate block even if requestedSize is 0 because unlike
    // CreateKey which is used for S3 use case where the requested dataSize is known in advance
    // and KeyArgs.dataSize is not going to be set for empty key
    // File system client does not know the final file size in advance but use 0 as
    // the placeholder for the data size. Therefore, we should at least allocate a
    // single block and we cannot simply skip the allocate block call
    List< OmKeyLocationInfo > omKeyLocationInfoList =
        allocateBlock(ozoneManager.getScmClient(),
              ozoneManager.getBlockTokenSecretManager(), repConfig,
              new ExcludeList(), requestedSize, scmBlockSize,
              ozoneManager.getPreallocateBlocksMax(),
              ozoneManager.isGrpcBlockTokenEnabled(),
              ozoneManager.getOMServiceId(),
              ozoneManager.getMetrics(),
              keyArgs.getSortDatanodes(),
              userInfo);

    KeyArgs.Builder newKeyArgs = keyArgs.toBuilder()
        .setModificationTime(Time.now()).setType(type).setFactor(factor)
        .setDataSize(requestedSize);

    newKeyArgs.addAllKeyLocations(omKeyLocationInfoList.stream()
        .map(info -> info.getProtobuf(getOmRequest().getVersion()))
        .collect(Collectors.toList()));

    generateRequiredEncryptionInfo(keyArgs, newKeyArgs, ozoneManager);

    KeyArgs resolvedArgs = resolveBucketAndCheckKeyAcls(newKeyArgs.build(),
        ozoneManager, IAccessAuthorizer.ACLType.CREATE);
    CreateFileRequest.Builder newCreateFileRequest =
        createFileRequest.toBuilder().setKeyArgs(resolvedArgs)
            .setClientID(UniqueId.next());

    return getOmRequest().toBuilder()
        .setCreateFileRequest(newCreateFileRequest).setUserInfo(userInfo)
        .build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    CreateFileRequest createFileRequest = getOmRequest().getCreateFileRequest();
    KeyArgs keyArgs = createFileRequest.getKeyArgs();
    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();
    int numMissingParents = 0;

    // if isRecursive is true, file would be created even if parent
    // directories does not exist.
    boolean isRecursive = createFileRequest.getIsRecursive();
    if (LOG.isDebugEnabled()) {
      LOG.debug("File create for : " + volumeName + "/" + bucketName + "/"
          + keyName + ":" + isRecursive);
    }

    // if isOverWrite is true, file would be over written.
    boolean isOverWrite = createFileRequest.getIsOverwrite();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumCreateFile();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    boolean acquiredLock = false;

    OmKeyInfo omKeyInfo = null;
    OmBucketInfo omBucketInfo = null;
    final List<OmKeyLocationInfo> locations = new ArrayList<>();
    List<OmKeyInfo> missingParentInfos;

    OMClientResponse omClientResponse = null;
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    Exception exception = null;
    Result result = null;
    try {
      // acquire lock
      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      if (keyName.isEmpty()) {
        // Check if this is the root of the filesystem.
        throw new OMException("Can not write to directory: " + keyName,
            OMException.ResultCodes.NOT_A_FILE);
      }

      String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
          keyName);
      OmKeyInfo dbKeyInfo = omMetadataManager.getKeyTable(getBucketLayout())
          .getIfExist(ozoneKey);

      OMFileRequest.OMPathInfo pathInfo =
          OMFileRequest.verifyFilesInPath(omMetadataManager, volumeName,
              bucketName, keyName, Paths.get(keyName));
      OMFileRequest.OMDirectoryResult omDirectoryResult =
          pathInfo.getDirectoryResult();

      // Check if a file or directory exists with same key name.
      checkDirectoryResult(keyName, isOverWrite, omDirectoryResult);

      if (!isRecursive) {
        checkAllParentsExist(keyArgs, pathInfo);
      }

      // do open key
      omBucketInfo =
          getBucketInfo(omMetadataManager, volumeName, bucketName);
      final ReplicationConfig repConfig = OzoneConfigUtil
          .resolveReplicationConfigPreference(keyArgs.getType(),
              keyArgs.getFactor(), keyArgs.getEcReplicationConfig(),
              omBucketInfo.getDefaultReplicationConfig(),
              ozoneManager);

      omKeyInfo = prepareKeyInfo(omMetadataManager, keyArgs, dbKeyInfo,
          keyArgs.getDataSize(), locations, getFileEncryptionInfo(keyArgs),
          ozoneManager.getPrefixManager(), omBucketInfo, pathInfo, trxnLogIndex,
          ozoneManager.getObjectIdFromTxId(trxnLogIndex),
          repConfig, ozoneManager.getConfig());
      validateEncryptionKeyInfo(omBucketInfo, keyArgs);

      long openVersion = omKeyInfo.getLatestVersionLocations().getVersion();
      long clientID = createFileRequest.getClientID();
      String dbOpenKeyName = omMetadataManager.getOpenKey(volumeName,
          bucketName, keyName, clientID);

      missingParentInfos = getAllParentInfo(ozoneManager, keyArgs,
              pathInfo.getMissingParents(), omBucketInfo,
              pathInfo, trxnLogIndex);

      // Append new blocks
      List<OmKeyLocationInfo> newLocationList = keyArgs.getKeyLocationsList()
          .stream().map(OmKeyLocationInfo::getFromProtobuf)
          .collect(Collectors.toList());
      omKeyInfo.appendNewBlocks(newLocationList, false);
      // check bucket and volume quota
      long preAllocatedSpace = newLocationList.size()
          * ozoneManager.getScmBlockSize()
          * omKeyInfo.getReplicationConfig().getRequiredNodes();
      checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
          preAllocatedSpace);
      numMissingParents = missingParentInfos.size();
      checkBucketQuotaInNamespace(omBucketInfo, numMissingParents + 1L);
      omBucketInfo.incrUsedNamespace(numMissingParents);

      // Add to cache entry can be done outside of lock for this openKey.
      // Even if bucket gets deleted, when commitKey we shall identify if
      // bucket gets deleted.
      omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
          dbOpenKeyName, omKeyInfo, trxnLogIndex);

      // Add cache entries for the prefix directories.
      // Skip adding for the file key itself, until Key Commit.
      OMFileRequest.addKeyTableCacheEntries(omMetadataManager, volumeName,
          bucketName, omBucketInfo.getBucketLayout(),
          null, missingParentInfos, trxnLogIndex);

      // Prepare response
      omResponse.setCreateFileResponse(CreateFileResponse.newBuilder()
          .setKeyInfo(omKeyInfo.getNetworkProtobuf(getOmRequest().getVersion(),
              keyArgs.getLatestVersionLocation()))
          .setID(clientID)
          .setOpenVersion(openVersion).build())
          .setCmdType(CreateFile);
      omClientResponse = new OMFileCreateResponse(omResponse.build(),
          omKeyInfo, missingParentInfos, clientID, omBucketInfo.copyObject());

      result = Result.SUCCESS;
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omMetrics.incNumCreateFileFails();
      omResponse.setCmdType(CreateFile);
      omClientResponse = new OMFileCreateResponse(createErrorOMResponse(
            omResponse, exception), getBucketLayout());
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    // Audit Log outside the lock
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.CREATE_FILE, auditMap, exception,
        getOmRequest().getUserInfo()));

    switch (result) {
    case SUCCESS:
      // Missing directories are created immediately, counting that here.
      // The metric for the file is incremented as part of the file commit.
      omMetrics.incNumKeys(numMissingParents);
      LOG.debug("File created. Volume:{}, Bucket:{}, Key:{}", volumeName,
          bucketName, keyName);
      break;
    case FAILURE:
      LOG.error("File create failed. Volume:{}, Bucket:{}, Key{}.",
          volumeName, bucketName, keyName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMFileCreateRequest: {}",
          createFileRequest);
    }

    return omClientResponse;
  }

  /**
   * Verify om directory result.
   *
   * @param keyName           key name
   * @param isOverWrite       flag represents whether file can be overwritten
   * @param omDirectoryResult directory result
   * @throws OMException if file or directory or file exists in the given path
   */
  protected void checkDirectoryResult(String keyName, boolean isOverWrite,
      OMFileRequest.OMDirectoryResult omDirectoryResult) throws OMException {
    if (omDirectoryResult == FILE_EXISTS) {
      if (!isOverWrite) {
        throw new OMException("File " + keyName + " already exists",
            OMException.ResultCodes.FILE_ALREADY_EXISTS);
      }
    } else if (omDirectoryResult == DIRECTORY_EXISTS) {
      throw new OMException("Can not write to directory: " + keyName,
          OMException.ResultCodes.NOT_A_FILE);
    } else if (omDirectoryResult == FILE_EXISTS_IN_GIVENPATH) {
      throw new OMException(
          "Can not create file: " + keyName + " as there " +
              "is already file in the given path",
          OMException.ResultCodes.NOT_A_FILE);
    }
  }

  /**
   * Verify the existence of parent directory.
   *
   * @param keyArgs  key arguments
   * @param pathInfo om path info
   * @throws IOException directory not found
   */
  protected void checkAllParentsExist(KeyArgs keyArgs,
      OMFileRequest.OMPathInfo pathInfo) throws IOException {
    String keyName = keyArgs.getKeyName();

    // if immediate parent exists, assume higher level directories exist.
    if (!pathInfo.directParentExists()) {
      throw new OMException("Cannot create file : " + keyName
          + " as one of parent directory is not created",
          OMException.ResultCodes.DIRECTORY_NOT_FOUND);
    }
  }

  @OMLayoutVersionValidator(
      applyBefore = OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = CreateFile
  )
  public static OMRequest disallowCreateFileWithECReplicationConfig(
      OMRequest req, ValidationContext ctx) throws OMException {
    if (!ctx.versionManager()
        .isAllowed(OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT)) {
      if (req.getCreateFileRequest().getKeyArgs().hasEcReplicationConfig()) {
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
   * Validates file create requests.
   * Handles the cases where an older client attempts to create a file
   * inside a bucket with a non LEGACY bucket layout.
   * We do not want an older client modifying a bucket that it cannot
   * understand.
   *
   * @param req - the request to validate
   * @param ctx - the validation context
   * @return the validated request
   * @throws OMException if the request is invalid
   */
  @OMClientVersionValidator(
      applyBefore = ClientVersion.BUCKET_LAYOUT_SUPPORT,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CreateFile
  )
  public static OMRequest blockCreateFileWithBucketLayoutFromOldClient(
      OMRequest req, ValidationContext ctx) throws IOException {
    if (req.getCreateFileRequest().hasKeyArgs()) {

      KeyArgs keyArgs = req.getCreateFileRequest().getKeyArgs();

      if (keyArgs.hasVolumeName() && keyArgs.hasBucketName()) {
        BucketLayout bucketLayout = ctx.getBucketLayout(
            keyArgs.getVolumeName(), keyArgs.getBucketName());
        bucketLayout.validateSupportedOperation();
      }
    }
    return req;
  }
}
