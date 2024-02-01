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

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.file.OMDirectoryCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateDirectoryRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateDirectoryResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .Status;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_KEY_NAME;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS_IN_GIVENPATH;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.NONE;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS;

/**
 * Handle create directory request.
 */
public class OMDirectoryCreateRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDirectoryCreateRequest.class);

  // The maximum number of directories which can be created through a single
  // transaction (recursive directory creations) is 2^8 - 1 as only 8
  // bits are set aside for this in ObjectID.
  private static final long MAX_NUM_OF_RECURSIVE_DIRS = 255;

  /**
   * Stores the result of request execution in
   * OMClientRequest#validateAndUpdateCache.
   */
  public enum Result {
    SUCCESS, // The request was executed successfully

    DIRECTORY_ALREADY_EXISTS, // Directory key already exists in DB

    FAILURE // The request failed and exception was thrown
  }

  public OMDirectoryCreateRequest(OMRequest omRequest,
                                  BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    CreateDirectoryRequest createDirectoryRequest =
        getOmRequest().getCreateDirectoryRequest();
    Preconditions.checkNotNull(createDirectoryRequest);

    OmUtils.verifyKeyNameWithSnapshotReservedWord(
        createDirectoryRequest.getKeyArgs().getKeyName());

    KeyArgs.Builder newKeyArgs = createDirectoryRequest.getKeyArgs()
        .toBuilder().setModificationTime(Time.now());

    CreateDirectoryRequest.Builder newCreateDirectoryRequest =
        createDirectoryRequest.toBuilder().setKeyArgs(newKeyArgs);

    return getOmRequest().toBuilder().setCreateDirectoryRequest(
        newCreateDirectoryRequest).setUserInfo(getUserInfo()).build();

  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex) {

    CreateDirectoryRequest createDirectoryRequest = getOmRequest()
        .getCreateDirectoryRequest();
    KeyArgs keyArgs = createDirectoryRequest.getKeyArgs();

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    omResponse.setCreateDirectoryResponse(CreateDirectoryResponse.newBuilder());
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumCreateDirectory();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquiredLock = false;
    Exception exception = null;
    OMClientResponse omClientResponse = null;
    Result result = Result.FAILURE;
    List<OmKeyInfo> missingParentInfos;
    int numMissingParents = 0;

    try {
      keyArgs = resolveBucketLink(ozoneManager, keyArgs, auditMap);
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();

      // check Acl
      checkKeyAcls(ozoneManager, volumeName, bucketName, keyName,
          IAccessAuthorizer.ACLType.CREATE, OzoneObj.ResourceType.KEY);

      // Check if this is the root of the filesystem.
      if (keyName.length() == 0) {
        throw new OMException("Directory create failed. Cannot create " +
            "directory at root of the filesystem",
            OMException.ResultCodes.CANNOT_CREATE_DIRECTORY_AT_ROOT);
      }
      // acquire lock
      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      Path keyPath = Paths.get(keyName);

      // Need to check if any files exist in the given path, if they exist we
      // cannot create a directory with the given key.
      OMFileRequest.OMPathInfo omPathInfo =
          OMFileRequest.verifyFilesInPath(omMetadataManager, volumeName,
              bucketName, keyName, keyPath);
      OMFileRequest.OMDirectoryResult omDirectoryResult =
          omPathInfo.getDirectoryResult();

      OmKeyInfo dirKeyInfo = null;
      if (omDirectoryResult == FILE_EXISTS ||
          omDirectoryResult == FILE_EXISTS_IN_GIVENPATH) {
        throw new OMException("Unable to create directory: " + keyName
            + " in volume/bucket: " + volumeName + "/" + bucketName,
            FILE_ALREADY_EXISTS);
      } else if (omDirectoryResult == DIRECTORY_EXISTS_IN_GIVENPATH ||
          omDirectoryResult == NONE) {
        List<String> missingParents = omPathInfo.getMissingParents();
        long baseObjId = ozoneManager.getObjectIdFromTxId(trxnLogIndex);
        OmBucketInfo omBucketInfo =
            getBucketInfo(omMetadataManager, volumeName, bucketName);

        dirKeyInfo = createDirectoryKeyInfoWithACL(keyName, keyArgs, baseObjId,
            omBucketInfo, omPathInfo, trxnLogIndex,
            ozoneManager.getDefaultReplicationConfig());

        missingParentInfos = getAllParentInfo(ozoneManager, keyArgs,
            missingParents, omBucketInfo, omPathInfo, trxnLogIndex);

        numMissingParents = missingParentInfos.size();
        checkBucketQuotaInNamespace(omBucketInfo, numMissingParents + 1L);
        omBucketInfo.incrUsedNamespace(numMissingParents + 1L);

        OMFileRequest.addKeyTableCacheEntries(omMetadataManager, volumeName,
            bucketName, omBucketInfo.getBucketLayout(),
            dirKeyInfo, missingParentInfos, trxnLogIndex);
        
        result = Result.SUCCESS;
        omClientResponse = new OMDirectoryCreateResponse(omResponse.build(),
            dirKeyInfo, missingParentInfos, result, getBucketLayout(),
            omBucketInfo.copyObject());
      } else {
        // omDirectoryResult == DIRECTORY_EXITS
        result = Result.DIRECTORY_ALREADY_EXISTS;
        omResponse.setStatus(Status.DIRECTORY_ALREADY_EXISTS);
        omClientResponse = new OMDirectoryCreateResponse(omResponse.build(),
            result);
      }
    } catch (IOException | InvalidPathException ex) {
      exception = ex;
      omClientResponse = new OMDirectoryCreateResponse(
          createErrorOMResponse(omResponse, exception), result);
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    auditLog(auditLogger, buildAuditMessage(OMAction.CREATE_DIRECTORY,
        auditMap, exception, userInfo));

    logResult(createDirectoryRequest, keyArgs, omMetrics, result,
        exception, numMissingParents);

    return omClientResponse;
  }

  /**
   * Construct OmKeyInfo for every parent directory in missing list.
   * @param ozoneManager
   * @param keyArgs
   * @param missingParents list of parent directories to be created
   * @param bucketInfo
   * @param omPathInfo
   * @param trxnLogIndex
   * @return
   * @throws IOException
   */
  public static List<OmKeyInfo> getAllParentInfo(OzoneManager ozoneManager,
      KeyArgs keyArgs, List<String> missingParents, OmBucketInfo bucketInfo,
      OMFileRequest.OMPathInfo omPathInfo, long trxnLogIndex)
      throws IOException {
    List<OmKeyInfo> missingParentInfos = new ArrayList<>();

    // The base id is left shifted by 8 bits for creating space to
    // create (2^8 - 1) object ids in every request.
    // maxObjId represents the largest object id allocation possible inside
    // the transaction.
    long baseObjId = ozoneManager.getObjectIdFromTxId(trxnLogIndex);
    long maxObjId = baseObjId + MAX_NUM_OF_RECURSIVE_DIRS;
    long objectCount = 1; // baseObjID is used by the leaf directory

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    for (String missingKey : missingParents) {
      long nextObjId = baseObjId + objectCount;
      if (nextObjId > maxObjId) {
        throw new OMException("Too many directories in path. Exceeds limit of "
            + MAX_NUM_OF_RECURSIVE_DIRS + ". Unable to create directory: "
            + keyName + " in volume/bucket: " + volumeName + "/" + bucketName,
            INVALID_KEY_NAME);
      }

      LOG.debug("missing parent {} getting added to KeyTable", missingKey);

      OmKeyInfo parentKeyInfo =
          createDirectoryKeyInfoWithACL(missingKey, keyArgs, nextObjId,
              bucketInfo, omPathInfo, trxnLogIndex,
              ozoneManager.getDefaultReplicationConfig());
      objectCount++;

      missingParentInfos.add(parentKeyInfo);
    }

    return missingParentInfos;
  }

  private void logResult(CreateDirectoryRequest createDirectoryRequest,
      KeyArgs keyArgs, OMMetrics omMetrics, Result result,
      Exception exception, int numMissingParents) {

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    switch (result) {
    case SUCCESS:
      // Count for the missing parents plus the directory being created.
      omMetrics.incNumKeys(numMissingParents + 1);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Directory created. Volume:{}, Bucket:{}, Key:{}",
            volumeName, bucketName, keyName);
      }
      break;
    case DIRECTORY_ALREADY_EXISTS:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Directory already exists. Volume:{}, Bucket:{}, Key{}",
            volumeName, bucketName, keyName, exception);
      }
      break;
    case FAILURE:
      omMetrics.incNumCreateDirectoryFails();
      LOG.error("Directory creation failed. Volume:{}, Bucket:{}, Key{}. " +
          "Exception:{}", volumeName, bucketName, keyName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMDirectoryCreateRequest: {}",
          createDirectoryRequest);
    }
  }

  /**
   * fill in a KeyInfo for a new directory entry in OM database.
   * without initializing ACLs from the KeyArgs - used for intermediate
   * directories which get created internally/recursively during file
   * and directory create.
   * @param keyName
   * @param keyArgs
   * @param objectId
   * @param bucketInfo
   * @param omPathInfo
   * @param transactionIndex
   * @param serverDefaultReplConfig
   * @return the OmKeyInfo structure
   */
  public static OmKeyInfo createDirectoryKeyInfoWithACL(String keyName,
      KeyArgs keyArgs, long objectId, OmBucketInfo bucketInfo,
      OMFileRequest.OMPathInfo omPathInfo, long transactionIndex,
      ReplicationConfig serverDefaultReplConfig) {
    return dirKeyInfoBuilderNoACL(keyName, keyArgs, objectId,
        serverDefaultReplConfig)
        .setAcls(getAclsForDir(keyArgs, bucketInfo, omPathInfo))
        .setUpdateID(transactionIndex).build();
  }

  private static OmKeyInfo.Builder dirKeyInfoBuilderNoACL(String keyName,
      KeyArgs keyArgs, long objectId,
      ReplicationConfig serverDefaultReplConfig) {
    String dirName = OzoneFSUtils.addTrailingSlashIfNeeded(keyName);

    OmKeyInfo.Builder keyInfoBuilder =
        new OmKeyInfo.Builder()
            .setVolumeName(keyArgs.getVolumeName())
            .setBucketName(keyArgs.getBucketName())
            .setKeyName(dirName)
            .setOmKeyLocationInfos(Collections.singletonList(
                new OmKeyLocationInfoGroup(0, new ArrayList<>())))
            .setCreationTime(keyArgs.getModificationTime())
            .setModificationTime(keyArgs.getModificationTime())
            .setDataSize(0);
    if (keyArgs.getFactor() != null && keyArgs
        .getFactor() != HddsProtos.ReplicationFactor.ZERO && keyArgs
        .getType() != HddsProtos.ReplicationType.EC) {
      // Factor available and not an EC replication config.
      keyInfoBuilder.setReplicationConfig(ReplicationConfig
          .fromProtoTypeAndFactor(keyArgs.getType(), keyArgs.getFactor()));
    } else if (keyArgs.getType() == HddsProtos.ReplicationType.EC) {
      // Found EC type
      keyInfoBuilder.setReplicationConfig(
          new ECReplicationConfig(keyArgs.getEcReplicationConfig()));
    } else {
      // default type
      keyInfoBuilder.setReplicationConfig(serverDefaultReplConfig);
    }

    keyInfoBuilder.setObjectID(objectId);
    return keyInfoBuilder;
  }

  static long getMaxNumOfRecursiveDirs() {
    return MAX_NUM_OF_RECURSIVE_DIRS;
  }

  @RequestFeatureValidator(
      conditions = ValidationCondition.CLUSTER_NEEDS_FINALIZATION,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CreateDirectory
  )
  public static OMRequest disallowCreateDirectoryWithECReplicationConfig(
      OMRequest req, ValidationContext ctx) throws OMException {
    if (!ctx.versionManager().
        isAllowed(OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT)) {
      if (req.getCreateDirectoryRequest().getKeyArgs()
          .hasEcReplicationConfig()) {
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
   * Validates directory create requests.
   * Handles the cases where an older client attempts to create a directory
   * inside a bucket with a non LEGACY bucket layout.
   * We do not want an older client modifying a bucket that it cannot
   * understand.
   *
   * @param req - the request to validate
   * @param ctx - the validation context
   * @return the validated request
   * @throws OMException if the request is invalid
   */
  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CreateDirectory
  )
  public static OMRequest blockCreateDirectoryWithBucketLayoutFromOldClient(
      OMRequest req, ValidationContext ctx) throws IOException {
    if (req.getCreateDirectoryRequest().hasKeyArgs()) {
      KeyArgs keyArgs = req.getCreateDirectoryRequest().getKeyArgs();

      if (keyArgs.hasVolumeName() && keyArgs.hasBucketName()) {
        BucketLayout bucketLayout = ctx.getBucketLayout(
            keyArgs.getVolumeName(), keyArgs.getBucketName());
        bucketLayout.validateSupportedOperation();
      }
    }
    return req;
  }
}
