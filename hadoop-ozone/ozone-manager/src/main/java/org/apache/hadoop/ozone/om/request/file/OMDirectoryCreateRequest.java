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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS_IN_GIVENPATH;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.NONE;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.ozone.OmUtils;
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
import org.apache.hadoop.ozone.om.request.OMClientRequestUtils;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.file.OMDirectoryCreateResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDirectoryRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDirectoryResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle create directory request.
 */
public class OMDirectoryCreateRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDirectoryCreateRequest.class);

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
        super.preExecute(ozoneManager).getCreateDirectoryRequest();
    Objects.requireNonNull(createDirectoryRequest, "createDirectoryRequest == null");

    KeyArgs keyArgs = createDirectoryRequest.getKeyArgs();

    OmUtils.verifyKeyNameWithSnapshotReservedWord(keyArgs.getKeyName());

    KeyArgs.Builder newKeyArgs = createDirectoryRequest.getKeyArgs()
        .toBuilder().setModificationTime(Time.now());

    KeyArgs resolvedKeyArgs = resolveBucketAndCheckKeyAcls(newKeyArgs.build(),
        ozoneManager, ACLType.CREATE);
    CreateDirectoryRequest.Builder newCreateDirectoryRequest =
        createDirectoryRequest.toBuilder().setKeyArgs(resolvedKeyArgs);

    return getOmRequest().toBuilder().setCreateDirectoryRequest(
        newCreateDirectoryRequest).setUserInfo(getUserInfo()).build();

  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

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
      // Check if this is the root of the filesystem.
      if (keyName.isEmpty()) {
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
            ozoneManager.getDefaultReplicationConfig(), ozoneManager.getConfig());

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

    markForAudit(auditLogger, buildAuditMessage(OMAction.CREATE_DIRECTORY,
        auditMap, exception, userInfo));

    logResult(createDirectoryRequest, keyArgs, omMetrics, result,
        exception, numMissingParents);

    return omClientResponse;
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
    final KeyArgs keyArgs = req.getCreateDirectoryRequest().getKeyArgs();
    OMClientRequestUtils.validateVolumeName(keyArgs.getVolumeName());
    OMClientRequestUtils.validateBucketName(keyArgs.getBucketName());

    final BucketLayout bucketLayout = ctx.getBucketLayout(
        keyArgs.getVolumeName(), keyArgs.getBucketName());
    bucketLayout.validateSupportedOperation();

    return req;
  }
}
