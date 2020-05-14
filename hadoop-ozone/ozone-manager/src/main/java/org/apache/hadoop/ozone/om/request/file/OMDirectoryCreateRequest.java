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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMReplayException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
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
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

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

  /**
   * Stores the result of request execution in
   * OMClientRequest#validateAndUpdateCache.
   */
  public enum Result {
    SUCCESS, // The request was executed successfully

    REPLAY, // The request is a replay and was ignored

    DIRECTORY_ALREADY_EXISTS, // Directory key already exists in DB

    FAILURE // The request failed and exception was thrown
  }

  public OMDirectoryCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) {
    CreateDirectoryRequest createDirectoryRequest =
        getOmRequest().getCreateDirectoryRequest();
    Preconditions.checkNotNull(createDirectoryRequest);

    KeyArgs.Builder newKeyArgs = createDirectoryRequest.getKeyArgs()
        .toBuilder().setModificationTime(Time.now());

    CreateDirectoryRequest.Builder newCreateDirectoryRequest =
        createDirectoryRequest.toBuilder().setKeyArgs(newKeyArgs);

    return getOmRequest().toBuilder().setCreateDirectoryRequest(
        newCreateDirectoryRequest).setUserInfo(getUserInfo()).build();

  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

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
    IOException exception = null;
    OMClientResponse omClientResponse = null;
    Result result = Result.FAILURE;
    List<OmKeyInfo> missingParentInfos;

    try {
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
      acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
          volumeName, bucketName);

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
        throw new OMException("Unable to create directory: " +keyName
            + " in volume/bucket: " + volumeName + "/" + bucketName,
            FILE_ALREADY_EXISTS);
      } else if (omDirectoryResult == DIRECTORY_EXISTS_IN_GIVENPATH ||
          omDirectoryResult == NONE) {
        List<String> missingParents = omPathInfo.getMissingParents();
        long baseObjId = OMFileRequest.getObjIDFromTxId(trxnLogIndex);
        List<OzoneAcl> inheritAcls = omPathInfo.getAcls();

        dirKeyInfo = createDirectoryKeyInfoWithACL(keyName,
            keyArgs, baseObjId,
            OzoneAclUtil.fromProtobuf(keyArgs.getAclsList()), trxnLogIndex);

        missingParentInfos = getAllParentInfo(ozoneManager, keyArgs,
            missingParents, inheritAcls, trxnLogIndex);

        OMFileRequest.addKeyTableCacheEntries(omMetadataManager, volumeName,
            bucketName, Optional.of(dirKeyInfo),
            Optional.of(missingParentInfos), trxnLogIndex);

        omClientResponse = new OMDirectoryCreateResponse(omResponse.build(),
            dirKeyInfo, missingParentInfos);
        result = Result.SUCCESS;
      } else {
        // omDirectoryResult == DIRECTORY_EXITS
        // Check if this is a replay of ratis logs
        String dirKey = omMetadataManager.getOzoneDirKey(volumeName,
            bucketName, keyName);
        OmKeyInfo dbKeyInfo = omMetadataManager.getKeyTable().get(dirKey);
        if (isReplay(ozoneManager, dbKeyInfo, trxnLogIndex)) {
          throw new OMReplayException();
        } else {
          result = Result.DIRECTORY_ALREADY_EXISTS;
          omResponse.setStatus(Status.DIRECTORY_ALREADY_EXISTS);
          omClientResponse = new OMDirectoryCreateResponse(omResponse.build());
        }
      }
    } catch (IOException ex) {
      if (ex instanceof OMReplayException) {
        result = Result.REPLAY;
        omClientResponse = new OMDirectoryCreateResponse(
            createReplayOMResponse(omResponse));
      } else {
        exception = ex;
        omClientResponse = new OMDirectoryCreateResponse(
            createErrorOMResponse(omResponse, exception));
      }
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    if (result != Result.REPLAY) {
      auditLog(auditLogger, buildAuditMessage(OMAction.CREATE_DIRECTORY,
          auditMap, exception, userInfo));
    }

    logResult(createDirectoryRequest, keyArgs, omMetrics, result, trxnLogIndex,
        exception);

    return omClientResponse;
  }

  /**
   * Construct OmKeyInfo for every parent directory in missing list.
   * @param ozoneManager
   * @param keyArgs
   * @param missingParents list of parent directories to be created
   * @param inheritAcls ACLs to be assigned to each new parent dir
   * @param trxnLogIndex
   * @return
   * @throws IOException
   */
  public static List<OmKeyInfo> getAllParentInfo(OzoneManager ozoneManager,
      KeyArgs keyArgs, List<String> missingParents, List<OzoneAcl> inheritAcls,
      long trxnLogIndex) throws IOException {
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    List<OmKeyInfo> missingParentInfos = new ArrayList<>();

    ImmutablePair<Long, Long> objIdRange = OMFileRequest
        .getObjIdRangeFromTxId(trxnLogIndex);
    long baseObjId = objIdRange.getLeft();
    long maxObjId = objIdRange.getRight();
    long maxLevels = maxObjId - baseObjId;
    long objectCount = 1; // baseObjID is used by the leaf directory

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    for (String missingKey : missingParents) {
      long nextObjId = baseObjId + objectCount;
      if (nextObjId > maxObjId) {
        throw new OMException("Too many directories in path. Exceeds limit of "
            + maxLevels + ". Unable to create directory: " + keyName
            + " in volume/bucket: " + volumeName + "/" + bucketName,
            INVALID_KEY_NAME);
      }

      LOG.debug("missing parent {} getting added to KeyTable", missingKey);
      // what about keyArgs for parent directories? TODO
      OmKeyInfo parentKeyInfo = createDirectoryKeyInfoWithACL(
          missingKey, keyArgs, nextObjId, inheritAcls, trxnLogIndex);
      objectCount++;

      missingParentInfos.add(parentKeyInfo);
      omMetadataManager.getKeyTable().addCacheEntry(
          new CacheKey<>(omMetadataManager.getOzoneKey(volumeName,
              bucketName, parentKeyInfo.getKeyName())),
          new CacheValue<>(Optional.of(parentKeyInfo),
              trxnLogIndex));
    }

    return missingParentInfos;
  }

  private void logResult(CreateDirectoryRequest createDirectoryRequest,
      KeyArgs keyArgs, OMMetrics omMetrics, Result result, long trxnLogIndex,
      IOException exception) {

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    switch (result) {
    case SUCCESS:
      omMetrics.incNumKeys();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Directory created. Volume:{}, Bucket:{}, Key:{}",
            volumeName, bucketName, keyName);
      }
      break;
    case REPLAY:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Replayed Transaction {} ignored. Request: {}", trxnLogIndex,
            createDirectoryRequest);
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
   * @param transactionIndex
   * @return the OmKeyInfo structure
   */
  public static OmKeyInfo createDirectoryKeyInfoWithACL(
      String keyName, KeyArgs keyArgs, long objectId,
      List<OzoneAcl> inheritAcls, long transactionIndex) {
    return dirKeyInfoBuilderNoACL(keyName, keyArgs, objectId)
        .setAcls(inheritAcls).setUpdateID(transactionIndex).build();
  }

  private static OmKeyInfo.Builder dirKeyInfoBuilderNoACL(String keyName,
      KeyArgs keyArgs, long objectId) {
    String dirName = OzoneFSUtils.addTrailingSlashIfNeeded(keyName);

    return new OmKeyInfo.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(dirName)
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())))
        .setCreationTime(keyArgs.getModificationTime())
        .setModificationTime(keyArgs.getModificationTime())
        .setDataSize(0)
        .setReplicationType(HddsProtos.ReplicationType.RATIS)
        .setReplication(1)
        .setObjectID(objectId)
        .setUpdateID(objectId);
  }

}
