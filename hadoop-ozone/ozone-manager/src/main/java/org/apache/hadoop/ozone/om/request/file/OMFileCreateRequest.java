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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMReplayException;
import org.apache.hadoop.ozone.om.response.file.OMFileCreateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateFileResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .Type;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.UniqueId;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles create file request.
 */
public class OMFileCreateRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMFileCreateRequest.class);
  public OMFileCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    CreateFileRequest createFileRequest = getOmRequest().getCreateFileRequest();
    Preconditions.checkNotNull(createFileRequest);

    KeyArgs keyArgs = createFileRequest.getKeyArgs();

    if (keyArgs.getKeyName().length() == 0) {
      // Check if this is the root of the filesystem.
      // Not throwing exception here, as need to throw exception after
      // checking volume/bucket exists.
      return getOmRequest().toBuilder().setUserInfo(getUserInfo()).build();
    }

    long scmBlockSize = ozoneManager.getScmBlockSize();

    // NOTE size of a key is not a hard limit on anything, it is a value that
    // client should expect, in terms of current size of key. If client sets
    // a value, then this value is used, otherwise, we allocate a single
    // block which is the current size, if read by the client.
    final long requestedSize = keyArgs.getDataSize() > 0 ?
        keyArgs.getDataSize() : scmBlockSize;

    boolean useRatis = ozoneManager.shouldUseRatis();

    HddsProtos.ReplicationFactor factor = keyArgs.getFactor();
    if (factor == null) {
      factor = useRatis ? HddsProtos.ReplicationFactor.THREE :
          HddsProtos.ReplicationFactor.ONE;
    }

    HddsProtos.ReplicationType type = keyArgs.getType();
    if (type == null) {
      type = useRatis ? HddsProtos.ReplicationType.RATIS :
          HddsProtos.ReplicationType.STAND_ALONE;
    }

    // TODO: Here we are allocating block with out any check for
    //  bucket/key/volume or not and also with out any authorization checks.

    List< OmKeyLocationInfo > omKeyLocationInfoList =
        allocateBlock(ozoneManager.getScmClient(),
              ozoneManager.getBlockTokenSecretManager(), type, factor,
              new ExcludeList(), requestedSize, scmBlockSize,
              ozoneManager.getPreallocateBlocksMax(),
              ozoneManager.isGrpcBlockTokenEnabled(),
              ozoneManager.getOMNodeId());

    KeyArgs.Builder newKeyArgs = keyArgs.toBuilder()
        .setModificationTime(Time.now()).setType(type).setFactor(factor)
        .setDataSize(requestedSize);

    newKeyArgs.addAllKeyLocations(omKeyLocationInfoList.stream()
        .map(OmKeyLocationInfo::getProtobuf).collect(Collectors.toList()));

    CreateFileRequest.Builder newCreateFileRequest =
        createFileRequest.toBuilder().setKeyArgs(newKeyArgs)
            .setClientID(UniqueId.next());

    return getOmRequest().toBuilder()
        .setCreateFileRequest(newCreateFileRequest).setUserInfo(getUserInfo())
        .build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    CreateFileRequest createFileRequest = getOmRequest().getCreateFileRequest();
    KeyArgs keyArgs = createFileRequest.getKeyArgs();

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

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
    Optional<FileEncryptionInfo> encryptionInfo = Optional.absent();
    OmKeyInfo omKeyInfo = null;
    final List<OmKeyLocationInfo> locations = new ArrayList<>();
    List<OmKeyInfo> missingParentInfos;

    OMClientResponse omClientResponse = null;
    OMResponse.Builder omResponse = OMResponse.newBuilder()
        .setCmdType(Type.CreateFile)
        .setStatus(Status.OK);
    IOException exception = null;
    Result result = null;
    try {
      // check Acl
      checkKeyAcls(ozoneManager, volumeName, bucketName, keyName,
          IAccessAuthorizer.ACLType.CREATE, OzoneObj.ResourceType.KEY);

      // acquire lock
      acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
          volumeName, bucketName);

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      if (keyName.length() == 0) {
        // Check if this is the root of the filesystem.
        throw new OMException("Can not write to directory: " + keyName,
            OMException.ResultCodes.NOT_A_FILE);
      }

      // Check if Key already exists in KeyTable and this transaction is a
      // replay.
      String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
          keyName);
      OmKeyInfo dbKeyInfo = omMetadataManager.getKeyTable()
          .getIfExist(ozoneKey);
      if (dbKeyInfo != null) {
        // Check if this transaction is a replay of ratis logs.
        // We check only the KeyTable here and not the OpenKeyTable. In case
        // this transaction is a replay but the transaction was not committed
        // to the KeyTable, then we recreate the key in OpenKey table. This is
        // okay as all the subsequent transactions would also be replayed and
        // the openKey table would eventually reach the same state.
        // The reason we do not check the OpenKey table is to avoid a DB read
        // in regular non-replay scenario.
        if (isReplay(ozoneManager, dbKeyInfo, trxnLogIndex)) {
          // Replay implies the response has already been returned to
          // the client. So take no further action and return a dummy response.
          throw new OMReplayException();
        }
      }

      OMFileRequest.OMPathInfo pathInfo =
          OMFileRequest.verifyFilesInPath(omMetadataManager, volumeName,
              bucketName, keyName, Paths.get(keyName));
      OMFileRequest.OMDirectoryResult omDirectoryResult =
          pathInfo.getDirectoryResult();
      List<OzoneAcl> inheritAcls = pathInfo.getAcls();

      // Check if a file or directory exists with same key name.
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

      if (!isRecursive) {
        checkAllParentsExist(ozoneManager, keyArgs, pathInfo);
      }

      // do open key
      OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(
          omMetadataManager.getBucketKey(volumeName, bucketName));
      encryptionInfo = getFileEncryptionInfo(ozoneManager, bucketInfo);

      omKeyInfo = prepareKeyInfo(omMetadataManager, keyArgs, dbKeyInfo,
          keyArgs.getDataSize(), locations, encryptionInfo.orNull(),
          ozoneManager.getPrefixManager(), bucketInfo, trxnLogIndex,
          ozoneManager.isRatisEnabled());

      long openVersion = omKeyInfo.getLatestVersionLocations().getVersion();
      long clientID = createFileRequest.getClientID();
      String dbOpenKeyName = omMetadataManager.getOpenKey(volumeName,
          bucketName, keyName, clientID);

      missingParentInfos = OMDirectoryCreateRequest
          .getAllParentInfo(ozoneManager, keyArgs,
              pathInfo.getMissingParents(), inheritAcls, trxnLogIndex);

      // Append new blocks
      omKeyInfo.appendNewBlocks(keyArgs.getKeyLocationsList().stream()
          .map(OmKeyLocationInfo::getFromProtobuf)
          .collect(Collectors.toList()), false);

      // Add to cache entry can be done outside of lock for this openKey.
      // Even if bucket gets deleted, when commitKey we shall identify if
      // bucket gets deleted.
      omMetadataManager.getOpenKeyTable().addCacheEntry(
          new CacheKey<>(dbOpenKeyName),
          new CacheValue<>(Optional.of(omKeyInfo), trxnLogIndex));

      // Add cache entries for the prefix directories.
      // Skip adding for the file key itself, until Key Commit.
      OMFileRequest.addKeyTableCacheEntries(omMetadataManager, volumeName,
          bucketName, Optional.absent(), Optional.of(missingParentInfos),
          trxnLogIndex);

      // Prepare response
      omResponse.setCreateFileResponse(CreateFileResponse.newBuilder()
          .setKeyInfo(omKeyInfo.getProtobuf())
          .setID(clientID)
          .setOpenVersion(openVersion).build())
          .setCmdType(Type.CreateFile);
      omClientResponse = new OMFileCreateResponse(omResponse.build(),
          omKeyInfo, missingParentInfos, clientID);

      result = Result.SUCCESS;
    } catch (IOException ex) {
      if (ex instanceof OMReplayException) {
        result = Result.REPLAY;
        omClientResponse = new OMFileCreateResponse(createReplayOMResponse(
            omResponse));
      } else {
        result = Result.FAILURE;
        exception = ex;
        omMetrics.incNumCreateFileFails();
        omResponse.setCmdType(Type.CreateFile);
        omClientResponse = new OMFileCreateResponse(createErrorOMResponse(
            omResponse, exception));
      }
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    // Audit Log outside the lock
    if (result != Result.REPLAY) {
      Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
      auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
          OMAction.CREATE_FILE, auditMap, exception,
          getOmRequest().getUserInfo()));
    }

    switch (result) {
    case REPLAY:
      LOG.debug("Replayed Transaction {} ignored. Request: {}", trxnLogIndex,
          createFileRequest);
      break;
    case SUCCESS:
      LOG.debug("File created. Volume:{}, Bucket:{}, Key:{}", volumeName,
          bucketName, keyName);
      break;
    case FAILURE:
      LOG.error("File create failed. Volume:{}, Bucket:{}, Key{}. Exception:{}",
          volumeName, bucketName, keyName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMFileCreateRequest: {}",
          createFileRequest);
    }

    return omClientResponse;
  }

  private void checkAllParentsExist(OzoneManager ozoneManager,
      KeyArgs keyArgs,
      OMFileRequest.OMPathInfo pathInfo) throws IOException {
    String keyName = keyArgs.getKeyName();

    // if immediate parent exists, assume higher level directories exist.
    if (!pathInfo.directParentExists()) {
      throw new OMException("Cannot create file : " + keyName
          + " as one of parent directory is not created",
          OMException.ResultCodes.DIRECTORY_NOT_FOUND);
    }
  }
}
