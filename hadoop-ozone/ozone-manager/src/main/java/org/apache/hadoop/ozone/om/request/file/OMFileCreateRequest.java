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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.file.OMFileCreateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateFileResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.UniqueId;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS;

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

    // Verify key name
    final boolean checkKeyNameEnabled = ozoneManager.getConfiguration()
         .getBoolean(OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_KEY,
                 OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_DEFAULT);
    if(checkKeyNameEnabled){
      OmUtils.validateKeyName(StringUtils.removeEnd(keyArgs.getKeyName(),
              OzoneConsts.FS_FILE_COPYING_TEMP_SUFFIX));
    }

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
        .map(info -> info.getProtobuf(getOmRequest().getVersion()))
        .collect(Collectors.toList()));

    generateRequiredEncryptionInfo(keyArgs, newKeyArgs, ozoneManager);
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
    IOException exception = null;
    Result result = null;
    try {
      keyArgs = resolveBucketLink(ozoneManager, keyArgs, auditMap);
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();

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

      String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
          keyName);
      OmKeyInfo dbKeyInfo = omMetadataManager.getKeyTable()
          .getIfExist(ozoneKey);

      if (dbKeyInfo != null) {
        ozoneManager.getKeyManager().refresh(dbKeyInfo);
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

      omKeyInfo = prepareKeyInfo(omMetadataManager, keyArgs, dbKeyInfo,
          keyArgs.getDataSize(), locations, getFileEncryptionInfo(keyArgs),
          ozoneManager.getPrefixManager(), bucketInfo, trxnLogIndex,
          ozoneManager.getObjectIdFromTxId(trxnLogIndex),
          ozoneManager.isRatisEnabled());

      long openVersion = omKeyInfo.getLatestVersionLocations().getVersion();
      long clientID = createFileRequest.getClientID();
      String dbOpenKeyName = omMetadataManager.getOpenKey(volumeName,
          bucketName, keyName, clientID);

      missingParentInfos = OMDirectoryCreateRequest
          .getAllParentInfo(ozoneManager, keyArgs,
              pathInfo.getMissingParents(), inheritAcls, trxnLogIndex);

      // Append new blocks
      List<OmKeyLocationInfo> newLocationList = keyArgs.getKeyLocationsList()
          .stream().map(OmKeyLocationInfo::getFromProtobuf)
          .collect(Collectors.toList());
      omKeyInfo.appendNewBlocks(newLocationList, false);

      omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);
      // check bucket and volume quota
      long preAllocatedSpace = newLocationList.size()
          * ozoneManager.getScmBlockSize()
          * omKeyInfo.getFactor().getNumber();
      checkBucketQuotaInBytes(omBucketInfo, preAllocatedSpace);
      checkBucketQuotaInNamespace(omBucketInfo, 1L);

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

      omBucketInfo.incrUsedBytes(preAllocatedSpace);
      // Update namespace quota
      omBucketInfo.incrUsedNamespace(1L);

      numMissingParents = missingParentInfos.size();
      // Prepare response
      omResponse.setCreateFileResponse(CreateFileResponse.newBuilder()
          .setKeyInfo(omKeyInfo.getProtobuf(getOmRequest().getVersion()))
          .setID(clientID)
          .setOpenVersion(openVersion).build())
          .setCmdType(Type.CreateFile);
      omClientResponse = new OMFileCreateResponse(omResponse.build(),
          omKeyInfo, missingParentInfos, clientID, omBucketInfo.copyObject());

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omMetrics.incNumCreateFileFails();
      omResponse.setCmdType(Type.CreateFile);
      omClientResponse = new OMFileCreateResponse(createErrorOMResponse(
            omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    // Audit Log outside the lock
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
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
