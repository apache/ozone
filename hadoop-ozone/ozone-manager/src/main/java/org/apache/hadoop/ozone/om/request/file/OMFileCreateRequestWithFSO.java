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

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneConfigUtil;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.file.OMFileCreateResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateFileResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles create file request layout version1.
 */
public class OMFileCreateRequestWithFSO extends OMFileCreateRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMFileCreateRequestWithFSO.class);

  public OMFileCreateRequestWithFSO(OMRequest omRequest,
                                    BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
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

    OmBucketInfo omBucketInfo = null;
    final List<OmKeyLocationInfo> locations = new ArrayList<>();
    List<OmDirectoryInfo> missingParentInfos;
    int numKeysCreated = 0;

    OMClientResponse omClientResponse = null;
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    Exception exception = null;
    Result result = null;
    try {
      if (keyName.isEmpty()) {
        // Check if this is the root of the filesystem.
        throw new OMException("Can not write to directory: " + keyName,
                OMException.ResultCodes.NOT_A_FILE);
      }

      // acquire lock
      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      final long volumeId = omMetadataManager.getVolumeId(volumeName);
      final long bucketId = omMetadataManager
              .getBucketId(volumeName, bucketName);

      OmKeyInfo dbFileInfo = null;

      OMFileRequest.OMPathInfoWithFSO pathInfoFSO =
              OMFileRequest.verifyDirectoryKeysInPath(omMetadataManager,
                      volumeName, bucketName, keyName, Paths.get(keyName));

      if (pathInfoFSO.getDirectoryResult()
              == OMFileRequest.OMDirectoryResult.FILE_EXISTS) {
        String dbFileKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
                pathInfoFSO.getLastKnownParentId(),
                pathInfoFSO.getLeafNodeName());
        dbFileInfo = OMFileRequest.getOmKeyInfoFromFileTable(false,
                omMetadataManager, dbFileKey, keyName);
      }

      // check if the file or directory already existed in OM
      checkDirectoryResult(keyName, isOverWrite,
              pathInfoFSO.getDirectoryResult());

      if (!isRecursive) {
        checkAllParentsExist(keyArgs, pathInfoFSO);
      }

      // do open key
      OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(
          omMetadataManager.getBucketKey(volumeName, bucketName));
      // add all missing parents to dir table

      missingParentInfos = getAllMissingParentDirInfo(
          ozoneManager, keyArgs, bucketInfo, pathInfoFSO, trxnLogIndex);

      // total number of keys created.
      numKeysCreated = missingParentInfos.size();

      final ReplicationConfig repConfig = OzoneConfigUtil
          .resolveReplicationConfigPreference(keyArgs.getType(),
              keyArgs.getFactor(), keyArgs.getEcReplicationConfig(),
              bucketInfo.getDefaultReplicationConfig(),
              ozoneManager);

      OmKeyInfo omFileInfo = prepareFileInfo(omMetadataManager, keyArgs,
              dbFileInfo, keyArgs.getDataSize(), locations,
              getFileEncryptionInfo(keyArgs), ozoneManager.getPrefixManager(),
              bucketInfo, pathInfoFSO, trxnLogIndex,
              pathInfoFSO.getLeafNodeObjectId(),
          repConfig, ozoneManager.getConfig());
      validateEncryptionKeyInfo(bucketInfo, keyArgs);

      long openVersion = omFileInfo.getLatestVersionLocations().getVersion();
      long clientID = createFileRequest.getClientID();
      String dbOpenFileName = omMetadataManager
          .getOpenFileName(volumeId, bucketId,
                  pathInfoFSO.getLastKnownParentId(),
                  pathInfoFSO.getLeafNodeName(), clientID);

      // Append new blocks
      List<OmKeyLocationInfo> newLocationList = keyArgs.getKeyLocationsList()
          .stream().map(OmKeyLocationInfo::getFromProtobuf)
          .collect(Collectors.toList());
      omFileInfo.appendNewBlocks(newLocationList, false);

      omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);
      // check bucket and volume quota
      long preAllocatedSpace =
          newLocationList.size() * ozoneManager.getScmBlockSize() * repConfig
              .getRequiredNodes();
      checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
          preAllocatedSpace);
      checkBucketQuotaInNamespace(omBucketInfo, numKeysCreated + 1L);
      omBucketInfo.incrUsedNamespace(numKeysCreated);

      // Add to cache entry can be done outside of lock for this openKey.
      // Even if bucket gets deleted, when commitKey we shall identify if
      // bucket gets deleted.
      OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager,
          dbOpenFileName, omFileInfo, keyName, trxnLogIndex);

      // Add cache entries for the prefix directories.
      // Skip adding for the file key itself, until Key Commit.
      OMFileRequest.addDirectoryTableCacheEntries(omMetadataManager, volumeId,
              bucketId, trxnLogIndex, missingParentInfos, null);

      // Prepare response. Sets user given full key name in the 'keyName'
      // attribute in response object.
      int clientVersion = getOmRequest().getVersion();
      omResponse.setCreateFileResponse(CreateFileResponse.newBuilder()
          .setKeyInfo(omFileInfo.getNetworkProtobuf(keyName, clientVersion,
              keyArgs.getLatestVersionLocation()))
          .setID(clientID)
          .setOpenVersion(openVersion).build())
          .setCmdType(Type.CreateFile);
      omClientResponse = new OMFileCreateResponseWithFSO(omResponse.build(),
              omFileInfo, missingParentInfos, clientID,
              omBucketInfo.copyObject(), volumeId);

      result = Result.SUCCESS;
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omMetrics.incNumCreateFileFails();
      omResponse.setCmdType(Type.CreateFile);
      omClientResponse = new OMFileCreateResponseWithFSO(createErrorOMResponse(
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
      omMetrics.incNumKeys(numKeysCreated);
      LOG.debug("File created. Volume:{}, Bucket:{}, Key:{}", volumeName,
          bucketName, keyName);
      break;
    case FAILURE:
      LOG.error("File create failed. Volume:{}, Bucket:{}, Key:{}.",
          volumeName, bucketName, keyName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMFileCreateRequest: {}",
          createFileRequest);
    }

    return omClientResponse;
  }
}
