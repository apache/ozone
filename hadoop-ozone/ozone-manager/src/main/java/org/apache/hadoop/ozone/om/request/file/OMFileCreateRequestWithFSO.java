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
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    CreateFileRequest createFileRequest = getOmRequest().getCreateFileRequest();
    KeyArgs keyArgs = createFileRequest.getKeyArgs();
    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    if (LOG.isDebugEnabled()) {
      LOG.debug("File create for : " + volumeName + "/" + bucketName + "/"
          + keyName + ":" + createFileRequest.getIsRecursive());
    }

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumCreateFile();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    boolean acquiredLock = false;
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

      PreparedFileCreate prepared =
          prepareFileCreate(ozoneManager, createFileRequest, trxnLogIndex);
      numKeysCreated = prepared.missingParentInfos.size();

      // Phase 2 (bucket write lock): re-check the overwrite invariant, then apply the cache
      // mutations and publish the quota copy. The lock is held only for this mutation tail, so
      // bucket read-lock holders still observe each transaction atomically (all mutations or none),
      // but are no longer blocked for the Phase 1 path walk.
      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();

      omClientResponse = applyFileCreate(omMetadataManager, createFileRequest,
          prepared, trxnLogIndex, omResponse);

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

    auditAndLogResult(ozoneManager, createFileRequest, auditMap, exception, result, numKeysCreated);

    return omClientResponse;
  }

  /**
   * Phase 1 (no bucket lock): resolves the path and prepares the open-file entry, the missing parent
   * directories and the quota delta. All of this reads committed state only. The OM apply path is
   * single-threaded (OzoneManagerStateMachine uses a single-thread executor), so no other transaction
   * can change what these reads observe before Phase 2 mutates; the double-buffer flush/cleanup
   * threads only materialize already-committed epochs and never alter a key's visible value.
   * <p>
   * Keeping this walk out of the bucket write lock is the point of HDDS-16289: it stops the lone apply
   * thread from gating readers of a hot bucket (getBucketInfo/getFileStatus/lookupKey) during the
   * per-segment path resolution. If OM ever applies transactions in parallel per bucket/key, these
   * reads must be re-validated under the lock in {@link #applyFileCreate}.
   */
  private PreparedFileCreate prepareFileCreate(OzoneManager ozoneManager,
      CreateFileRequest createFileRequest, long trxnLogIndex) throws IOException {
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    KeyArgs keyArgs = createFileRequest.getKeyArgs();
    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();
    // if isOverWrite is true, file would be over written.
    boolean isOverWrite = createFileRequest.getIsOverwrite();

    validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager
            .getBucketId(volumeName, bucketName);

    OmKeyInfo dbFileInfo = null;

    OMFileRequest.OMPathInfoWithFSO pathInfoFSO =
            OMFileRequest.verifyDirectoryKeysInPath(omMetadataManager,
                    volumeName, bucketName, keyName, Paths.get(keyName));

    final String dbFileKey = omMetadataManager.getOzonePathKey(volumeId,
            bucketId, pathInfoFSO.getLastKnownParentId(),
            pathInfoFSO.getLeafNodeName());
    if (pathInfoFSO.getDirectoryResult()
            == OMFileRequest.OMDirectoryResult.FILE_EXISTS) {
      dbFileInfo = OMFileRequest.getOmKeyInfoFromFileTable(false,
              omMetadataManager, dbFileKey, keyName);
    }

    // check if the file or directory already existed in OM
    checkDirectoryResult(keyName, isOverWrite,
            pathInfoFSO.getDirectoryResult());

    // if isRecursive is true, file would be created even if parent
    // directories does not exist.
    if (!createFileRequest.getIsRecursive()) {
      checkAllParentsExist(keyArgs, pathInfoFSO);
    }

    // do open key
    // Read-only copy, used here only to inherit ACLs, encryption and replication defaults. The quota
    // read-modify-publish in Phase 2 takes its own getBucketInfoForUpdate copy under the lock.
    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName));
    // add all missing parents to dir table

    List<OmDirectoryInfo> missingParentInfos = getAllMissingParentDirInfo(
        ozoneManager, keyArgs, bucketInfo, pathInfoFSO, trxnLogIndex);

    final ReplicationConfig repConfig = OzoneConfigUtil
        .resolveReplicationConfigPreference(keyArgs.getType(),
            keyArgs.getFactor(), keyArgs.getEcReplicationConfig(),
            bucketInfo.getDefaultReplicationConfig(),
            ozoneManager);

    OmKeyInfo omFileInfo = prepareFileInfo(omMetadataManager, keyArgs,
            dbFileInfo, keyArgs.getDataSize(), new ArrayList<>(),
            getFileEncryptionInfo(keyArgs), ozoneManager.getPrefixManager(),
            bucketInfo, pathInfoFSO, trxnLogIndex,
            pathInfoFSO.getLeafNodeObjectId(),
        repConfig, ozoneManager.getConfig());
    validateEncryptionKeyInfo(bucketInfo, keyArgs);

    String dbOpenFileName = omMetadataManager
        .getOpenFileName(volumeId, bucketId,
                pathInfoFSO.getLastKnownParentId(),
                pathInfoFSO.getLeafNodeName(), createFileRequest.getClientID());

    // Append new blocks
    List<OmKeyLocationInfo> newLocationList = keyArgs.getKeyLocationsList()
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());
    omFileInfo.appendNewBlocks(newLocationList, false);

    final long preAllocatedSpace =
        newLocationList.size() * ozoneManager.getScmBlockSize() * repConfig
            .getRequiredNodes();

    return new PreparedFileCreate(volumeId, bucketId, dbFileKey, omFileInfo,
        missingParentInfos, dbOpenFileName, preAllocatedSpace);
  }

  /**
   * Phase 2 (under the bucket write lock): re-checks the overwrite guard resolved in Phase 1, then
   * applies the open-file and directory cache entries and the bucket quota charge and publishes the
   * bucket copy.
   */
  private OMClientResponse applyFileCreate(OMMetadataManager omMetadataManager,
      CreateFileRequest createFileRequest, PreparedFileCreate prepared, long trxnLogIndex,
      OMResponse.Builder omResponse) throws IOException {
    KeyArgs keyArgs = createFileRequest.getKeyArgs();
    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    // Cheap O(1) re-check of the overwrite guard resolved in Phase 1. Under serial apply this always
    // holds; it is a tripwire that fails safe (as checkDirectoryResult would) rather than silently
    // overwriting if that invariant is ever broken by a concurrent writer.
    if (!createFileRequest.getIsOverwrite() && OMFileRequest.getOmKeyInfoFromFileTable(false,
        omMetadataManager, prepared.dbFileKey, keyName) != null) {
      throw new OMException("File " + keyName + " already exists",
          OMException.ResultCodes.FILE_ALREADY_EXISTS);
    }

    OmBucketInfo omBucketInfo = getBucketInfoForUpdate(omMetadataManager, volumeName, bucketName);
    // check bucket and volume quota
    checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
        prepared.preAllocatedSpace);
    final int numKeysCreated = prepared.missingParentInfos.size();
    checkBucketQuotaInNamespace(omBucketInfo, numKeysCreated + 1L);
    omBucketInfo.incrUsedNamespace(numKeysCreated);

    OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager,
        prepared.dbOpenFileName, prepared.omFileInfo, keyName, trxnLogIndex);

    // Add cache entries for the prefix directories.
    // Skip adding for the file key itself, until Key Commit.
    OMFileRequest.addDirectoryTableCacheEntries(omMetadataManager, prepared.volumeId,
            prepared.bucketId, trxnLogIndex, prepared.missingParentInfos, null);

    omMetadataManager.getBucketTable().addCacheEntry(
        omMetadataManager.getBucketKey(volumeName, bucketName), omBucketInfo, trxnLogIndex);

    // Prepare response. Sets user given full key name in the 'keyName'
    // attribute in response object.
    int clientVersion = getOmRequest().getVersion();
    long openVersion = prepared.omFileInfo.getLatestVersionLocations().getVersion();
    omResponse.setCreateFileResponse(CreateFileResponse.newBuilder()
        .setKeyInfo(prepared.omFileInfo.getNetworkProtobuf(keyName, clientVersion,
            keyArgs.getLatestVersionLocation()))
        .setID(createFileRequest.getClientID())
        .setOpenVersion(openVersion).build())
        .setCmdType(Type.CreateFile);
    return new OMFileCreateResponseWithFSO(omResponse.build(),
            prepared.omFileInfo, prepared.missingParentInfos, createFileRequest.getClientID(),
            omBucketInfo.copyObject(), prepared.volumeId);
  }

  /**
   * Phase 1 output of {@link #prepareFileCreate}, consumed by {@link #applyFileCreate} under the
   * bucket write lock: the resolved path keys, the open file and missing parent directories to cache,
   * and the quota deltas to charge.
   */
  private static final class PreparedFileCreate {
    private final long volumeId;
    private final long bucketId;
    private final String dbFileKey;
    private final OmKeyInfo omFileInfo;
    private final List<OmDirectoryInfo> missingParentInfos;
    private final String dbOpenFileName;
    private final long preAllocatedSpace;

    PreparedFileCreate(long volumeId, long bucketId, String dbFileKey, OmKeyInfo omFileInfo,
        List<OmDirectoryInfo> missingParentInfos, String dbOpenFileName, long preAllocatedSpace) {
      this.volumeId = volumeId;
      this.bucketId = bucketId;
      this.dbFileKey = dbFileKey;
      this.omFileInfo = omFileInfo;
      this.missingParentInfos = missingParentInfos;
      this.dbOpenFileName = dbOpenFileName;
      this.preAllocatedSpace = preAllocatedSpace;
    }
  }

  /**
   * Emits the audit log and the result log outside the bucket lock.
   */
  private void auditAndLogResult(OzoneManager ozoneManager, CreateFileRequest createFileRequest,
      Map<String, String> auditMap, Exception exception, Result result, int numKeysCreated) {
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.CREATE_FILE, auditMap, exception,
        getOmRequest().getUserInfo()));

    KeyArgs keyArgs = createFileRequest.getKeyArgs();
    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    switch (result) {
    case SUCCESS:
      ozoneManager.getMetrics().incNumKeys(numKeysCreated);
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
  }
}
