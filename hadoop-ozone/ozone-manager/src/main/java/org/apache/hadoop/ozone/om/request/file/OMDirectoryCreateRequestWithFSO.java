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
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.file.OMDirectoryCreateResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDirectoryRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDirectoryResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle create directory request. It will add path components to the directory
 * table and maintains file system semantics.
 */
public class OMDirectoryCreateRequestWithFSO extends OMDirectoryCreateRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDirectoryCreateRequestWithFSO.class);

  public OMDirectoryCreateRequestWithFSO(OMRequest omRequest,
                                         BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
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
    int numKeysCreated = 0;

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    omResponse.setCreateDirectoryResponse(CreateDirectoryResponse.newBuilder());
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumCreateDirectory();

    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquiredLock = false;
    Exception exception = null;
    OMClientResponse omClientResponse = null;
    Result result = Result.FAILURE;

    try {
      // Check if this is the root of the filesystem.
      if (keyName.isEmpty()) {
        throw new OMException("Directory create failed. Cannot create " +
            "directory at root of the filesystem",
            OMException.ResultCodes.CANNOT_CREATE_DIRECTORY_AT_ROOT);
      }

      PreparedDirCreate prepared =
          prepareDirectoryCreate(ozoneManager, keyArgs, trxnLogIndex);
      if (prepared == null) {
        // The directory already exists: nothing to mutate, so no bucket lock is taken at all.
        result = Result.DIRECTORY_ALREADY_EXISTS;
        omResponse.setStatus(Status.DIRECTORY_ALREADY_EXISTS);
        omClientResponse =
            new OMDirectoryCreateResponseWithFSO(omResponse.build(), result);
      } else {
        numKeysCreated = prepared.numKeysCreated;

        // Phase 2 (bucket write lock): re-check the leaf, then apply the cache mutations and publish
        // the quota copy. The lock is held only for this mutation tail, so bucket read-lock holders
        // still observe each transaction atomically (all mutations or none), but are no longer
        // blocked for the Phase 1 path walk.
        mergeOmLockDetails(
            omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
                bucketName));
        acquiredLock = getOmLockDetails().isLockAcquired();

        omClientResponse = applyDirectoryCreate(omMetadataManager, keyArgs,
            prepared, trxnLogIndex, omResponse);
        result = OMDirectoryCreateRequest.Result.SUCCESS;
      }
    } catch (IOException | InvalidPathException ex) {
      exception = ex;
      omClientResponse = new OMDirectoryCreateResponseWithFSO(
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

    auditAndLogResult(ozoneManager, createDirectoryRequest, auditMap, exception, result, numKeysCreated);

    return omClientResponse;
  }

  /**
   * Phase 1 (no bucket lock): resolves the path and prepares the leaf directory, the missing parent
   * directories and the namespace delta. All of this reads committed state only. The OM apply path is
   * single-threaded (OzoneManagerStateMachine uses a single-thread executor), so no other transaction
   * can change what these reads observe before Phase 2 mutates; the double-buffer flush/cleanup
   * threads only materialize already-committed epochs and never alter a key's visible value.
   * <p>
   * Keeping this walk out of the bucket write lock is the point of HDDS-16289: it stops the lone apply
   * thread from gating readers of a hot bucket (getBucketInfo/getFileStatus/lookupKey) during the
   * per-segment path resolution. If OM ever applies transactions in parallel per bucket/key, these
   * reads must be re-validated under the lock in {@link #applyDirectoryCreate}.
   *
   * @return the prepared directory, or {@code null} when the directory already exists, in which case
   *         the caller reports DIRECTORY_ALREADY_EXISTS without taking the lock
   */
  private PreparedDirCreate prepareDirectoryCreate(OzoneManager ozoneManager,
      KeyArgs keyArgs, long trxnLogIndex) throws IOException {
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

    Path keyPath = Paths.get(keyName);

    // Need to check if any files exist in the given path, if they exist we
    // cannot create a directory with the given key.
    // Verify the path against directory table
    OMFileRequest.OMPathInfoWithFSO omPathInfo =
        OMFileRequest.verifyDirectoryKeysInPath(omMetadataManager, volumeName,
            bucketName, keyName, keyPath);
    OMFileRequest.OMDirectoryResult omDirectoryResult =
        omPathInfo.getDirectoryResult();

    if (omDirectoryResult == FILE_EXISTS ||
        omDirectoryResult == FILE_EXISTS_IN_GIVENPATH) {
      throw new OMException("Unable to create directory: " + keyName
          + " in volume/bucket: " + volumeName + "/" + bucketName + " as " +
              "file:" + omPathInfo.getFileExistsInPath() + " already exists",
          FILE_ALREADY_EXISTS);
    }
    if (omDirectoryResult != DIRECTORY_EXISTS_IN_GIVENPATH &&
        omDirectoryResult != NONE) {
      return null;
    }

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager
            .getBucketId(volumeName, bucketName);

    // Read-only copy, used here only to inherit ACLs. The quota read-modify-publish in Phase 2 takes
    // its own getBucketInfoForUpdate copy under the lock.
    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable()
        .get(omMetadataManager.getBucketKey(volumeName, bucketName));
    // prepare all missing parents
    List<OmDirectoryInfo> missingParentInfos = getAllMissingParentDirInfo(
            ozoneManager, keyArgs, bucketInfo, omPathInfo, trxnLogIndex);

    // prepare leafNode dir. Keep after getAllMissingParentDirInfo, which sets the leaf node object id
    // and the last known parent id on omPathInfo. This can still fail with UNAUTHORIZED, so it must
    // precede the bucket publish; running it in Phase 1 assures that.
    OmDirectoryInfo dirInfo = createDirectoryInfoWithACL(
        omPathInfo.getLeafNodeName(),
        keyArgs, omPathInfo.getLeafNodeObjectId(),
        omPathInfo.getLastKnownParentId(), trxnLogIndex,
        bucketInfo, omPathInfo, ozoneManager.getConfig());

    // total number of keys created.
    return new PreparedDirCreate(volumeId, bucketId, omPathInfo, missingParentInfos, dirInfo,
        missingParentInfos.size() + 1);
  }

  /**
   * Phase 2 (under the bucket write lock): re-checks the leaf resolved in Phase 1, then applies the
   * directory-table cache entries and the bucket namespace increment and publishes the bucket copy.
   */
  private OMClientResponse applyDirectoryCreate(OMMetadataManager omMetadataManager,
      KeyArgs keyArgs, PreparedDirCreate prepared, long trxnLogIndex,
      OMResponse.Builder omResponse) throws IOException {
    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    // Cheap O(1) re-check at the leaf of what the Phase 1 walk resolved. Under serial apply this
    // always holds; it is a tripwire that fails rather than creating a duplicate if that invariant is
    // ever broken by a concurrent writer.
    final String dbLeafKey = omMetadataManager.getOzonePathKey(prepared.volumeId,
        prepared.bucketId, prepared.omPathInfo.getLastKnownParentId(),
        prepared.omPathInfo.getLeafNodeName());
    if (omMetadataManager.getDirectoryTable().get(dbLeafKey) != null) {
      throw new OMException("Unable to create directory: " + keyName
          + " in volume/bucket: " + volumeName + "/" + bucketName + " as it already exists",
          OMException.ResultCodes.DIRECTORY_ALREADY_EXISTS);
    }
    if (omMetadataManager.getKeyTable(getBucketLayout()).get(dbLeafKey) != null) {
      throw new OMException("Unable to create directory: " + keyName
          + " in volume/bucket: " + volumeName + "/" + bucketName
          + " as a file already exists at that path", FILE_ALREADY_EXISTS);
    }

    OmBucketInfo omBucketInfo =
        getBucketInfoForUpdate(omMetadataManager, volumeName, bucketName);
    checkBucketQuotaInNamespace(omBucketInfo, prepared.numKeysCreated);
    omBucketInfo.incrUsedNamespace(prepared.numKeysCreated);

    OMFileRequest.addDirectoryTableCacheEntries(omMetadataManager,
        prepared.volumeId, prepared.bucketId, trxnLogIndex,
        prepared.missingParentInfos, prepared.dirInfo);

    omMetadataManager.getBucketTable().addCacheEntry(
        omMetadataManager.getBucketKey(volumeName, bucketName), omBucketInfo, trxnLogIndex);

    return new OMDirectoryCreateResponseWithFSO(omResponse.build(),
        prepared.volumeId, prepared.bucketId, prepared.dirInfo,
        prepared.missingParentInfos, Result.SUCCESS,
        getBucketLayout(), omBucketInfo.copyObject());
  }

  /**
   * Phase 1 output of {@link #prepareDirectoryCreate}, consumed by {@link #applyDirectoryCreate}
   * under the bucket write lock: the resolved path, the leaf and missing parent directories to cache
   * and the namespace delta to charge.
   */
  private static final class PreparedDirCreate {
    private final long volumeId;
    private final long bucketId;
    private final OMFileRequest.OMPathInfoWithFSO omPathInfo;
    private final List<OmDirectoryInfo> missingParentInfos;
    private final OmDirectoryInfo dirInfo;
    private final int numKeysCreated;

    PreparedDirCreate(long volumeId, long bucketId, OMFileRequest.OMPathInfoWithFSO omPathInfo,
        List<OmDirectoryInfo> missingParentInfos, OmDirectoryInfo dirInfo, int numKeysCreated) {
      this.volumeId = volumeId;
      this.bucketId = bucketId;
      this.omPathInfo = omPathInfo;
      this.missingParentInfos = missingParentInfos;
      this.dirInfo = dirInfo;
      this.numKeysCreated = numKeysCreated;
    }
  }

  /**
   * Emits the audit log and the result log outside the bucket lock.
   */
  private void auditAndLogResult(OzoneManager ozoneManager, CreateDirectoryRequest createDirectoryRequest,
      Map<String, String> auditMap, Exception exception, Result result, int numKeys) {
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(OMAction.CREATE_DIRECTORY,
        auditMap, exception, getOmRequest().getUserInfo()));

    KeyArgs keyArgs = createDirectoryRequest.getKeyArgs();
    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();
    OMMetrics omMetrics = ozoneManager.getMetrics();

    switch (result) {
    case SUCCESS:
      omMetrics.incNumKeys(numKeys);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Directory created. Volume:{}, Bucket:{}, Key:{}",
            volumeName, bucketName, keyName);
      }
      break;
    case DIRECTORY_ALREADY_EXISTS:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Directory already exists. Volume:{}, Bucket:{}, Key:{}",
            volumeName, bucketName, keyName, exception);
      }
      break;
    case FAILURE:
      omMetrics.incNumCreateDirectoryFails();
      LOG.error("Directory creation failed. Volume:{}, Bucket:{}, Key:{}. " +
          "Exception:{}", volumeName, bucketName, keyName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMDirectoryCreateRequest: {}",
          createDirectoryRequest);
    }
  }
}
