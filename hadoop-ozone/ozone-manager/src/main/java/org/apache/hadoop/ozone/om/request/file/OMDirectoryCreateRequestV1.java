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

import com.google.common.base.Optional;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.file.OMDirectoryCreateResponseV1;
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
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_KEY_NAME;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.*;

/**
 * Handle create directory request. It will add path components to the directory
 * table and maintains file system semantics.
 */
public class OMDirectoryCreateRequestV1 extends OMDirectoryCreateRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDirectoryCreateRequestV1.class);

  public OMDirectoryCreateRequestV1(OMRequest omRequest) {
    super(omRequest);
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
    int numKeysCreated = 0;

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
    List<OmDirectoryInfo> missingParentInfos;

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
      acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
          volumeName, bucketName);

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      Path keyPath = Paths.get(keyName);

      // Need to check if any files exist in the given path, if they exist we
      // cannot create a directory with the given key.
      // Verify the path against directory table
      OMFileRequest.OMPathInfoV1 omPathInfo =
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
      } else if (omDirectoryResult == DIRECTORY_EXISTS_IN_GIVENPATH ||
          omDirectoryResult == NONE) {

        // prepare all missing parents
        missingParentInfos =
                OMDirectoryCreateRequestV1.getAllMissingParentDirInfo(
                        ozoneManager, keyArgs, omPathInfo, trxnLogIndex);

        // prepare leafNode dir
        OmDirectoryInfo dirInfo = createDirectoryInfoWithACL(
                omPathInfo.getLeafNodeName(),
                keyArgs, omPathInfo.getLeafNodeObjectId(),
                omPathInfo.getLastKnownParentId(), trxnLogIndex,
                OzoneAclUtil.fromProtobuf(keyArgs.getAclsList()));
        OMFileRequest.addDirectoryTableCacheEntries(omMetadataManager,
                Optional.of(dirInfo), Optional.of(missingParentInfos),
                trxnLogIndex);

        // total number of keys created.
        numKeysCreated = missingParentInfos.size() + 1;

        result = OMDirectoryCreateRequest.Result.SUCCESS;
        omClientResponse = new OMDirectoryCreateResponseV1(omResponse.build(),
                dirInfo, missingParentInfos, result);
      } else {
        result = Result.DIRECTORY_ALREADY_EXISTS;
        omResponse.setStatus(Status.DIRECTORY_ALREADY_EXISTS);
        omClientResponse = new OMDirectoryCreateResponseV1(omResponse.build(),
            result);
      }
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMDirectoryCreateResponseV1(
          createErrorOMResponse(omResponse, exception), result);
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    auditLog(auditLogger, buildAuditMessage(OMAction.CREATE_DIRECTORY,
        auditMap, exception, userInfo));

    logResult(createDirectoryRequest, keyArgs, omMetrics, numKeysCreated,
            result, exception);

    return omClientResponse;
  }

  private void logResult(CreateDirectoryRequest createDirectoryRequest,
                         KeyArgs keyArgs, OMMetrics omMetrics, int numKeys,
                         Result result,
                         IOException exception) {

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

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
   * Construct OmDirectoryInfo for every parent directory in missing list.
   *
   * @param ozoneManager Ozone Manager
   * @param keyArgs      key arguments
   * @param pathInfo     list of parent directories to be created and its ACLs
   * @param trxnLogIndex transaction log index id
   * @return list of missing parent directories
   * @throws IOException DB failure
   */
  public static List<OmDirectoryInfo> getAllMissingParentDirInfo(
          OzoneManager ozoneManager, KeyArgs keyArgs,
          OMFileRequest.OMPathInfoV1 pathInfo, long trxnLogIndex)
          throws IOException {
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    List<OmDirectoryInfo> missingParentInfos = new ArrayList<>();

    // The base id is left shifted by 8 bits for creating space to
    // create (2^8 - 1) object ids in every request.
    // maxObjId represents the largest object id allocation possible inside
    // the transaction.
    long baseObjId = ozoneManager.getObjectIdFromTxId(trxnLogIndex);
    long maxObjId = baseObjId + getMaxNumOfRecursiveDirs();
    long objectCount = 1;

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    long lastKnownParentId = pathInfo.getLastKnownParentId();
    List<String> missingParents = pathInfo.getMissingParents();
    List<OzoneAcl> inheritAcls = pathInfo.getAcls();
    for (String missingKey : missingParents) {
      long nextObjId = baseObjId + objectCount;
      if (nextObjId > maxObjId) {
        throw new OMException("Too many directories in path. Exceeds limit of "
            + getMaxNumOfRecursiveDirs() + ". Unable to create directory: "
            + keyName + " in volume/bucket: " + volumeName + "/" + bucketName,
            INVALID_KEY_NAME);
      }

      LOG.debug("missing parent {} getting added to DirectoryTable",
              missingKey);
      OmDirectoryInfo dirInfo = createDirectoryInfoWithACL(missingKey,
              keyArgs, nextObjId, lastKnownParentId, trxnLogIndex, inheritAcls);
      objectCount++;

      missingParentInfos.add(dirInfo);

      // updating id for the next sub-dir
      lastKnownParentId = nextObjId;
    }
    pathInfo.setLastKnownParentId(lastKnownParentId);
    pathInfo.setLeafNodeObjectId(baseObjId + objectCount);
    return missingParentInfos;
  }

  /**
   * Fill in a DirectoryInfo for a new directory entry in OM database.
   * without initializing ACLs from the KeyArgs - used for intermediate
   * directories which get created internally/recursively during file
   * and directory create.
   * @param dirName
   * @param keyArgs
   * @param objectId
   * @param parentObjectId
   * @param inheritAcls
   * @return the OmDirectoryInfo structure
   */
  public static OmDirectoryInfo createDirectoryInfoWithACL(
          String dirName, KeyArgs keyArgs, long objectId,
          long parentObjectId, long transactionIndex,
          List<OzoneAcl> inheritAcls) {

    return OmDirectoryInfo.newBuilder()
            .setName(dirName)
            .setCreationTime(keyArgs.getModificationTime())
            .setModificationTime(keyArgs.getModificationTime())
            .setObjectID(objectId)
            .setUpdateID(transactionIndex)
            .setParentObjectID(parentObjectId)
            .setAcls(inheritAcls).build();
  }
}
