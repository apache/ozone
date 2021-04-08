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

package org.apache.hadoop.ozone.om.request.key;

import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponseV1;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles CommitKey request layout version V1.
 */
public class OMKeyCommitRequestV1 extends OMKeyCommitRequest {

  private static final Logger LOG =
          LoggerFactory.getLogger(OMKeyCommitRequestV1.class);

  public OMKeyCommitRequestV1(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    CommitKeyRequest commitKeyRequest = getOmRequest().getCommitKeyRequest();

    KeyArgs commitKeyArgs = commitKeyRequest.getKeyArgs();

    String volumeName = commitKeyArgs.getVolumeName();
    String bucketName = commitKeyArgs.getBucketName();
    String keyName = commitKeyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyCommits();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    Map<String, String> auditMap = buildKeyArgsAuditMap(commitKeyArgs);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
            getOmRequest());

    IOException exception = null;
    OmKeyInfo omKeyInfo = null;
    OmVolumeArgs omVolumeArgs = null;
    OmBucketInfo omBucketInfo = null;
    OMClientResponse omClientResponse = null;
    boolean bucketLockAcquired = false;
    Result result;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    try {
      commitKeyArgs = resolveBucketLink(ozoneManager, commitKeyArgs, auditMap);
      volumeName = commitKeyArgs.getVolumeName();
      bucketName = commitKeyArgs.getBucketName();

      // check Acl
      checkKeyAclsInOpenKeyTable(ozoneManager, volumeName, bucketName,
              keyName, IAccessAuthorizer.ACLType.WRITE,
              commitKeyRequest.getClientID());


      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
      Iterator<Path> pathComponents = Paths.get(keyName).iterator();
      String dbOpenFileKey = null;

      List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
      for (KeyLocation keyLocation : commitKeyArgs.getKeyLocationsList()) {
        locationInfoList.add(OmKeyLocationInfo.getFromProtobuf(keyLocation));
      }

      bucketLockAcquired =
              omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
                      volumeName, bucketName);

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      String fileName = OzoneFSUtils.getFileName(keyName);
      omBucketInfo = omMetadataManager.getBucketTable().get(bucketKey);
      long bucketId = omBucketInfo.getObjectID();
      long parentID = getParentID(bucketId, pathComponents, keyName,
              omMetadataManager, ozoneManager);
      String dbFileKey = omMetadataManager.getOzonePathKey(parentID, fileName);
      dbOpenFileKey = omMetadataManager.getOpenFileName(parentID, fileName,
              commitKeyRequest.getClientID());

      omKeyInfo = OMFileRequest.getOmKeyInfoFromFileTable(true,
              omMetadataManager, dbOpenFileKey, keyName);
      if (omKeyInfo == null) {
        throw new OMException("Failed to commit key, as " + dbOpenFileKey +
                "entry is not found in the OpenKey table", KEY_NOT_FOUND);
      }
      omKeyInfo.setDataSize(commitKeyArgs.getDataSize());

      omKeyInfo.setModificationTime(commitKeyArgs.getModificationTime());

      // Update the block length for each block
      omKeyInfo.updateLocationInfoList(locationInfoList, false);

      // Set the UpdateID to current transactionLogIndex
      omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      // Add to cache of open key table and key table.
      OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager, dbFileKey,
              null, fileName, trxnLogIndex);

      OMFileRequest.addFileTableCacheEntry(omMetadataManager, dbFileKey,
              omKeyInfo, fileName, trxnLogIndex);

      long scmBlockSize = ozoneManager.getScmBlockSize();
      int factor = omKeyInfo.getFactor().getNumber();
      // Block was pre-requested and UsedBytes updated when createKey and
      // AllocatedBlock. The space occupied by the Key shall be based on
      // the actual Key size, and the total Block size applied before should
      // be subtracted.
      long correctedSpace = omKeyInfo.getDataSize() * factor -
              locationInfoList.size() * scmBlockSize * factor;
      omBucketInfo.incrUsedBytes(correctedSpace);

      omClientResponse = new OMKeyCommitResponseV1(omResponse.build(),
              omKeyInfo, dbFileKey, dbOpenFileKey, omVolumeArgs, omBucketInfo);

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyCommitResponseV1(createErrorOMResponse(
              omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
              omDoubleBufferHelper);

      if(bucketLockAcquired) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
                bucketName);
      }
    }

    auditLog(auditLogger, buildAuditMessage(OMAction.COMMIT_KEY, auditMap,
            exception, getOmRequest().getUserInfo()));

    processResult(commitKeyRequest, volumeName, bucketName, keyName, omMetrics,
            exception, omKeyInfo, result);

    return omClientResponse;
  }


  /**
   * Check for directory exists with same name, if it exists throw error.
   *
   * @param keyName                  key name
   * @param ozoneManager             Ozone Manager
   * @param reachedLastPathComponent true if the path component is a fileName
   * @throws IOException if directory exists with same name
   */
  private void checkDirectoryAlreadyExists(String keyName,
                                           OzoneManager ozoneManager,
                                           boolean reachedLastPathComponent)
          throws IOException {
    // Reached last component, which would be a file. Returns its parentID.
    if (reachedLastPathComponent && ozoneManager.getEnableFileSystemPaths()) {
      throw new OMException("Can not create file: " + keyName +
              " as there is already directory in the given path", NOT_A_FILE);
    }
  }

  /**
   * Get parent id for the user given path.
   *
   * @param bucketId          bucket id
   * @param pathComponents    fie path elements
   * @param keyName           user given key name
   * @param omMetadataManager metadata manager
   * @return lastKnownParentID
   * @throws IOException DB failure or parent not exists in DirectoryTable
   */
  private long getParentID(long bucketId, Iterator<Path> pathComponents,
                           String keyName, OMMetadataManager omMetadataManager,
                           OzoneManager ozoneManager)
          throws IOException {

    long lastKnownParentId = bucketId;

    // If no sub-dirs then bucketID is the root/parent.
    if(!pathComponents.hasNext()){
      return bucketId;
    }

    OmDirectoryInfo omDirectoryInfo;
    while (pathComponents.hasNext()) {
      String nodeName = pathComponents.next().toString();
      boolean reachedLastPathComponent = !pathComponents.hasNext();
      String dbNodeName =
              omMetadataManager.getOzonePathKey(lastKnownParentId, nodeName);

      omDirectoryInfo = omMetadataManager.
              getDirectoryTable().get(dbNodeName);
      if (omDirectoryInfo != null) {
        checkDirectoryAlreadyExists(keyName, ozoneManager,
                reachedLastPathComponent);
        lastKnownParentId = omDirectoryInfo.getObjectID();
      } else {
        // One of the sub-dir doesn't exists in DB. Immediate parent should
        // exists for committing the key, otherwise will fail the operation.
        if (!reachedLastPathComponent) {
          throw new OMException("Failed to commit key, as parent directory of "
                  + keyName + " entry is not found in DirectoryTable",
                  KEY_NOT_FOUND);
        }
        break;
      }
    }

    return lastKnownParentId;
  }
}
