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

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneConfigUtil;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.file.OMDirectoryCreateRequestWithFSO;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCreateResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.getParentId;

/**
 * Handles CreateKey request layout version1.
 */
public class OMKeyCreateRequestWithFSO extends OMKeyCreateRequest {

  public OMKeyCreateRequestWithFSO(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    OzoneManagerProtocolProtos.CreateKeyRequest createKeyRequest =
            getOmRequest().getCreateKeyRequest();

    OzoneManagerProtocolProtos.KeyArgs keyArgs = createKeyRequest.getKeyArgs();
    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyAllocates();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OmBucketInfo omBucketInfo = null;
    final List<OmKeyLocationInfo> locations = new ArrayList<>();

    boolean acquireLock = false;
    OMClientResponse omClientResponse = null;
    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
            OmResponseUtil.getOMResponseBuilder(getOmRequest());
    IOException exception = null;
    Result result;
    List<OmDirectoryInfo> missingParentInfos;
    int numKeysCreated = 0;
    try {
      keyArgs = resolveBucketLink(ozoneManager, keyArgs, auditMap);
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();

      // check Acl
      checkKeyAcls(ozoneManager, volumeName, bucketName, keyName,
              IAccessAuthorizer.ACLType.CREATE, OzoneObj.ResourceType.KEY);

      acquireLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
              volumeName, bucketName);
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      final long volumeId = omMetadataManager.getVolumeTable()
              .get(omMetadataManager.getVolumeKey(volumeName)).getObjectID();
      final long bucketId = omMetadataManager.getBucketTable()
              .get(omMetadataManager.getBucketKey(volumeName, bucketName))
              .getObjectID();

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

      // Check if a file or directory exists with same key name.
      if (pathInfoFSO.getDirectoryResult() == DIRECTORY_EXISTS) {
        throw new OMException("Cannot write to "
            + "directory. createIntermediateDirs behavior is enabled and "
            + "hence / has special interpretation: " + keyName, NOT_A_FILE);
      } else if (pathInfoFSO.getDirectoryResult()
          == FILE_EXISTS_IN_GIVENPATH) {
        throw new OMException("Can not create file: " + keyName
            + " as there is already file in the given path", NOT_A_FILE);
      }

      // add all missing parents to dir table
      missingParentInfos =
              OMDirectoryCreateRequestWithFSO.getAllMissingParentDirInfo(
                      ozoneManager, keyArgs, pathInfoFSO, trxnLogIndex);

      // total number of keys created.
      numKeysCreated = missingParentInfos.size();

      // do open key
      OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(
              omMetadataManager.getBucketKey(volumeName, bucketName));
      final ReplicationConfig repConfig = OzoneConfigUtil
          .resolveReplicationConfigPreference(keyArgs.getType(),
              keyArgs.getFactor(), keyArgs.getEcReplicationConfig(),
              bucketInfo.getDefaultReplicationConfig(),
              ozoneManager.getDefaultReplicationConfig());

      OmKeyInfo omFileInfo = prepareFileInfo(omMetadataManager, keyArgs,
              dbFileInfo, keyArgs.getDataSize(), locations,
              getFileEncryptionInfo(keyArgs), ozoneManager.getPrefixManager(),
              bucketInfo, pathInfoFSO, trxnLogIndex,
              pathInfoFSO.getLeafNodeObjectId(),
              ozoneManager.isRatisEnabled(), repConfig);

      long openVersion = omFileInfo.getLatestVersionLocations().getVersion();
      long clientID = createKeyRequest.getClientID();
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
      checkBucketQuotaInBytes(omBucketInfo, preAllocatedSpace);
      checkBucketQuotaInNamespace(omBucketInfo, 1L);

      // Add to cache entry can be done outside of lock for this openKey.
      // Even if bucket gets deleted, when commitKey we shall identify if
      // bucket gets deleted.
      OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager,
              dbOpenFileName, omFileInfo, pathInfoFSO.getLeafNodeName(),
              trxnLogIndex);

      // Add cache entries for the prefix directories.
      // Skip adding for the file key itself, until Key Commit.
      OMFileRequest.addDirectoryTableCacheEntries(omMetadataManager,
              volumeId, bucketId, trxnLogIndex,
              Optional.of(missingParentInfos), Optional.absent());

      // Prepare response. Sets user given full key name in the 'keyName'
      // attribute in response object.
      int clientVersion = getOmRequest().getVersion();
      omBucketInfo.incrUsedNamespace(numKeysCreated);
      omResponse.setCreateKeyResponse(CreateKeyResponse.newBuilder()
              .setKeyInfo(omFileInfo.getNetworkProtobuf(keyName, clientVersion,
                  keyArgs.getLatestVersionLocation()))
              .setID(clientID)
              .setOpenVersion(openVersion).build())
              .setCmdType(Type.CreateKey);
      omClientResponse = new OMKeyCreateResponseWithFSO(omResponse.build(),
              omFileInfo, missingParentInfos, clientID,
              omBucketInfo.copyObject(), volumeId);

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omMetrics.incNumKeyAllocateFails();
      omResponse.setCmdType(Type.CreateKey);
      omClientResponse = new OMKeyCreateResponseWithFSO(
              createErrorOMResponse(omResponse, exception), getBucketLayout());
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
              omDoubleBufferHelper);
      if (acquireLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
                bucketName);
      }
    }

    // Audit Log outside the lock
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
            OMAction.ALLOCATE_KEY, auditMap, exception,
            getOmRequest().getUserInfo()));

    logResult(createKeyRequest, omMetrics, exception, result,
            numKeysCreated);

    return omClientResponse;
  }

  /**
   * Returns the DB key name of a multipart open key in OM metadata store.
   *
   * @param volumeName        - volume name.
   * @param bucketName        - bucket name.
   * @param keyName           - key name.
   * @param uploadID          - Multi part upload ID for this key.
   * @param omMetadataManager
   * @return
   * @throws IOException
   */
  @Override
  protected String getDBMultipartOpenKey(String volumeName, String bucketName,
                                         String keyName, String uploadID,
                                         OMMetadataManager omMetadataManager)
      throws IOException {

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    long parentId =
        getParentId(omMetadataManager, volumeName, bucketName, keyName);

    String fileName = OzoneFSUtils.getFileName(keyName);

    return omMetadataManager.getMultipartKey(volumeId, bucketId, parentId,
            fileName, uploadID);
  }
}
