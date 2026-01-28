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

package org.apache.hadoop.ozone.om.request.s3.multipart;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneConfigUtil;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.OMClientVersionValidator;
import org.apache.hadoop.ozone.om.request.validation.OMLayoutVersionValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3MultipartUploadCompleteResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadCompleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle Multipart upload complete request.
 */
public class S3MultipartUploadCompleteRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3MultipartUploadCompleteRequest.class);

  private BiFunction<OzoneManagerProtocolProtos.Part, PartKeyInfo, MultipartCommitRequestPart> eTagBasedValidator =
      (part, partKeyInfo) -> {
        String eTag = part.getETag();
        AtomicReference<String> dbPartETag = new AtomicReference<>();
        String dbPartName = null;
        if (partKeyInfo != null) {
          partKeyInfo.getPartKeyInfo().getMetadataList()
              .stream()
              .filter(keyValue -> keyValue.getKey().equals(OzoneConsts.ETAG))
              .findFirst().ifPresent(kv -> dbPartETag.set(kv.getValue()));
          dbPartName = partKeyInfo.getPartName();
        }
        return new MultipartCommitRequestPart(eTag, partKeyInfo == null ? null :
            dbPartETag.get(), StringUtils.equals(eTag, dbPartETag.get()) || StringUtils.equals(eTag, dbPartName));
      };
  private BiFunction<OzoneManagerProtocolProtos.Part, PartKeyInfo, MultipartCommitRequestPart> partNameBasedValidator =
      (part, partKeyInfo) -> {
        String partName = part.getPartName();
        String dbPartName = null;
        if (partKeyInfo != null) {
          dbPartName = partKeyInfo.getPartName();
        }
        return new MultipartCommitRequestPart(partName, partKeyInfo == null ? null :
            dbPartName, StringUtils.equals(partName, dbPartName));
      };

  public S3MultipartUploadCompleteRequest(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    MultipartUploadCompleteRequest multipartUploadCompleteRequest =
        super.preExecute(ozoneManager).getCompleteMultiPartUploadRequest();

    KeyArgs keyArgs = multipartUploadCompleteRequest.getKeyArgs();
    String keyPath = keyArgs.getKeyName();
    keyPath = validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(),
        keyPath, getBucketLayout());

    KeyArgs newKeyArgs = keyArgs.toBuilder().setModificationTime(Time.now())
            .setKeyName(keyPath).build();
    KeyArgs resolvedArgs = resolveBucketAndCheckKeyAcls(newKeyArgs,
        ozoneManager, ACLType.WRITE);

    return getOmRequest().toBuilder().setCompleteMultiPartUploadRequest(
        multipartUploadCompleteRequest.toBuilder().setKeyArgs(
            resolvedArgs)).setUserInfo(getUserInfo()).build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();
    MultipartUploadCompleteRequest multipartUploadCompleteRequest =
        getOmRequest().getCompleteMultiPartUploadRequest();

    KeyArgs keyArgs = multipartUploadCompleteRequest.getKeyArgs();

    List<OzoneManagerProtocolProtos.Part> partsList =
        multipartUploadCompleteRequest.getPartsListList();
    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
    auditMap.put(OzoneConsts.UPLOAD_ID, keyArgs.getMultipartUploadID());

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    final String requestedVolume = volumeName;
    final String requestedBucket = bucketName;
    String keyName = keyArgs.getKeyName();
    String uploadID = keyArgs.getMultipartUploadID();
    String multipartKey = null;

    ozoneManager.getMetrics().incNumCompleteMultipartUploads();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    boolean acquiredLock = false;
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;
    Exception exception = null;
    Result result = null;
    try {
      multipartKey = omMetadataManager.getMultipartKey(volumeName,
          bucketName, keyName, uploadID);

      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);
      OmBucketInfo omBucketInfo = getBucketInfo(omMetadataManager,
          volumeName, bucketName);

      List<OmDirectoryInfo> missingParentInfos;
      OMFileRequest.OMPathInfoWithFSO pathInfoFSO = OMFileRequest
          .verifyDirectoryKeysInPath(omMetadataManager, volumeName, bucketName,
              keyName, Paths.get(keyName));
      missingParentInfos = getAllMissingParentDirInfo(ozoneManager, keyArgs, omBucketInfo,
              pathInfoFSO, trxnLogIndex);

      if (missingParentInfos != null) {
        final long volumeId = omMetadataManager.getVolumeId(volumeName);
        final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);

        // add all missing parents to directory table
        addMissingParentsToCache(omBucketInfo, missingParentInfos,
            omMetadataManager, volumeId, bucketId, trxnLogIndex);

        String multipartOpenKey = omMetadataManager
            .getMultipartKey(volumeId, bucketId,
                pathInfoFSO.getLastKnownParentId(),
                pathInfoFSO.getLeafNodeName(),
                keyArgs.getMultipartUploadID());

        if (getOmKeyInfoFromOpenKeyTable(multipartOpenKey,
            keyName, omMetadataManager) == null) {

          final ReplicationConfig replicationConfig = OzoneConfigUtil
              .resolveReplicationConfigPreference(keyArgs.getType(),
                  keyArgs.getFactor(), keyArgs.getEcReplicationConfig(),
                  omBucketInfo != null ?
                      omBucketInfo.getDefaultReplicationConfig() :
                      null, ozoneManager);

          OmMultipartKeyInfo multipartKeyInfoFromArgs =
              new OmMultipartKeyInfo.Builder()
                  .setUploadID(keyArgs.getMultipartUploadID())
                  .setCreationTime(keyArgs.getModificationTime())
                  .setReplicationConfig(replicationConfig)
                  .setObjectID(pathInfoFSO.getLeafNodeObjectId())
                  .setUpdateID(trxnLogIndex)
                  .setParentID(pathInfoFSO.getLastKnownParentId())
                  .build();

          OmKeyInfo keyInfoFromArgs = new OmKeyInfo.Builder()
              .setVolumeName(volumeName)
              .setBucketName(bucketName)
              .setKeyName(keyName)
              .setCreationTime(keyArgs.getModificationTime())
              .setModificationTime(keyArgs.getModificationTime())
              .setReplicationConfig(replicationConfig)
              .setOmKeyLocationInfos(Collections.singletonList(
                  new OmKeyLocationInfoGroup(0, new ArrayList<>(), true)))
              .setAcls(getAclsForKey(keyArgs, omBucketInfo, pathInfoFSO,
                  ozoneManager.getPrefixManager(), ozoneManager.getConfig()))
              .setObjectID(pathInfoFSO.getLeafNodeObjectId())
              .setUpdateID(trxnLogIndex)
              .setFileEncryptionInfo(keyArgs.hasFileEncryptionInfo() ?
                  OMPBHelper.convert(keyArgs.getFileEncryptionInfo()) : null)
              .setParentObjectID(pathInfoFSO.getLastKnownParentId())
              .build();

          // Add missing multi part info to open key table
          addMultiPartToCache(omMetadataManager, multipartOpenKey,
              pathInfoFSO, keyInfoFromArgs, keyName, trxnLogIndex);
        }
      }

      String dbMultipartOpenKey =
          getDBMultipartOpenKey(volumeName, bucketName, keyName, uploadID,
              omMetadataManager);

      OmMultipartKeyInfo multipartKeyInfo = omMetadataManager
          .getMultipartInfoTable().get(multipartKey);

      String ozoneKey = omMetadataManager.getOzoneKey(
          volumeName, bucketName, keyName);

      String dbOzoneKey =
          getDBOzoneKey(omMetadataManager, volumeName, bucketName, keyName);

      // Check for directory exists with same name for the LEGACY_FS,
      // if it exists throw error.
      checkDirectoryAlreadyExists(ozoneManager, omBucketInfo, keyName,
          omMetadataManager);

      if (multipartKeyInfo == null) {
        throw new OMException(
            failureMessage(requestedVolume, requestedBucket, keyName),
            OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
      }

      if (!partsList.isEmpty()) {
        final OmMultipartKeyInfo.PartKeyInfoMap partKeyInfoMap
            = multipartKeyInfo.getPartKeyInfoMap();
        if (partKeyInfoMap.size() == 0) {
          LOG.error("Complete MultipartUpload failed for key {} , MPU Key has" +
                  " no parts in OM, parts given to upload are {}", ozoneKey,
              partsList);
          throw new OMException(
              failureMessage(requestedVolume, requestedBucket, keyName),
              OMException.ResultCodes.INVALID_PART);
        }

        // First Check for Invalid Part Order.
        List< Integer > partNumbers = new ArrayList<>();
        int partsListSize = getPartsListSize(requestedVolume,
                requestedBucket, keyName, ozoneKey, partNumbers, partsList);

        List<OmKeyLocationInfo> partLocationInfos = new ArrayList<>();
        long dataSize = getMultipartDataSize(requestedVolume, requestedBucket,
                keyName, ozoneKey, partKeyInfoMap, partsListSize,
                partLocationInfos, partsList, ozoneManager);

        // All parts have same replication information. Here getting from last
        // part.
        OmKeyInfo omKeyInfo =
            getOmKeyInfo(trxnLogIndex, keyArgs, volumeName,
                bucketName, keyName, dbMultipartOpenKey, omMetadataManager,
                dbOzoneKey, partKeyInfoMap, partLocationInfos, dataSize);

        long usedBytesDiff = 0;
        //Find all unused parts.
        List<OmKeyInfo> allKeyInfoToRemove = new ArrayList<>();
        for (PartKeyInfo partKeyInfo : partKeyInfoMap) {
          if (!partNumbers.contains(partKeyInfo.getPartNumber())) {
            OmKeyInfo delPartKeyInfo =
                OmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());
            allKeyInfoToRemove.add(delPartKeyInfo);
            usedBytesDiff -= delPartKeyInfo.getReplicatedSize();
          }
        }

        // If bucket versioning is turned on during the update, between key
        // creation and key commit, old versions will be just overwritten and
        // not kept. Bucket versioning will be effective from the first key
        // creation after the knob turned on.
        OmKeyInfo keyToDelete =
            omMetadataManager.getKeyTable(getBucketLayout()).get(dbOzoneKey);
        boolean isNamespaceUpdate = false;
        if (keyToDelete != null && !omBucketInfo.getIsVersionEnabled()) {
          RepeatedOmKeyInfo oldKeyVersionsToDelete = getOldVersionsToCleanUp(
              keyToDelete, omBucketInfo.getObjectID(), trxnLogIndex);
          allKeyInfoToRemove.addAll(oldKeyVersionsToDelete.getOmKeyInfoList());
          usedBytesDiff -= keyToDelete.getReplicatedSize();
        } else {
          checkBucketQuotaInNamespace(omBucketInfo, 1L);
          omBucketInfo.incrUsedNamespace(1L);
          isNamespaceUpdate = true;
        }

        String dbBucketKey = omMetadataManager.getBucketKey(
            omBucketInfo.getVolumeName(), omBucketInfo.getBucketName());
        if (usedBytesDiff != 0) {
          omBucketInfo.incrUsedBytes(usedBytesDiff);
        } else if (!isNamespaceUpdate) {
          // If no bucket size and Namespace changed, prevent from updating
          // bucket object.
          omBucketInfo = null;
        }

        updateCache(omMetadataManager, dbBucketKey, omBucketInfo, dbOzoneKey,
            dbMultipartOpenKey, multipartKey, omKeyInfo, trxnLogIndex);

        omResponse.setCompleteMultiPartUploadResponse(
            MultipartUploadCompleteResponse.newBuilder()
                .setVolume(requestedVolume)
                .setBucket(requestedBucket)
                .setKey(keyName)
                .setHash(omKeyInfo.getMetadata().get(OzoneConsts.ETAG)));

        long volumeId = omMetadataManager.getVolumeId(volumeName);
        long bucketId = omMetadataManager.getBucketId(volumeName, bucketName);
        omClientResponse =
            getOmClientResponse(multipartKey, omResponse, dbMultipartOpenKey,
                omKeyInfo, allKeyInfoToRemove, omBucketInfo,
                volumeId, bucketId, missingParentInfos, multipartKeyInfo);

        result = Result.SUCCESS;
      } else {
        throw new OMException(
            failureMessage(requestedVolume, requestedBucket, keyName) +
            " because of empty part list",
            OMException.ResultCodes.INVALID_REQUEST);
      }

    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = getOmClientResponse(omResponse, exception);
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    logResult(ozoneManager, multipartUploadCompleteRequest, partsList,
            auditMap, volumeName, bucketName, keyName, exception, result);

    return omClientResponse;
  }

  protected S3MultipartUploadCompleteResponse getOmClientResponse(
      OMResponse.Builder omResponse, Exception exception) {
    return new S3MultipartUploadCompleteResponse(
        createErrorOMResponse(omResponse, exception), getBucketLayout());
  }

  @SuppressWarnings("parameternumber")
  protected OMClientResponse getOmClientResponse(String multipartKey,
      OMResponse.Builder omResponse, String dbMultipartOpenKey,
      OmKeyInfo omKeyInfo,  List<OmKeyInfo> allKeyInfoToRemove,
      OmBucketInfo omBucketInfo,
      long volumeId, long bucketId, List<OmDirectoryInfo> missingParentInfos,
      OmMultipartKeyInfo multipartKeyInfo) {

    return new S3MultipartUploadCompleteResponse(omResponse.build(),
        multipartKey, dbMultipartOpenKey, omKeyInfo, allKeyInfoToRemove,
        getBucketLayout(), omBucketInfo, bucketId);
  }

  protected void checkDirectoryAlreadyExists(OzoneManager ozoneManager,
      OmBucketInfo omBucketInfo, String keyName,
      OMMetadataManager omMetadataManager) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("BucketName: {}, BucketLayout: {}",
          omBucketInfo.getBucketName(), omBucketInfo.getBucketLayout());
    }
    if (omBucketInfo.getBucketLayout()
        .shouldNormalizePaths(ozoneManager.getEnableFileSystemPaths())) {
      if (checkDirectoryAlreadyExists(omBucketInfo.getVolumeName(),
          omBucketInfo.getBucketName(), keyName, omMetadataManager)) {
        throw new OMException("Can not Complete MPU for file: " + keyName +
                " as there is already directory in the given path",
                NOT_A_FILE);
      }
    }
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  protected void logResult(OzoneManager ozoneManager,
      MultipartUploadCompleteRequest multipartUploadCompleteRequest,
      List<OzoneManagerProtocolProtos.Part> partsList,
      Map<String, String> auditMap, String volumeName,
      String bucketName, String keyName, Exception exception,
      Result result) {
    auditMap.put(OzoneConsts.MULTIPART_LIST, partsList.toString()
        .replaceAll("\\n", " "));

    // audit log
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.COMPLETE_MULTIPART_UPLOAD, auditMap, exception,
        getOmRequest().getUserInfo()));

    switch (result) {
    case SUCCESS:
      LOG.debug("MultipartUpload Complete request is successful for Key: {} " +
          "in Volume/Bucket {}/{}", keyName, volumeName, bucketName);
      break;
    case FAILURE:
      ozoneManager.getMetrics().incNumCompleteMultipartUploadFails();
      LOG.error("MultipartUpload Complete request failed for Key: {} " +
          "in Volume/Bucket {}/{}", keyName, volumeName, bucketName,
          exception);
      break;
    default:
      LOG.error("Unrecognized Result for S3MultipartUploadCommitRequest: {}",
          multipartUploadCompleteRequest);
    }
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  protected OmKeyInfo getOmKeyInfo(long trxnLogIndex,
      KeyArgs keyArgs, String volumeName, String bucketName, String keyName,
      String multipartOpenKey, OMMetadataManager omMetadataManager,
      String ozoneKey, OmMultipartKeyInfo.PartKeyInfoMap partKeyInfoMap,
      List<OmKeyLocationInfo> partLocationInfos, long dataSize)
          throws IOException {
    OzoneManagerProtocolProtos.KeyInfo partKeyInfo =
        partKeyInfoMap.lastEntry().getPartKeyInfo();

    OmKeyInfo omKeyInfo = getOmKeyInfoFromKeyTable(ozoneKey, keyName,
            omMetadataManager);
    OmKeyInfo.Builder builder;
    if (omKeyInfo == null) {
      // This is a newly added key, it does not have any versions.
      OmKeyLocationInfoGroup keyLocationInfoGroup = new
          OmKeyLocationInfoGroup(0, partLocationInfos, true);

      // Get the objectID of the key from OpenKeyTable
      OmKeyInfo dbOpenKeyInfo = getOmKeyInfoFromOpenKeyTable(multipartOpenKey,
              keyName, omMetadataManager);

      // A newly created key, this is the first version.
      builder = dbOpenKeyInfo.toBuilder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setReplicationConfig(ReplicationConfig.fromProto(
              partKeyInfo.getType(), partKeyInfo.getFactor(),
              partKeyInfo.getEcReplicationConfig()))
          .setCreationTime(keyArgs.getModificationTime())
          .setModificationTime(keyArgs.getModificationTime())
          .setDataSize(dataSize)
          .setOmKeyLocationInfos(
              Collections.singletonList(keyLocationInfoGroup))
          .addMetadata(OzoneConsts.ETAG,
              multipartUploadedKeyHash(partKeyInfoMap))
          .setOwnerName(keyArgs.getOwnerName());
    } else {
      OmKeyInfo dbOpenKeyInfo = getOmKeyInfoFromOpenKeyTable(multipartOpenKey,
          keyName, omMetadataManager);

      // Already a version exists, so we should add it as a new version.
      // But now as versioning is not supported, just following the commit
      // key approach. When versioning support comes, then we can uncomment
      // below code keyInfo.addNewVersion(locations);

      // As right now versioning is not supported, we can set encryption info
      // at KeyInfo level, but once we start supporting versioning,
      // encryption info needs to be set at KeyLocation level, as each version
      // will have it's own file encryption info.
      if (dbOpenKeyInfo.getFileEncryptionInfo() != null) {
        omKeyInfo.setFileEncryptionInfo(dbOpenKeyInfo.getFileEncryptionInfo());
      }
      omKeyInfo.updateLocationInfoList(partLocationInfos, true, true);
      omKeyInfo.setModificationTime(keyArgs.getModificationTime());
      omKeyInfo.setDataSize(dataSize);
      omKeyInfo.setReplicationConfig(dbOpenKeyInfo.getReplicationConfig());
      final String multipartHash = multipartUploadedKeyHash(partKeyInfoMap);
      builder = omKeyInfo.toBuilder();
      if (dbOpenKeyInfo.getMetadata() != null) {
        // make modifiable because ETAG needs to be added
        builder.setMetadata(new LinkedHashMap<>(dbOpenKeyInfo.getMetadata()));
      }
      builder.addMetadata(OzoneConsts.ETAG, multipartHash);
      if (dbOpenKeyInfo.getTags() != null) {
        builder.setTags(dbOpenKeyInfo.getTags());
      }
    }
    return builder.setUpdateID(trxnLogIndex).build();
  }

  protected String getDBOzoneKey(OMMetadataManager omMetadataManager,
      String volumeName, String bucketName, String keyName) throws IOException {
    return omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
  }

  protected void addMissingParentsToCache(OmBucketInfo omBucketInfo,
      List<OmDirectoryInfo> missingParentInfos,
      OMMetadataManager omMetadataManager,
      long volumeId, long bucketId, long transactionLogIndex
  ) throws IOException {
    // FSO is disabled. Do nothing.
  }

  protected void addMultiPartToCache(
      OMMetadataManager omMetadataManager, String multipartOpenKey,
      OMFileRequest.OMPathInfoWithFSO pathInfoFSO, OmKeyInfo omKeyInfo,
      String keyName, long transactionLogIndex
  ) throws IOException {
    // FSO is disabled. Do nothing.
  }

  protected OmKeyInfo getOmKeyInfoFromKeyTable(String dbOzoneKey,
      String keyName, OMMetadataManager omMetadataManager) throws IOException {
    return omMetadataManager.getKeyTable(getBucketLayout()).get(dbOzoneKey);
  }

  protected OmKeyInfo getOmKeyInfoFromOpenKeyTable(String dbMultipartKey,
      String keyName, OMMetadataManager omMetadataManager) throws IOException {
    return omMetadataManager.getOpenKeyTable(getBucketLayout())
        .get(dbMultipartKey);
  }

  protected void addKeyTableCacheEntry(OMMetadataManager omMetadataManager,
      String dbOzoneKey, OmKeyInfo omKeyInfo, long transactionLogIndex)
      throws IOException {

    // Add key entry to file table.
    omMetadataManager.getKeyTable(getBucketLayout())
        .addCacheEntry(new CacheKey<>(dbOzoneKey),
            CacheValue.get(transactionLogIndex, omKeyInfo));
  }

  private int getPartsListSize(String requestedVolume,
      String requestedBucket, String keyName, String ozoneKey,
      List<Integer> partNumbers,
      List<OzoneManagerProtocolProtos.Part> partsList) throws OMException {
    int prevPartNumber = partsList.get(0).getPartNumber();
    int partsListSize = partsList.size();
    partNumbers.add(prevPartNumber);
    for (int i = 1; i < partsListSize; i++) {
      int currentPartNumber = partsList.get(i).getPartNumber();
      if (prevPartNumber >= currentPartNumber) {
        LOG.error("PartNumber at index {} is {}, and its previous " +
                "partNumber at index {} is {} for ozonekey is " +
                "{}", i, currentPartNumber, i - 1, prevPartNumber,
            ozoneKey);
        throw new OMException(
            failureMessage(requestedVolume, requestedBucket, keyName) +
            " because parts are in Invalid order.",
            OMException.ResultCodes.INVALID_PART_ORDER);
      }
      prevPartNumber = currentPartNumber;
      partNumbers.add(prevPartNumber);
    }
    return partsListSize;
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private long getMultipartDataSize(String requestedVolume,
      String requestedBucket, String keyName, String ozoneKey,
      OmMultipartKeyInfo.PartKeyInfoMap partKeyInfoMap,
      int partsListSize, List<OmKeyLocationInfo> partLocationInfos,
      List<OzoneManagerProtocolProtos.Part> partsList,
      OzoneManager ozoneManager) throws OMException {
    long dataSize = 0;
    int currentPartCount = 0;
    boolean eTagBasedValidationAvailable = partsList.stream().allMatch(OzoneManagerProtocolProtos.Part::hasETag);
    // Now do actual logic, and check for any Invalid part during this.
    for (OzoneManagerProtocolProtos.Part part : partsList) {
      currentPartCount++;
      int partNumber = part.getPartNumber();
      PartKeyInfo partKeyInfo = partKeyInfoMap.get(partNumber);
      MultipartCommitRequestPart requestPart = eTagBasedValidationAvailable ?
          eTagBasedValidator.apply(part, partKeyInfo) : partNameBasedValidator.apply(part, partKeyInfo);
      if (!requestPart.isValid()) {
        throw new OMException(
            failureMessage(requestedVolume, requestedBucket, keyName) +
            ". Provided Part info is { " + requestPart.getRequestPartId() + ", " + partNumber +
            "}, whereas OM has eTag " + requestPart.getOmPartId(),
            OMException.ResultCodes.INVALID_PART);
      }

      final OmKeyInfo currentPartKeyInfo = OmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());

      // Except for last part all parts should have minimum size.
      if (currentPartCount != partsListSize) {
        if (currentPartKeyInfo.getDataSize() <
            ozoneManager.getMinMultipartUploadPartSize()) {
          LOG.error("MultipartUpload: {} Part number: {} size {}  is less" +
                  " than minimum part size {}", ozoneKey,
              partKeyInfo.getPartNumber(), currentPartKeyInfo.getDataSize(),
              ozoneManager.getMinMultipartUploadPartSize());
          throw new OMException(
              failureMessage(requestedVolume, requestedBucket, keyName) +
                  ". Entity too small.",
              OMException.ResultCodes.ENTITY_TOO_SMALL);
        }
      }

      // As all part keys will have only one version.
      OmKeyLocationInfoGroup currentKeyInfoGroup = currentPartKeyInfo
          .getKeyLocationVersions().get(0);

      // Set partNumber in each block.
      currentKeyInfoGroup.getLocationList().forEach(
          omKeyLocationInfo -> omKeyLocationInfo.setPartNumber(partNumber));

      partLocationInfos.addAll(currentKeyInfoGroup.getLocationList());
      dataSize += currentPartKeyInfo.getDataSize();
    }
    return dataSize;
  }

  private static String failureMessage(String volume, String bucket,
      String keyName) {
    return "Complete Multipart Upload Failed: volume: " +
        volume + " bucket: " + bucket + " key: " + keyName;
  }

  @SuppressWarnings("parameternumber")
  private void updateCache(OMMetadataManager omMetadataManager,
      String dbBucketKey, @Nullable OmBucketInfo omBucketInfo,
      String dbOzoneKey, String dbMultipartOpenKey, String dbMultipartKey,
      OmKeyInfo omKeyInfo, long transactionLogIndex) throws IOException {
    // Update cache.
    // 1. Add key entry to key table.
    // 2. Delete multipartKey entry from openKeyTable and multipartInfo table.
    // 3. If the bucket size has changed (omBucketInfo is not null),
    //    update bucket cache
    addKeyTableCacheEntry(omMetadataManager, dbOzoneKey, omKeyInfo,
        transactionLogIndex);

    omMetadataManager.getOpenKeyTable(getBucketLayout())
        .addCacheEntry(
            new CacheKey<>(dbMultipartOpenKey),
            CacheValue.get(transactionLogIndex));
    omMetadataManager.getMultipartInfoTable().addCacheEntry(
        new CacheKey<>(dbMultipartKey),
        CacheValue.get(transactionLogIndex));

    // Here, omBucketInfo can be null if its size has not changed. No need to
    // update the bucket info unless its size has changed. We never want to
    // delete the bucket info here, but just avoiding unnecessary update.
    if (omBucketInfo != null) {
      omMetadataManager.getBucketTable().addCacheEntry(
          new CacheKey<>(dbBucketKey),
          CacheValue.get(transactionLogIndex, omBucketInfo));
    }
  }

  @OMLayoutVersionValidator(
      applyBefore = OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CompleteMultiPartUpload
  )
  public static OMRequest
      disallowCompleteMultiPartUploadWithECReplicationConfig(
      OMRequest req, ValidationContext ctx) throws OMException {
    if (!ctx.versionManager().isAllowed(
        OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT)) {
      if (req.getCompleteMultiPartUploadRequest().getKeyArgs()
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
   * Validates S3 MPU complete requests.
   * We do not want to allow older clients to upload MPU keys to buckets which
   * use non LEGACY layouts.
   *
   * @param req - the request to validate
   * @param ctx - the validation context
   * @return the validated request
   * @throws OMException if the request is invalid
   */
  @OMClientVersionValidator(
      applyBefore = ClientVersion.BUCKET_LAYOUT_SUPPORT,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CompleteMultiPartUpload
  )
  public static OMRequest blockMPUCompleteWithBucketLayoutFromOldClient(
      OMRequest req, ValidationContext ctx) throws IOException {
    if (req.getCompleteMultiPartUploadRequest().hasKeyArgs()) {
      KeyArgs keyArgs = req.getCompleteMultiPartUploadRequest().getKeyArgs();

      if (keyArgs.hasVolumeName() && keyArgs.hasBucketName()) {
        BucketLayout bucketLayout = ctx.getBucketLayout(
            keyArgs.getVolumeName(), keyArgs.getBucketName());
        bucketLayout.validateSupportedOperation();
      }
    }
    return req;
  }

  private String multipartUploadedKeyHash(
      OmMultipartKeyInfo.PartKeyInfoMap partsList) {
    StringBuffer keysConcatenated = new StringBuffer();
    for (PartKeyInfo partKeyInfo: partsList) {
      String partPropertyToComputeHash = KeyValueUtil.getFromProtobuf(partKeyInfo.getPartKeyInfo().getMetadataList())
          .get(OzoneConsts.ETAG);
      if (partPropertyToComputeHash == null) {
        partPropertyToComputeHash = partKeyInfo.getPartName();
      }
      keysConcatenated.append(partPropertyToComputeHash);
    }
    return DigestUtils.md5Hex(keysConcatenated.toString()) + "-"
        + partsList.size();
  }

  private static class MultipartCommitRequestPart {
    private String requestPartId;

    private String omPartId;

    private boolean isValid;

    MultipartCommitRequestPart(String requestPartId, String omPartId, boolean isValid) {
      this.requestPartId = requestPartId;
      this.omPartId = omPartId;
      this.isValid = isValid;
    }

    public String getRequestPartId() {
      return requestPartId;
    }

    public String getOmPartId() {
      return omPartId;
    }

    public boolean isValid() {
      return isValid;
    }
  }

}
