/*
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

package org.apache.hadoop.ozone.om.request.s3.multipart;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3MultipartUploadCompleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadCompleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import com.google.common.base.Optional;
import org.apache.commons.codec.digest.DigestUtils;
import static org.apache.hadoop.ozone.OzoneConsts.OM_MULTIPART_MIN_SIZE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle Multipart upload complete request.
 */
public class S3MultipartUploadCompleteRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3MultipartUploadCompleteRequest.class);

  public S3MultipartUploadCompleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    MultipartUploadCompleteRequest multipartUploadCompleteRequest =
        getOmRequest().getCompleteMultiPartUploadRequest();

    KeyArgs keyArgs = multipartUploadCompleteRequest.getKeyArgs();

    return getOmRequest().toBuilder()
        .setCompleteMultiPartUploadRequest(multipartUploadCompleteRequest
            .toBuilder().setKeyArgs(keyArgs.toBuilder()
                .setModificationTime(Time.now())
                .setKeyName(validateAndNormalizeKey(
                    ozoneManager.getEnableFileSystemPaths(),
                    keyArgs.getKeyName()))))
        .setUserInfo(getUserInfo()).build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    MultipartUploadCompleteRequest multipartUploadCompleteRequest =
        getOmRequest().getCompleteMultiPartUploadRequest();

    KeyArgs keyArgs = multipartUploadCompleteRequest.getKeyArgs();

    List<OzoneManagerProtocolProtos.Part> partsList =
        multipartUploadCompleteRequest.getPartsListList();
    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);

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
    IOException exception = null;
    Result result = null;
    try {
      keyArgs = resolveBucketLink(ozoneManager, keyArgs, auditMap);
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();

      multipartKey = omMetadataManager.getMultipartKey(volumeName,
          bucketName, keyName, uploadID);

      // TODO to support S3 ACL later.

      acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
          volumeName, bucketName);

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      String ozoneKey = omMetadataManager.getOzoneKey(
          volumeName, bucketName, keyName);

      OmMultipartKeyInfo multipartKeyInfo = omMetadataManager
          .getMultipartInfoTable().get(multipartKey);

      // Check for directory exists with same name, if it exists throw error. 
      if (ozoneManager.getEnableFileSystemPaths()) {
        if (checkDirectoryAlreadyExists(volumeName, bucketName, keyName,
            omMetadataManager)) {
          throw new OMException("Can not Complete MPU for file: " + keyName +
              " as there is already directory in the given path", NOT_A_FILE);
        }
      }

      if (multipartKeyInfo == null) {
        throw new OMException(
            failureMessage(requestedVolume, requestedBucket, keyName),
            OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
      }
      TreeMap<Integer, PartKeyInfo> partKeyInfoMap =
          multipartKeyInfo.getPartKeyInfoMap();

      if (partsList.size() > 0) {
        if (partKeyInfoMap.size() == 0) {
          LOG.error("Complete MultipartUpload failed for key {} , MPU Key has" +
                  " no parts in OM, parts given to upload are {}", ozoneKey,
              partsList);
          throw new OMException(
              failureMessage(requestedVolume, requestedBucket, keyName),
              OMException.ResultCodes.INVALID_PART);
        }

        // First Check for Invalid Part Order.
        int prevPartNumber = partsList.get(0).getPartNumber();
        List< Integer > partNumbers = new ArrayList<>();
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


        List<OmKeyLocationInfo> partLocationInfos = new ArrayList<>();
        long dataSize = 0;
        int currentPartCount = 0;
        // Now do actual logic, and check for any Invalid part during this.
        for (OzoneManagerProtocolProtos.Part part : partsList) {
          currentPartCount++;
          int partNumber = part.getPartNumber();
          String partName = part.getPartName();

          PartKeyInfo partKeyInfo = partKeyInfoMap.get(partNumber);

          if (partKeyInfo == null ||
              !partName.equals(partKeyInfo.getPartName())) {
            String omPartName = partKeyInfo == null ? null :
                partKeyInfo.getPartName();
            throw new OMException(
                failureMessage(requestedVolume, requestedBucket, keyName) +
                ". Provided Part info is { " + partName + ", " + partNumber +
                "}, whereas OM has partName " + omPartName,
                OMException.ResultCodes.INVALID_PART);
          }

          OmKeyInfo currentPartKeyInfo = OmKeyInfo
              .getFromProtobuf(partKeyInfo.getPartKeyInfo());

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
          currentKeyInfoGroup.getLocationList().forEach( omKeyLocationInfo -> {
            omKeyLocationInfo.setPartNumber(partNumber);
          });

          partLocationInfos.addAll(currentKeyInfoGroup.getLocationList());
          dataSize += currentPartKeyInfo.getDataSize();
        }

        // All parts have same replication information. Here getting from last
        // part.
        HddsProtos.ReplicationType type = partKeyInfoMap.lastEntry().getValue()
            .getPartKeyInfo().getType();
        HddsProtos.ReplicationFactor factor =
            partKeyInfoMap.lastEntry().getValue().getPartKeyInfo().getFactor();

        OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);
        if (omKeyInfo == null) {
          // This is a newly added key, it does not have any versions.
          OmKeyLocationInfoGroup keyLocationInfoGroup = new
              OmKeyLocationInfoGroup(0, partLocationInfos, true);

          // Get the objectID of the key from OpenKeyTable
          OmKeyInfo dbOpenKeyInfo = omMetadataManager.getOpenKeyTable()
              .get(multipartKey);

          // A newly created key, this is the first version.
          OmKeyInfo.Builder builder =
              new OmKeyInfo.Builder().setVolumeName(volumeName)
              .setBucketName(bucketName).setKeyName(keyName)
              .setReplicationFactor(factor).setReplicationType(type)
              .setCreationTime(keyArgs.getModificationTime())
              .setModificationTime(keyArgs.getModificationTime())
              .setDataSize(dataSize)
              .setFileEncryptionInfo(dbOpenKeyInfo.getFileEncryptionInfo())
              .setOmKeyLocationInfos(
                  Collections.singletonList(keyLocationInfoGroup))
              .setAcls(OzoneAclUtil.fromProtobuf(keyArgs.getAclsList()));
          // Check if db entry has ObjectID. This check is required because
          // it is possible that between multipart key uploads and complete,
          // we had an upgrade.
          if (dbOpenKeyInfo.getObjectID() != 0) {
            builder.setObjectID(dbOpenKeyInfo.getObjectID());
          }
          omKeyInfo = builder.build();
        } else {
          // Already a version exists, so we should add it as a new version.
          // But now as versioning is not supported, just following the commit
          // key approach. When versioning support comes, then we can uncomment
          // below code keyInfo.addNewVersion(locations);
          omKeyInfo.updateLocationInfoList(partLocationInfos, true);
          omKeyInfo.setModificationTime(keyArgs.getModificationTime());
          omKeyInfo.setDataSize(dataSize);
        }
        omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

        //Find all unused parts.
        List< OmKeyInfo > unUsedParts = new ArrayList<>();
        for (Map.Entry< Integer, PartKeyInfo > partKeyInfo :
            partKeyInfoMap.entrySet()) {
          if (!partNumbers.contains(partKeyInfo.getKey())) {
            unUsedParts.add(OmKeyInfo
                .getFromProtobuf(partKeyInfo.getValue().getPartKeyInfo()));
          }
        }

        updateCache(omMetadataManager, ozoneKey, multipartKey, omKeyInfo,
            trxnLogIndex);

        omResponse.setCompleteMultiPartUploadResponse(
            MultipartUploadCompleteResponse.newBuilder()
                .setVolume(requestedVolume)
                .setBucket(requestedBucket)
                .setKey(keyName)
                .setHash(DigestUtils.sha256Hex(keyName)));

        omClientResponse = new S3MultipartUploadCompleteResponse(
            omResponse.build(), multipartKey, omKeyInfo, unUsedParts);

        result = Result.SUCCESS;
      } else {
        throw new OMException(
            failureMessage(requestedVolume, requestedBucket, keyName) +
            " because of empty part list",
            OMException.ResultCodes.INVALID_REQUEST);
      }

    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new S3MultipartUploadCompleteResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK,
            volumeName, bucketName);
      }
    }

    auditMap.put(OzoneConsts.MULTIPART_LIST, partsList.toString());

    // audit log
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
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

    return omClientResponse;
  }

  private static String failureMessage(String volume, String bucket,
      String keyName) {
    return "Complete Multipart Upload Failed: volume: " +
        volume + " bucket: " + bucket + " key: " + keyName;
  }

  private void updateCache(OMMetadataManager omMetadataManager,
      String ozoneKey, String multipartKey, OmKeyInfo omKeyInfo,
      long transactionLogIndex) {
    // Update cache.
    // 1. Add key entry to key table.
    // 2. Delete multipartKey entry from openKeyTable and multipartInfo table.
    omMetadataManager.getKeyTable().addCacheEntry(
        new CacheKey<>(ozoneKey),
        new CacheValue<>(Optional.of(omKeyInfo), transactionLogIndex));

    omMetadataManager.getOpenKeyTable().addCacheEntry(
        new CacheKey<>(multipartKey),
        new CacheValue<>(Optional.absent(), transactionLogIndex));
    omMetadataManager.getMultipartInfoTable().addCacheEntry(
        new CacheKey<>(multipartKey),
        new CacheValue<>(Optional.absent(), transactionLogIndex));
  }
}

