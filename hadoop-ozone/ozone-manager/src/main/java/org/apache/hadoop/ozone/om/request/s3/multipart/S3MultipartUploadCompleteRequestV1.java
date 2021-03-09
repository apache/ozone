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

import com.google.common.base.Optional;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3MultipartUploadCompleteResponseV1;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadCompleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS;

/**
 * Handle Multipart upload complete request.
 */
public class S3MultipartUploadCompleteRequestV1
        extends S3MultipartUploadCompleteRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3MultipartUploadCompleteRequestV1.class);

  public S3MultipartUploadCompleteRequestV1(OMRequest omRequest) {
    super(omRequest);
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
    String dbMultipartKey;

    ozoneManager.getMetrics().incNumCompleteMultipartUploads();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    boolean acquiredLock = false;
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;
    IOException exception = null;
    Result result;
    try {
      keyArgs = resolveBucketLink(ozoneManager, keyArgs, auditMap);
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();

      acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
          volumeName, bucketName);

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      String fileName = OzoneFSUtils.getFileName(keyName);
      Path keyPath = Paths.get(keyName);
      OMFileRequest.OMPathInfoV1 pathInfoV1 =
              OMFileRequest.verifyDirectoryKeysInPath(omMetadataManager,
                      volumeName, bucketName, keyName, keyPath);
      long parentID = pathInfoV1.getLastKnownParentId();

      dbMultipartKey = omMetadataManager.getMultipartKey(parentID,
              fileName, uploadID);

      String dbOzoneKey = omMetadataManager.getOzonePathKey(parentID, fileName);

      String ozoneKey = omMetadataManager.getOzoneKey(
              volumeName, bucketName, keyName);

      OmMultipartKeyInfo multipartKeyInfo =
              omMetadataManager.getMultipartInfoTable().get(dbMultipartKey);

      // Check for directory exists with same name, if it exists throw error.
      if (pathInfoV1.getDirectoryResult() == DIRECTORY_EXISTS) {
        throw new OMException("Can not Complete MPU for file: " + keyName +
                " as there is already directory in the given path",
                NOT_A_FILE);
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
        List< Integer > partNumbers = new ArrayList<>();
        int partsListSize = getPartsListSize(requestedVolume,
                requestedBucket, keyName, ozoneKey, partNumbers, partsList);

        List<OmKeyLocationInfo> partLocationInfos = new ArrayList<>();
        long dataSize = getMultipartDataSize(requestedVolume, requestedBucket,
                keyName, ozoneKey, partKeyInfoMap, partsListSize,
                partLocationInfos, partsList, ozoneManager);

        // All parts have same replication information. Here getting from last
        // part.
        OmKeyInfo omKeyInfo = getOmKeyInfo(ozoneManager, trxnLogIndex, keyArgs,
                volumeName, bucketName, keyName, dbMultipartKey,
                omMetadataManager, dbOzoneKey, partKeyInfoMap,
                partLocationInfos, dataSize);

        //Find all unused parts.
        List< OmKeyInfo > unUsedParts = new ArrayList<>();
        for (Map.Entry< Integer, PartKeyInfo > partKeyInfo :
            partKeyInfoMap.entrySet()) {
          if (!partNumbers.contains(partKeyInfo.getKey())) {
            unUsedParts.add(OmKeyInfo
                .getFromProtobuf(partKeyInfo.getValue().getPartKeyInfo()));
          }
        }

        updateCache(omMetadataManager, dbOzoneKey, dbMultipartKey, omKeyInfo,
            trxnLogIndex);

        omResponse.setCompleteMultiPartUploadResponse(
            MultipartUploadCompleteResponse.newBuilder()
                .setVolume(requestedVolume)
                .setBucket(requestedBucket)
                .setKey(keyName)
                .setHash(DigestUtils.sha256Hex(keyName)));

        omClientResponse = new S3MultipartUploadCompleteResponseV1(
            omResponse.build(), dbMultipartKey, omKeyInfo, unUsedParts);

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
      omClientResponse = new S3MultipartUploadCompleteResponseV1(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK,
            volumeName, bucketName);
      }
    }

    logResult(ozoneManager, multipartUploadCompleteRequest, partsList,
            auditMap, volumeName, bucketName, keyName, exception, result);

    return omClientResponse;
  }

  protected OmKeyInfo getOmKeyInfoFromKeyTable(String dbOzoneFileKey,
      String keyName, OMMetadataManager omMetadataManager) throws IOException {
    return OMFileRequest.getOmKeyInfoFromFileTable(true,
            omMetadataManager, dbOzoneFileKey, keyName);
  }

  @Override
  protected OmKeyInfo getOmKeyInfoFromOpenKeyTable(String dbMultipartKey,
      String keyName, OMMetadataManager omMetadataManager) throws IOException {
    return OMFileRequest.getOmKeyInfoFromFileTable(true,
            omMetadataManager, dbMultipartKey, keyName);
  }

  @Override
  protected void updateCache(OMMetadataManager omMetadataManager,
      String ozoneKey, String multipartKey, OmKeyInfo omKeyInfo,
      long transactionLogIndex) {
    // Update cache.
    // 1. Add key entry to key table.
    // 2. Delete multipartKey entry from openKeyTable and multipartInfo table.
    OMFileRequest.addFileTableCacheEntry(omMetadataManager, ozoneKey,
            omKeyInfo, omKeyInfo.getFileName(), transactionLogIndex);

    omMetadataManager.getOpenKeyTable().addCacheEntry(
            new CacheKey<>(multipartKey),
            new CacheValue<>(Optional.absent(), transactionLogIndex));
    omMetadataManager.getMultipartInfoTable().addCacheEntry(
            new CacheKey<>(multipartKey),
            new CacheValue<>(Optional.absent(), transactionLogIndex));
  }

  @Override
  protected void updatePrefixFSOInfo(OmKeyInfo dbOpenKeyInfo,
                                     OmKeyInfo.Builder builder) {
    // updates parentID and fileName
    builder.setParentObjectID(dbOpenKeyInfo.getParentObjectID());
    builder.setFileName(dbOpenKeyInfo.getFileName());
  }

  @Override
  protected String preparePartName(String requestedVolume,
      String requestedBucket, String keyName, PartKeyInfo partKeyInfo,
      OMMetadataManager omMetadataManager) {

    String parentPath = OzoneFSUtils.getParent(keyName);
    StringBuffer keyPath = new StringBuffer(parentPath);
    String partFileName = OzoneFSUtils.getFileName(partKeyInfo.getPartName());
    keyPath.append(partFileName);

    return omMetadataManager.getOzoneKey(requestedVolume,
        requestedBucket, keyPath.toString());
  }


  private static String failureMessage(String volume, String bucket,
                                       String keyName) {
    return "Complete Multipart Upload Failed: volume: " +
        volume + " bucket: " + bucket + " key: " + keyName;
  }
}

