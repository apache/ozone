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

package org.apache.hadoop.ozone.om.request.bucket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Optional;

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketCreateResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .BucketEncryptionInfoProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .BucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CryptoProtocolVersionProto.ENCRYPTION_ZONES;

/**
 * Handles CreateBucket Request.
 */
public class OMBucketCreateRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMBucketCreateRequest.class);

  public OMBucketCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }
  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    // Get original request.
    CreateBucketRequest createBucketRequest =
        getOmRequest().getCreateBucketRequest();
    BucketInfo bucketInfo = createBucketRequest.getBucketInfo();
    // Verify resource name
    OmUtils.validateBucketName(bucketInfo.getBucketName());

    // Get KMS provider.
    KeyProviderCryptoExtension kmsProvider =
        ozoneManager.getKmsProvider();

    // Create new Bucket request with new bucket info.
    CreateBucketRequest.Builder newCreateBucketRequest =
        createBucketRequest.toBuilder();

    BucketInfo.Builder newBucketInfo = bucketInfo.toBuilder();

    // Set creation time & modification time.
    long initialTime = Time.now();
    newBucketInfo.setCreationTime(initialTime)
        .setModificationTime(initialTime);

    if (bucketInfo.hasBeinfo()) {
      newBucketInfo.setBeinfo(getBeinfo(kmsProvider, bucketInfo));
    }

    newCreateBucketRequest.setBucketInfo(newBucketInfo.build());

    return getOmRequest().toBuilder().setUserInfo(getUserInfo())
       .setCreateBucketRequest(newCreateBucketRequest.build()).build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumBucketCreates();

    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();

    CreateBucketRequest createBucketRequest = getOmRequest()
        .getCreateBucketRequest();
    BucketInfo bucketInfo = createBucketRequest.getBucketInfo();

    String volumeName = bucketInfo.getVolumeName();
    String bucketName = bucketInfo.getBucketName();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OmBucketInfo omBucketInfo = OmBucketInfo.getFromProtobuf(bucketInfo);

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    String volumeKey = metadataManager.getVolumeKey(volumeName);
    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
    IOException exception = null;
    boolean acquiredBucketLock = false;
    boolean acquiredVolumeLock = false;
    OMClientResponse omClientResponse = null;

    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.CREATE,
            volumeName, bucketName, null);
      }

      acquiredVolumeLock =
          metadataManager.getLock().acquireReadLock(VOLUME_LOCK, volumeName);
      acquiredBucketLock = metadataManager.getLock().acquireWriteLock(
          BUCKET_LOCK, volumeName, bucketName);

      OmVolumeArgs omVolumeArgs =
          metadataManager.getVolumeTable().getReadCopy(volumeKey);
      //Check if the volume exists
      if (omVolumeArgs == null) {
        LOG.debug("volume: {} not found ", volumeName);
        throw new OMException("Volume doesn't exist",
            OMException.ResultCodes.VOLUME_NOT_FOUND);
      }

      //Check if bucket already exists
      OmBucketInfo dbBucketInfo = metadataManager.getBucketTable()
          .getReadCopy(bucketKey);
      if (dbBucketInfo != null) {
        // Check if this transaction is a replay of ratis logs.
        if (isReplay(ozoneManager, dbBucketInfo, transactionLogIndex)) {
          // Replay implies the response has already been returned to
          // the client. So take no further action and return a dummy
          // OMClientResponse.
          LOG.debug("Replayed Transaction {} ignored. Request: {}",
              transactionLogIndex, createBucketRequest);
          return new OMBucketCreateResponse(createReplayOMResponse(omResponse));
        } else {
          LOG.debug("bucket: {} already exists ", bucketName);
          throw new OMException("Bucket already exist",
              OMException.ResultCodes.BUCKET_ALREADY_EXISTS);
        }
      }

      // Add objectID and updateID
      omBucketInfo.setObjectID(
          OMFileRequest.getObjIDFromTxId(transactionLogIndex));
      omBucketInfo.setUpdateID(transactionLogIndex,
          ozoneManager.isRatisEnabled());

      // Add default acls from volume.
      addDefaultAcls(omBucketInfo, omVolumeArgs);


      // Update table cache.
      metadataManager.getBucketTable().addCacheEntry(new CacheKey<>(bucketKey),
          new CacheValue<>(Optional.of(omBucketInfo), transactionLogIndex));

      omResponse.setCreateBucketResponse(
          CreateBucketResponse.newBuilder().build());
      omClientResponse = new OMBucketCreateResponse(omResponse.build(),
          omBucketInfo);
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMBucketCreateResponse(
          createErrorOMResponse(omResponse, exception), omBucketInfo);
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredBucketLock) {
        metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
      if (acquiredVolumeLock) {
        metadataManager.getLock().releaseReadLock(VOLUME_LOCK, volumeName);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.CREATE_BUCKET,
        omBucketInfo.toAuditMap(), exception, userInfo));

    // return response.
    if (exception == null) {
      LOG.debug("created bucket: {} in volume: {}", bucketName, volumeName);
      omMetrics.incNumBuckets();
      return omClientResponse;
    } else {
      omMetrics.incNumBucketCreateFails();
      LOG.error("Bucket creation failed for bucket:{} in volume:{}",
          bucketName, volumeName, exception);
      return omClientResponse;
    }
  }


  /**
   * Add default acls for bucket. These acls are inherited from volume
   * default acl list.
   * @param omBucketInfo
   * @param omVolumeArgs
   */
  private void addDefaultAcls(OmBucketInfo omBucketInfo,
      OmVolumeArgs omVolumeArgs) {
    // Add default acls from volume.
    List<OzoneAcl> acls = new ArrayList<>();
    if (omBucketInfo.getAcls() != null) {
      acls.addAll(omBucketInfo.getAcls());
    }

    List<OzoneAcl> defaultVolumeAclList = omVolumeArgs.getAclMap()
        .getDefaultAclList().stream().map(OzoneAcl::fromProtobuf)
        .collect(Collectors.toList());

    OzoneAclUtil.inheritDefaultAcls(acls, defaultVolumeAclList);
    omBucketInfo.setAcls(acls);
  }

  private BucketEncryptionInfoProto getBeinfo(
      KeyProviderCryptoExtension kmsProvider, BucketInfo bucketInfo)
      throws IOException {
    BucketEncryptionInfoProto bek = bucketInfo.getBeinfo();
    BucketEncryptionInfoProto.Builder bekb = null;
    if (kmsProvider == null) {
      throw new OMException("Invalid KMS provider, check configuration " +
          CommonConfigurationKeys.HADOOP_SECURITY_KEY_PROVIDER_PATH,
          OMException.ResultCodes.INVALID_KMS_PROVIDER);
    }
    if (bek.getKeyName() == null) {
      throw new OMException("Bucket encryption key needed.", OMException
          .ResultCodes.BUCKET_ENCRYPTION_KEY_NOT_FOUND);
    }
    // Talk to KMS to retrieve the bucket encryption key info.
    KeyProvider.Metadata metadata = kmsProvider.getMetadata(
        bek.getKeyName());
    if (metadata == null) {
      throw new OMException("Bucket encryption key " + bek.getKeyName()
          + " doesn't exist.",
          OMException.ResultCodes.BUCKET_ENCRYPTION_KEY_NOT_FOUND);
    }
    // If the provider supports pool for EDEKs, this will fill in the pool
    kmsProvider.warmUpEncryptedKeys(bek.getKeyName());
    bekb = BucketEncryptionInfoProto.newBuilder()
        .setKeyName(bek.getKeyName())
        .setCryptoProtocolVersion(ENCRYPTION_ZONES)
        .setSuite(OMPBHelper.convert(
            CipherSuite.convert(metadata.getCipher())));
    return bekb.build();
  }
}
