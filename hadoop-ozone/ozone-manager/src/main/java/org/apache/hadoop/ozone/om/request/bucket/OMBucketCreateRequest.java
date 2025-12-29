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

package org.apache.hadoop.ozone.om.request.bucket;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.om.helpers.OzoneAclUtil.getDefaultAclList;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.VOLUME_LOCK;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.List;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.common.BekInfoUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.AclListBuilder;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.OMClientVersionValidator;
import org.apache.hadoop.ozone.om.request.validation.OMLayoutVersionValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketCreateResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    super.preExecute(ozoneManager);

    // Get original request.
    CreateBucketRequest createBucketRequest =
        getOmRequest().getCreateBucketRequest();
    BucketInfo bucketInfo = createBucketRequest.getBucketInfo();
    // Verify resource name
    OmUtils.validateBucketName(bucketInfo.getBucketName(),
        ozoneManager.isStrictS3());

    // ACL check during preExecute
    if (ozoneManager.getAclsEnabled()) {
      try {
        checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.CREATE,
            bucketInfo.getVolumeName(), bucketInfo.getBucketName(), null);
      } catch (IOException ex) {
        // Ensure audit log captures preExecute failures
        markForAudit(ozoneManager.getAuditLogger(),
            buildAuditMessage(OMAction.CREATE_BUCKET,
                buildVolumeAuditMap(bucketInfo.getVolumeName()), ex,
                getOmRequest().getUserInfo()));
        throw ex;
      }
    }

    validateMaxBucket(ozoneManager);

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
      newBucketInfo.setBeinfo(
          BekInfoUtils.getBekInfo(kmsProvider, bucketInfo.getBeinfo()));
    }

    boolean hasSourceVolume = bucketInfo.hasSourceVolume();
    boolean hasSourceBucket = bucketInfo.hasSourceBucket();

    if (hasSourceBucket != hasSourceVolume) {
      throw new OMException("Both source volume and source bucket are " +
          "required for bucket links",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (hasSourceBucket && bucketInfo.hasBeinfo()) {
      throw new OMException("Encryption cannot be set for bucket links",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (bucketInfo.hasDefaultReplicationConfig()) {
      DefaultReplicationConfig drc = DefaultReplicationConfig.fromProto(
          bucketInfo.getDefaultReplicationConfig());
      ozoneManager.validateReplicationConfig(drc.getReplicationConfig());
    }

    newCreateBucketRequest.setBucketInfo(newBucketInfo.build());

    return getOmRequest().toBuilder().setUserInfo(getUserInfo())
        .setCreateBucketRequest(newCreateBucketRequest.build()).build();
  }

  private static void validateMaxBucket(OzoneManager ozoneManager)
      throws IOException {
    int maxBucket = ozoneManager.getConfiguration().getInt(
        OMConfigKeys.OZONE_OM_MAX_BUCKET,
        OMConfigKeys.OZONE_OM_MAX_BUCKET_DEFAULT);
    if (maxBucket <= 0) {
      maxBucket = OMConfigKeys.OZONE_OM_MAX_BUCKET_DEFAULT;
    }
    long nrOfBuckets = ozoneManager.getMetadataManager().getBucketTable()
        .getEstimatedKeyCount();
    if (nrOfBuckets >= maxBucket) {
      throw new OMException("Cannot create more than " + maxBucket
          + " buckets",
          ResultCodes.TOO_MANY_BUCKETS);
    }
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();
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
    OmBucketInfo omBucketInfo = null;

    // bucketInfo.hasBucketLayout() would be true when the request proto
    // specifies a bucket layout.
    // When the value is not specified in the proto, OM will use
    // "ozone.default.bucket.layout".
    if (!bucketInfo.hasBucketLayout()) {
      BucketLayout defaultBucketLayout =
          ozoneManager.getOMDefaultBucketLayout();
      omBucketInfo =
          OmBucketInfo.getFromProtobuf(bucketInfo, defaultBucketLayout);
    } else {
      omBucketInfo = OmBucketInfo.getFromProtobuf(bucketInfo);
    }

    if (omBucketInfo.getBucketLayout().isFileSystemOptimized()) {
      omMetrics.incNumFSOBucketCreates();
    }
    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    String volumeKey = metadataManager.getVolumeKey(volumeName);
    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
    Exception exception = null;
    boolean acquiredBucketLock = false;
    boolean acquiredVolumeLock = false;
    OMClientResponse omClientResponse = null;

    try {
      mergeOmLockDetails(
          metadataManager.getLock().acquireReadLock(VOLUME_LOCK, volumeName));
      acquiredVolumeLock = getOmLockDetails().isLockAcquired();

      mergeOmLockDetails(metadataManager.getLock().acquireWriteLock(
          BUCKET_LOCK, volumeName, bucketName));
      acquiredBucketLock = getOmLockDetails().isLockAcquired();

      OmVolumeArgs omVolumeArgs =
          metadataManager.getVolumeTable().getReadCopy(volumeKey);
      //Check if the volume exists
      if (omVolumeArgs == null) {
        LOG.debug("volume: {} not found ", volumeName);
        throw new OMException("Volume doesn't exist", VOLUME_NOT_FOUND);
      }

      //Check if bucket already exists
      if (metadataManager.getBucketTable().isExist(bucketKey)) {
        LOG.debug("bucket: {} already exists ", bucketName);
        throw new OMException("Bucket already exist", BUCKET_ALREADY_EXISTS);
      }

      //Check quotaInBytes to update
      if (!bucketInfo.hasSourceBucket()) {
        checkQuotaBytesValid(metadataManager, omVolumeArgs, omBucketInfo,
            volumeKey);
      }

      // Add objectID and updateID
      OmBucketInfo.Builder builder = omBucketInfo.toBuilder()
          .setObjectID(ozoneManager.getObjectIdFromTxId(transactionLogIndex))
          .setUpdateID(transactionLogIndex);

      addDefaultAcls(builder.acls(), omVolumeArgs, ozoneManager);

      omBucketInfo = builder.build();

      // check namespace quota
      checkQuotaInNamespace(omVolumeArgs, 1L);

      // update used namespace for volume
      omVolumeArgs = omVolumeArgs.toBuilder()
          .incrUsedNamespace(1L)
          .build();

      // Update table cache.
      metadataManager.getVolumeTable().addCacheEntry(new CacheKey<>(volumeKey),
          CacheValue.get(transactionLogIndex, omVolumeArgs));
      metadataManager.getBucketTable().addCacheEntry(new CacheKey<>(bucketKey),
          CacheValue.get(transactionLogIndex, omBucketInfo));

      omResponse.setCreateBucketResponse(
          CreateBucketResponse.newBuilder().build());
      omClientResponse = new OMBucketCreateResponse(omResponse.build(),
          omBucketInfo, omVolumeArgs.copyObject());
    } catch (IOException | InvalidPathException ex) {
      exception = ex;
      omClientResponse = new OMBucketCreateResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (acquiredBucketLock) {
        mergeOmLockDetails(
            metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
                bucketName));
      }
      if (acquiredVolumeLock) {
        mergeOmLockDetails(
            metadataManager.getLock().releaseReadLock(VOLUME_LOCK, volumeName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    // Performing audit logging outside of the lock.
    markForAudit(auditLogger, buildAuditMessage(OMAction.CREATE_BUCKET,
        omBucketInfo.toAuditMap(), exception, userInfo));

    // return response.
    if (exception == null) {
      LOG.info("created bucket: {} of layout {} in volume: {}", bucketName,
          omBucketInfo.getBucketLayout(), volumeName);
      omMetrics.incNumBuckets();
      if (isECBucket(bucketInfo)) {
        omMetrics.incEcBucketsTotal();
      }
      return omClientResponse;
    } else {
      omMetrics.incNumBucketCreateFails();
      if (isECBucket(bucketInfo)) {
        omMetrics.incEcBucketCreateFailsTotal();
      }
      LOG.error("Bucket creation failed for bucket:{} in volume:{}",
          bucketName, volumeName, exception);
      return omClientResponse;
    }
  }

  private boolean isECBucket(BucketInfo bucketInfo) {
    return bucketInfo.hasDefaultReplicationConfig() && bucketInfo
        .getDefaultReplicationConfig().hasEcReplicationConfig();
  }

  /**
   * Add default acls for bucket. These acls are inherited from volume
   * default acl list.
   */
  private void addDefaultAcls(AclListBuilder bucketAcls,
      OmVolumeArgs omVolumeArgs, OzoneManager ozoneManager) throws OMException {
    // Add default acls
    List<OzoneAcl> defaultAclList = getDefaultAclList(createUGIForApi(), ozoneManager.getConfig());
    if (ozoneManager.getConfig().ignoreClientACLs()) {
      bucketAcls.set(defaultAclList);
    } else {
      bucketAcls.addAll(defaultAclList);
    }

    // Add default acls from volume.
    List<OzoneAcl> defaultVolumeAcls = omVolumeArgs.getDefaultAcls();
    OzoneAclUtil.inheritDefaultAcls(bucketAcls, defaultVolumeAcls, ACCESS);
  }

  /**
   * Check namespace quota.
   */
  private void checkQuotaInNamespace(OmVolumeArgs omVolumeArgs,
      long allocatedNamespace) throws IOException {
    if (omVolumeArgs.getQuotaInNamespace() > 0) {
      long usedNamespace = omVolumeArgs.getUsedNamespace();
      long quotaInNamespace = omVolumeArgs.getQuotaInNamespace();
      long toUseNamespaceInTotal = usedNamespace + allocatedNamespace;
      if (quotaInNamespace < toUseNamespaceInTotal) {
        throw new OMException("The namespace quota of Volume:"
            + omVolumeArgs.getVolume() + " exceeded: quotaInNamespace: "
            + quotaInNamespace + " but namespace consumed: "
            + toUseNamespaceInTotal + ".",
            OMException.ResultCodes.QUOTA_EXCEEDED);
      }
    }
  }

  public boolean checkQuotaBytesValid(OMMetadataManager metadataManager,
      OmVolumeArgs omVolumeArgs, OmBucketInfo omBucketInfo, String volumeKey)
      throws IOException {
    long quotaInBytes = omBucketInfo.getQuotaInBytes();
    long volumeQuotaInBytes = omVolumeArgs.getQuotaInBytes();

    // When volume quota is set, then its mandatory to have bucket quota
    if (volumeQuotaInBytes > 0) {
      if (quotaInBytes <= 0) {
        throw new OMException("Bucket space quota in this volume " +
            "should be set as volume space quota is already set.",
            OMException.ResultCodes.QUOTA_ERROR);
      }
    }

    long totalBucketQuota = 0;
    if (quotaInBytes > 0) {
      totalBucketQuota = quotaInBytes;
    } else {
      return false;
    }

    List<OmBucketInfo>  bucketList = metadataManager.listBuckets(
        omVolumeArgs.getVolume(), null, null, Integer.MAX_VALUE, false);
    for (OmBucketInfo bucketInfo : bucketList) {
      long nextQuotaInBytes = bucketInfo.getQuotaInBytes();
      if (nextQuotaInBytes > OzoneConsts.QUOTA_RESET) {
        totalBucketQuota += nextQuotaInBytes;
      }
    }
    if (volumeQuotaInBytes < totalBucketQuota
        && volumeQuotaInBytes != OzoneConsts.QUOTA_RESET) {
      throw new OMException("Total buckets quota in this volume " +
          "should not be greater than volume quota : the total space quota is" +
          " set to:" + totalBucketQuota + ". But the volume space quota is:" +
          volumeQuotaInBytes, OMException.ResultCodes.QUOTA_EXCEEDED);
    }
    return true;

  }

  @OMLayoutVersionValidator(
      applyBefore = OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CreateBucket
  )
  public static OMRequest disallowCreateBucketWithECReplicationConfig(
      OMRequest req, ValidationContext ctx) throws OMException {
    if (!ctx.versionManager()
        .isAllowed(OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT)) {
      if (req.getCreateBucketRequest()
          .getBucketInfo().hasDefaultReplicationConfig()
          && req.getCreateBucketRequest().getBucketInfo()
          .getDefaultReplicationConfig().hasEcReplicationConfig()) {
        throw new OMException("Cluster does not have the Erasure Coded"
            + " Storage support feature finalized yet, but the request contains"
            + " an Erasure Coded replication type. Rejecting the request,"
            + " please finalize the cluster upgrade and then try again.",
            OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
      }
    }
    return req;
  }

  @OMLayoutVersionValidator(
      applyBefore = OMLayoutFeature.BUCKET_LAYOUT_SUPPORT,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CreateBucket
  )
  public static OMRequest handleCreateBucketWithBucketLayoutDuringPreFinalize(
      OMRequest req, ValidationContext ctx) throws OMException {
    if (!ctx.versionManager()
        .isAllowed(OMLayoutFeature.BUCKET_LAYOUT_SUPPORT)) {
      if (req.getCreateBucketRequest()
          .getBucketInfo().hasBucketLayout()) {
        // If the client explicitly specified a bucket layout while OM is
        // pre-finalized, reject the request.
        if (!BucketLayout.fromProto(req.getCreateBucketRequest().getBucketInfo()
            .getBucketLayout()).isLegacy()) {
          throw new OMException("Cluster does not have the Bucket Layout"
              + " support feature finalized yet, but the request contains"
              + " a non LEGACY bucket type. Rejecting the request,"
              + " please finalize the cluster upgrade and then try again.",
              ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
        }
      } else {
        // The client did not specify a bucket layout, meaning the OM's
        // default bucket layout would be used.
        // Since the OM is pre-finalized for bucket layout support,
        // explicitly override the server default by specifying this request
        // as a legacy bucket.
        return changeBucketLayout(req, BucketLayout.LEGACY);
      }
    }
    return req;
  }


  /**
   * When a client that does not support bucket layout types issues a create
   * bucket command, it will leave the bucket layout field empty. For these
   * old clients, they should create legacy buckets so that they can read and
   * write to them, instead of using the server default which may be in a layout
   * they do not understand.
   */
  @OMClientVersionValidator(
      applyBefore = ClientVersion.BUCKET_LAYOUT_SUPPORT,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.CreateBucket
  )
  public static OMRequest setDefaultBucketLayoutForOlderClients(OMRequest req,
      ValidationContext ctx) {
    if (ClientVersion.fromProtoValue(req.getVersion())
        .compareTo(ClientVersion.BUCKET_LAYOUT_SUPPORT) < 0) {
      // Older client will default bucket layout to LEGACY to
      // make its operations backward compatible.
      return changeBucketLayout(req, BucketLayout.LEGACY);
    } else {
      return req;
    }
  }

  private static OMRequest changeBucketLayout(OMRequest originalRequest,
      BucketLayout newLayout) {
    CreateBucketRequest createBucketRequest =
        originalRequest.getCreateBucketRequest();
    BucketInfo newBucketInfo = createBucketRequest.getBucketInfo().toBuilder()
        .setBucketLayout(newLayout.toProto()).build();
    CreateBucketRequest newCreateRequest = createBucketRequest.toBuilder()
        .setBucketInfo(newBucketInfo).build();
    return originalRequest.toBuilder()
        .setCreateBucketRequest(newCreateRequest).build();
  }
}
