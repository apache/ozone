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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.lock.OzoneLockStrategy;
import org.apache.hadoop.ozone.om.request.OMClientRequestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension
    .EncryptedKeyVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ScmClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.READ;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.WRITE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes
    .BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes
    .VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * Interface for key write requests.
 */
public abstract class OMKeyRequest extends OMClientRequest {

  private static final Logger LOG = LoggerFactory.getLogger(OMKeyRequest.class);

  private BucketLayout bucketLayout = BucketLayout.DEFAULT;

  public OMKeyRequest(OMRequest omRequest) {
    super(omRequest);
  }

  public OMKeyRequest(OMRequest omRequest, BucketLayout bucketLayoutArg) {
    super(omRequest);
    this.bucketLayout = bucketLayoutArg;
  }

  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }

  protected KeyArgs resolveBucketLink(
      OzoneManager ozoneManager, KeyArgs keyArgs,
      Map<String, String> auditMap) throws IOException {
    ResolvedBucket bucket = ozoneManager.resolveBucketLink(keyArgs, this);
    keyArgs = bucket.update(keyArgs);
    bucket.audit(auditMap);
    return keyArgs;
  }

  /**
   * This methods avoids multiple rpc calls to SCM by allocating multiple blocks
   * in one rpc call.
   * @throws IOException
   */
  @SuppressWarnings("parameternumber")
  protected List< OmKeyLocationInfo > allocateBlock(ScmClient scmClient,
      OzoneBlockTokenSecretManager secretManager,
      ReplicationConfig replicationConfig, ExcludeList excludeList,
      long requestedSize, long scmBlockSize, int preallocateBlocksMax,
      boolean grpcBlockTokenEnabled, String omID) throws IOException {
    int dataGroupSize = replicationConfig instanceof ECReplicationConfig
        ? ((ECReplicationConfig) replicationConfig).getData() : 1;
    int numBlocks = (int) Math.min(preallocateBlocksMax,
        (requestedSize - 1) / (scmBlockSize * dataGroupSize) + 1);

    List<OmKeyLocationInfo> locationInfos = new ArrayList<>(numBlocks);
    String remoteUser = getRemoteUser().getShortUserName();
    List<AllocatedBlock> allocatedBlocks;
    try {
      allocatedBlocks = scmClient.getBlockClient()
          .allocateBlock(scmBlockSize, numBlocks, replicationConfig, omID,
              excludeList);
    } catch (SCMException ex) {
      if (ex.getResult()
          .equals(SCMException.ResultCodes.SAFE_MODE_EXCEPTION)) {
        throw new OMException(ex.getMessage(),
            OMException.ResultCodes.SCM_IN_SAFE_MODE);
      }
      throw ex;
    }
    for (AllocatedBlock allocatedBlock : allocatedBlocks) {
      BlockID blockID = new BlockID(allocatedBlock.getBlockID());
      OmKeyLocationInfo.Builder builder = new OmKeyLocationInfo.Builder()
          .setBlockID(blockID)
          .setLength(scmBlockSize)
          .setOffset(0)
          .setPipeline(allocatedBlock.getPipeline());
      if (grpcBlockTokenEnabled) {
        builder.setToken(secretManager.generateToken(remoteUser, blockID,
            EnumSet.of(READ, WRITE), scmBlockSize));
      }
      locationInfos.add(builder.build());
    }
    return locationInfos;
  }

  /* Optimize ugi lookup for RPC operations to avoid a trip through
   * UGI.getCurrentUser which is synch'ed.
   */
  private UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = Server.getRemoteUser();
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
  }

  /**
   * Validate bucket and volume exists or not.
   * @param omMetadataManager
   * @param volumeName
   * @param bucketName
   * @throws IOException
   */
  public void validateBucketAndVolume(OMMetadataManager omMetadataManager,
      String volumeName, String bucketName)
      throws IOException {
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    // Check if bucket exists
    if (!omMetadataManager.getBucketTable().isExist(bucketKey)) {
      String volumeKey = omMetadataManager.getVolumeKey(volumeName);
      // If the volume also does not exist, we should throw volume not found
      // exception
      if (!omMetadataManager.getVolumeTable().isExist(volumeKey)) {
        throw new OMException("Volume not found " + volumeName,
            VOLUME_NOT_FOUND);
      }

      // if the volume exists but bucket does not exist, throw bucket not found
      // exception
      throw new OMException("Bucket not found " + bucketName, BUCKET_NOT_FOUND);
    }

    // Make sure associated bucket's layout matches the one associated with
    // the request.
    OMClientRequestUtils.checkClientRequestPrecondition(
        omMetadataManager.getBucketTable().get(bucketKey).getBucketLayout(),
        getBucketLayout());
  }

  // For keys batch delete and rename only
  protected String getVolumeOwner(OMMetadataManager omMetadataManager,
      String volumeName) throws IOException {
    String dbVolumeKey = omMetadataManager.getVolumeKey(volumeName);
    OmVolumeArgs volumeArgs =
        omMetadataManager.getVolumeTable().get(dbVolumeKey);
    if (volumeArgs == null) {
      throw new OMException("Volume not found " + volumeName,
          VOLUME_NOT_FOUND);
    }
    return volumeArgs.getOwnerName();
  }

  protected static Optional<FileEncryptionInfo> getFileEncryptionInfo(
      OzoneManager ozoneManager, OmBucketInfo bucketInfo) throws IOException {
    Optional<FileEncryptionInfo> encInfo = Optional.absent();
    BucketEncryptionKeyInfo ezInfo = bucketInfo.getEncryptionKeyInfo();
    if (ezInfo != null) {
      final String ezKeyName = ezInfo.getKeyName();
      EncryptedKeyVersion edek = generateEDEK(ozoneManager, ezKeyName);
      encInfo = Optional.of(new FileEncryptionInfo(ezInfo.getSuite(),
        ezInfo.getVersion(),
          edek.getEncryptedKeyVersion().getMaterial(),
          edek.getEncryptedKeyIv(), ezKeyName,
          edek.getEncryptionKeyVersionName()));
    }
    return encInfo;
  }

  private static EncryptedKeyVersion generateEDEK(OzoneManager ozoneManager,
      String ezKeyName) throws IOException {
    if (ezKeyName == null) {
      return null;
    }
    long generateEDEKStartTime = monotonicNow();
    EncryptedKeyVersion edek = SecurityUtil.doAsLoginUser(
        new PrivilegedExceptionAction<EncryptedKeyVersion >() {
          @Override
          public EncryptedKeyVersion run() throws IOException {
            try {
              return ozoneManager.getKmsProvider()
                  .generateEncryptedKey(ezKeyName);
            } catch (GeneralSecurityException e) {
              throw new IOException(e);
            }
          }
        });
    long generateEDEKTime = monotonicNow() - generateEDEKStartTime;
    LOG.debug("generateEDEK takes {} ms", generateEDEKTime);
    Preconditions.checkNotNull(edek);
    return edek;
  }

  protected List< OzoneAcl > getAclsForKey(KeyArgs keyArgs,
      OmBucketInfo bucketInfo, PrefixManager prefixManager) {
    List<OzoneAcl> acls = new ArrayList<>();

    if (keyArgs.getAclsList() != null) {
      acls.addAll(OzoneAclUtil.fromProtobuf(keyArgs.getAclsList()));
    }

    // Inherit DEFAULT acls from prefix.
    if (prefixManager != null) {
      List< OmPrefixInfo > prefixList = prefixManager.getLongestPrefixPath(
          OZONE_URI_DELIMITER +
              keyArgs.getVolumeName() + OZONE_URI_DELIMITER +
              keyArgs.getBucketName() + OZONE_URI_DELIMITER +
              keyArgs.getKeyName());

      if (prefixList.size() > 0) {
        // Add all acls from direct parent to key.
        OmPrefixInfo prefixInfo = prefixList.get(prefixList.size() - 1);
        if (prefixInfo  != null) {
          if (OzoneAclUtil.inheritDefaultAcls(acls, prefixInfo.getAcls())) {
            return acls;
          }
        }
      }
    }

    // Inherit DEFAULT acls from bucket only if DEFAULT acls for
    // prefix are not set.
    if (bucketInfo != null) {
      if (OzoneAclUtil.inheritDefaultAcls(acls, bucketInfo.getAcls())) {
        return acls;
      }
    }

    return acls;
  }

  /**
   * Check Acls for the ozone bucket.
   * @param ozoneManager
   * @param volume
   * @param bucket
   * @param key
   * @throws IOException
   */
  protected void checkBucketAcls(OzoneManager ozoneManager, String volume,
      String bucket, String key, IAccessAuthorizer.ACLType aclType)
      throws IOException {
    if (ozoneManager.getAclsEnabled()) {
      checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET,
          OzoneObj.StoreType.OZONE, aclType,
          volume, bucket, key);
    }
  }

  /**
   * Check Acls for the ozone key.
   * @param ozoneManager
   * @param volume
   * @param bucket
   * @param key
   * @param aclType
   * @param resourceType
   * @throws IOException
   */
  protected void checkKeyAcls(OzoneManager ozoneManager, String volume,
      String bucket, String key, IAccessAuthorizer.ACLType aclType,
      OzoneObj.ResourceType resourceType)
      throws IOException {
    if (ozoneManager.getAclsEnabled()) {
      checkAcls(ozoneManager, resourceType, OzoneObj.StoreType.OZONE, aclType,
          volume, bucket, key);
    }
  }

  /**
   * Check Acls for the ozone key with volumeOwner.
   * @param ozoneManager
   * @param volume
   * @param bucket
   * @param key
   * @param aclType
   * @param resourceType
   * @throws IOException
   */
  @SuppressWarnings("parameternumber")
  protected void checkKeyAcls(OzoneManager ozoneManager, String volume,
      String bucket, String key, IAccessAuthorizer.ACLType aclType,
      OzoneObj.ResourceType resourceType, String volumeOwner)
      throws IOException {
    if (ozoneManager.getAclsEnabled()) {
      checkAcls(ozoneManager, resourceType, OzoneObj.StoreType.OZONE,
          aclType, volume, bucket, key, volumeOwner,
          ozoneManager.getBucketOwner(volume, bucket, aclType, resourceType));
    }
  }

  /**
   * Check ACLs for Ozone Key in OpenKey table
   * if ozone native authorizer is enabled.
   * @param ozoneManager
   * @param volume
   * @param bucket
   * @param key
   * @param aclType
   * @param clientId
   * @throws IOException
   */
  protected void checkKeyAclsInOpenKeyTable(OzoneManager ozoneManager,
      String volume, String bucket, String key,
      IAccessAuthorizer.ACLType aclType, long clientId) throws IOException {
    String keyNameForAclCheck = key;
    // Native authorizer requires client id as part of key name to check
    // write ACL on key. Add client id to key name if ozone native
    // authorizer is configured.
    if (ozoneManager.isNativeAuthorizerEnabled()) {
      keyNameForAclCheck = key + "/" + clientId;
    }

    checkKeyAcls(ozoneManager, volume, bucket, keyNameForAclCheck,
          aclType, OzoneObj.ResourceType.KEY);
  }

  /**
   * Generate EncryptionInfo and set in to newKeyArgs.
   * @param keyArgs
   * @param newKeyArgs
   * @param ozoneManager
   */
  protected void generateRequiredEncryptionInfo(KeyArgs keyArgs,
      KeyArgs.Builder newKeyArgs, OzoneManager ozoneManager)
      throws IOException {

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();

    boolean acquireLock = false;
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    // When TDE is enabled, we are doing a DB read in pre-execute. As for
    // most of the operations we don't read from DB because of our isLeader
    // semantics. This issue will be solved with implementation of leader
    // leases which provider strong leader semantics in the system.

    // If KMS is not enabled, follow the normal approach of execution of not
    // reading DB in pre-execute.

    OmBucketInfo bucketInfo = null;
    if (ozoneManager.getKmsProvider() != null) {
      try {
        acquireLock = omMetadataManager.getLock().acquireReadLock(
            BUCKET_LOCK, volumeName, bucketName);

        bucketInfo = omMetadataManager.getBucketTable().get(
            omMetadataManager.getBucketKey(volumeName,
                bucketName));

        // If bucket is symlink, resolveBucketLink to figure out real
        // volume/bucket.
        if (bucketInfo.isLink()) {
          ResolvedBucket resolvedBucket = ozoneManager.resolveBucketLink(
              Pair.of(keyArgs.getVolumeName(), keyArgs.getBucketName()));

          bucketInfo = omMetadataManager.getBucketTable().get(
              omMetadataManager.getBucketKey(resolvedBucket.realVolume(),
                  resolvedBucket.realBucket()));
        }

      } finally {
        if (acquireLock) {
          omMetadataManager.getLock().releaseReadLock(
              BUCKET_LOCK, volumeName, bucketName);
        }
      }

      // Don't throw exception of bucket not found when bucketinfo is
      // null. If bucketinfo is null, later when request
      // is submitted and if bucket does not really exist it will fail in
      // applyTransaction step. Why we are doing this is if OM thinks it is
      // the leader, but it is not, we don't want to fail request in this
      // case. As anyway when it submits request to ratis it will fail with
      // not leader exception, and client will retry on correct leader and
      // request will be executed.

      if (bucketInfo != null) {
        Optional<FileEncryptionInfo> encryptionInfo =
            getFileEncryptionInfo(ozoneManager, bucketInfo);
        if (encryptionInfo.isPresent()) {
          newKeyArgs.setFileEncryptionInfo(
              OMPBHelper.convert(encryptionInfo.get()));
        }
      }
    }
  }

  protected void getFileEncryptionInfoForMpuKey(KeyArgs keyArgs,
      KeyArgs.Builder newKeyArgs, OzoneManager ozoneManager)
      throws IOException {

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();

    boolean acquireLock = false;
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    if (ozoneManager.getKmsProvider() != null) {
      acquireLock = omMetadataManager.getLock().acquireReadLock(
          BUCKET_LOCK, volumeName, bucketName);
      try {
        ResolvedBucket resolvedBucket = ozoneManager.resolveBucketLink(
            Pair.of(keyArgs.getVolumeName(), keyArgs.getBucketName()));

        // Get the DB key name for looking up keyInfo in OpenKeyTable with
        // resolved volume/bucket.
        String dbMultipartOpenKey =
            getDBMultipartOpenKey(resolvedBucket.realVolume(),
                resolvedBucket.realBucket(), keyArgs.getKeyName(),
                keyArgs.getMultipartUploadID(), omMetadataManager);

        OmKeyInfo omKeyInfo =
            omMetadataManager.getOpenKeyTable(getBucketLayout())
                .get(dbMultipartOpenKey);

        if (omKeyInfo != null && omKeyInfo.getFileEncryptionInfo() != null) {
          newKeyArgs.setFileEncryptionInfo(
              OMPBHelper.convert(omKeyInfo.getFileEncryptionInfo()));
        }
      } finally {
        if (acquireLock) {
          omMetadataManager.getLock()
              .releaseReadLock(BUCKET_LOCK, volumeName, bucketName);
        }
      }
    }
  }

  /**
   * Get FileEncryptionInfoProto from KeyArgs.
   * @param keyArgs
   * @return
   */
  protected FileEncryptionInfo getFileEncryptionInfo(KeyArgs keyArgs) {
    FileEncryptionInfo encryptionInfo = null;
    if (keyArgs.hasFileEncryptionInfo()) {
      encryptionInfo = OMPBHelper.convert(keyArgs.getFileEncryptionInfo());
    }
    return encryptionInfo;
  }

  /**
   * Check bucket quota in bytes.
   * @param omBucketInfo
   * @param allocateSize
   * @throws IOException
   */
  protected void checkBucketQuotaInBytes(OmBucketInfo omBucketInfo,
      long allocateSize) throws IOException {
    if (omBucketInfo.getQuotaInBytes() > OzoneConsts.QUOTA_RESET) {
      long usedBytes = omBucketInfo.getUsedBytes();
      long quotaInBytes = omBucketInfo.getQuotaInBytes();
      if (quotaInBytes - usedBytes < allocateSize) {
        throw new OMException("The DiskSpace quota of bucket:"
            + omBucketInfo.getBucketName() + "exceeded: quotaInBytes: "
            + quotaInBytes + " Bytes but diskspace consumed: " + (usedBytes
            + allocateSize) + " Bytes.",
            OMException.ResultCodes.QUOTA_EXCEEDED);
      }
    }
  }

  /**
   * Check namespace quota.
   */
  protected void checkBucketQuotaInNamespace(OmBucketInfo omBucketInfo,
      long allocatedNamespace) throws IOException {
    if (omBucketInfo.getQuotaInNamespace() > OzoneConsts.QUOTA_RESET) {
      long usedNamespace = omBucketInfo.getUsedNamespace();
      long quotaInNamespace = omBucketInfo.getQuotaInNamespace();
      long toUseNamespaceInTotal = usedNamespace + allocatedNamespace;
      if (quotaInNamespace < toUseNamespaceInTotal) {
        throw new OMException("The namespace quota of Bucket:"
            + omBucketInfo.getBucketName() + " exceeded: quotaInNamespace: "
            + quotaInNamespace + " but namespace consumed: "
            + toUseNamespaceInTotal + ".",
            OMException.ResultCodes.QUOTA_EXCEEDED);
      }
    }
  }

  /**
   * Check directory exists. If exists return true, else false.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param omMetadataManager
   * @throws IOException
   */
  protected boolean checkDirectoryAlreadyExists(String volumeName,
      String bucketName, String keyName, OMMetadataManager omMetadataManager)
      throws IOException {
    if (omMetadataManager.getKeyTable(getBucketLayout()).isExist(
        omMetadataManager.getOzoneDirKey(volumeName, bucketName,
            keyName))) {
      return true;
    }
    return false;
  }

  /**
   * @return the number of bytes used by blocks pointed to by {@code omKeyInfo}.
   */
  protected static long sumBlockLengths(OmKeyInfo omKeyInfo) {
    long bytesUsed = 0;
    for (OmKeyLocationInfoGroup group: omKeyInfo.getKeyLocationVersions()) {
      for (OmKeyLocationInfo locationInfo : group.getLocationList()) {
        bytesUsed += QuotaUtil.getReplicatedSize(
            locationInfo.getLength(), omKeyInfo.getReplicationConfig());
      }
    }

    return bytesUsed;
  }

  /**
   * Return bucket info for the specified bucket.
   */
  @Nullable
  protected OmBucketInfo getBucketInfo(OMMetadataManager omMetadataManager,
      String volume, String bucket) {
    String bucketKey = omMetadataManager.getBucketKey(volume, bucket);

    CacheValue<OmBucketInfo> value = omMetadataManager.getBucketTable()
        .getCacheValue(new CacheKey<>(bucketKey));

    return value != null ? value.getCacheValue() : null;
  }

  /**
   * Prepare OmKeyInfo which will be persisted to openKeyTable.
   * @return OmKeyInfo
   * @throws IOException
   */
  @SuppressWarnings("parameternumber")
  protected OmKeyInfo prepareKeyInfo(
          @Nonnull OMMetadataManager omMetadataManager,
          @Nonnull KeyArgs keyArgs, OmKeyInfo dbKeyInfo, long size,
          @Nonnull List<OmKeyLocationInfo> locations,
          @Nullable FileEncryptionInfo encInfo,
          @Nonnull PrefixManager prefixManager,
          @Nullable OmBucketInfo omBucketInfo,
          long transactionLogIndex, long objectID, boolean isRatisEnabled,
          ReplicationConfig replicationConfig)
          throws IOException {

    return prepareFileInfo(omMetadataManager, keyArgs, dbKeyInfo, size,
            locations, encInfo, prefixManager, omBucketInfo, null,
            transactionLogIndex, objectID, isRatisEnabled, replicationConfig);
  }

  /**
   * Prepare OmKeyInfo which will be persisted to openKeyTable.
   * @return OmKeyInfo
   * @throws IOException
   */
  @SuppressWarnings("parameternumber")
  protected OmKeyInfo prepareFileInfo(
          @Nonnull OMMetadataManager omMetadataManager,
          @Nonnull KeyArgs keyArgs, OmKeyInfo dbKeyInfo, long size,
          @Nonnull List<OmKeyLocationInfo> locations,
          @Nullable FileEncryptionInfo encInfo,
          @Nonnull PrefixManager prefixManager,
          @Nullable OmBucketInfo omBucketInfo,
          OMFileRequest.OMPathInfoWithFSO omPathInfo,
          long transactionLogIndex, long objectID,
          boolean isRatisEnabled, ReplicationConfig replicationConfig)
          throws IOException {
    if (keyArgs.getIsMultipartKey()) {
      return prepareMultipartFileInfo(omMetadataManager, keyArgs,
              size, locations, encInfo, prefixManager, omBucketInfo,
              omPathInfo, transactionLogIndex, objectID);
      //TODO args.getMetadata
    }
    if (dbKeyInfo != null) {
      // The key already exist, the new blocks will replace old ones
      // as new versions unless the bucket does not have versioning
      // turned on.
      dbKeyInfo.addNewVersion(locations, false,
              omBucketInfo.getIsVersionEnabled());
      long newSize = size;
      if (omBucketInfo.getIsVersionEnabled()) {
        newSize += dbKeyInfo.getDataSize();
      }
      dbKeyInfo.setDataSize(newSize);
      // The modification time is set in preExecute. Use the same
      // modification time.
      dbKeyInfo.setModificationTime(keyArgs.getModificationTime());
      dbKeyInfo.setUpdateID(transactionLogIndex, isRatisEnabled);
      dbKeyInfo.setReplicationConfig(replicationConfig);
      return dbKeyInfo;
    }

    // the key does not exist, create a new object.
    // Blocks will be appended as version 0.
    return createFileInfo(keyArgs, locations, replicationConfig,
            keyArgs.getDataSize(), encInfo, prefixManager,
            omBucketInfo, omPathInfo, transactionLogIndex, objectID);
  }

  /**
   * Create OmKeyInfo object.
   * @return OmKeyInfo
   */
  @SuppressWarnings("parameterNumber")
  protected OmKeyInfo createFileInfo(@Nonnull KeyArgs keyArgs,
      @Nonnull List<OmKeyLocationInfo> locations,
      @Nonnull ReplicationConfig replicationConfig,
      long size,
      @Nullable FileEncryptionInfo encInfo,
      @Nonnull PrefixManager prefixManager,
      @Nullable OmBucketInfo omBucketInfo,
      OMFileRequest.OMPathInfoWithFSO omPathInfo,
      long transactionLogIndex, long objectID) {
    OmKeyInfo.Builder builder = new OmKeyInfo.Builder();
    builder.setVolumeName(keyArgs.getVolumeName())
            .setBucketName(keyArgs.getBucketName())
            .setKeyName(keyArgs.getKeyName())
            .setOmKeyLocationInfos(Collections.singletonList(
                    new OmKeyLocationInfoGroup(0, locations)))
            .setCreationTime(keyArgs.getModificationTime())
            .setModificationTime(keyArgs.getModificationTime())
            .setDataSize(size)
            .setReplicationConfig(replicationConfig)
            .setFileEncryptionInfo(encInfo)
            .setAcls(getAclsForKey(keyArgs, omBucketInfo, prefixManager))
            .addAllMetadata(KeyValueUtil.getFromProtobuf(
                    keyArgs.getMetadataList()))
            .setUpdateID(transactionLogIndex);
    if (omPathInfo != null) {
      // FileTable metadata format
      objectID = omPathInfo.getLeafNodeObjectId();
      builder.setParentObjectID(omPathInfo.getLastKnownParentId());
      builder.setFileName(omPathInfo.getLeafNodeName());
    }
    builder.setObjectID(objectID);
    return builder.build();
  }

  /**
   * Prepare OmKeyInfo for multi-part upload part key which will be persisted
   * to openKeyTable.
   * @return OmKeyInfo
   * @throws IOException
   */
  @SuppressWarnings("parameternumber")
  private OmKeyInfo prepareMultipartFileInfo(
          @Nonnull OMMetadataManager omMetadataManager,
          @Nonnull KeyArgs args, long size,
          @Nonnull List<OmKeyLocationInfo> locations,
          FileEncryptionInfo encInfo,  @Nonnull PrefixManager prefixManager,
          @Nullable OmBucketInfo omBucketInfo,
          OMFileRequest.OMPathInfoWithFSO omPathInfo,
          @Nonnull long transactionLogIndex, long objectID)
          throws IOException {

    Preconditions.checkArgument(args.getMultipartNumber() > 0,
            "PartNumber Should be greater than zero");
    // When key is multipart upload part key, we should take replication
    // type and replication factor from original key which has done
    // initiate multipart upload. If we have not found any such, we throw
    // error no such multipart upload.
    String uploadID = args.getMultipartUploadID();
    Preconditions.checkNotNull(uploadID);
    String multipartKey = "";
    if (omPathInfo != null) {
      final long volumeId = omMetadataManager.getVolumeId(
              args.getVolumeName());
      final long bucketId = omMetadataManager.getBucketId(
              args.getVolumeName(), args.getBucketName());
      // FileTable metadata format
      multipartKey = omMetadataManager.getMultipartKey(volumeId, bucketId,
              omPathInfo.getLastKnownParentId(),
              omPathInfo.getLeafNodeName(), uploadID);
    } else {
      multipartKey = omMetadataManager
              .getMultipartKey(args.getVolumeName(), args.getBucketName(),
                      args.getKeyName(), uploadID);
    }
    OmKeyInfo partKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(multipartKey);
    if (partKeyInfo == null) {
      throw new OMException("No such Multipart upload is with specified " +
              "uploadId " + uploadID,
              OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
    }
    // For this upload part we don't need to check in KeyTable. As this
    // is not an actual key, it is a part of the key.
    return createFileInfo(args, locations, partKeyInfo.getReplicationConfig(),
            size, encInfo, prefixManager, omBucketInfo, omPathInfo,
            transactionLogIndex, objectID);
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
  protected String getDBMultipartOpenKey(String volumeName, String bucketName,
                                         String keyName, String uploadID,
                                         OMMetadataManager omMetadataManager)
      throws IOException {

    return omMetadataManager
        .getMultipartKey(volumeName, bucketName, keyName, uploadID);
  }

  /**
   * Prepare key for deletion service on overwrite.
   *
   * @param dbOzoneKey key to point to an object in RocksDB
   * @param keyToDelete OmKeyInfo of a key to be in deleteTable
   * @param omMetadataManager
   * @param trxnLogIndex
   * @param isRatisEnabled
   * @return Old keys eligible for deletion.
   * @throws IOException
   */
  protected RepeatedOmKeyInfo getOldVersionsToCleanUp(
      @Nonnull String dbOzoneKey, @Nonnull OmKeyInfo keyToDelete,
      OMMetadataManager omMetadataManager, long trxnLogIndex,
      boolean isRatisEnabled) throws IOException {

    // Past keys that was deleted but still in deleted table,
    // waiting for deletion service.
    RepeatedOmKeyInfo keysToDelete =
        omMetadataManager.getDeletedTable().get(dbOzoneKey);

    return OmUtils.prepareKeyForDelete(keyToDelete, keysToDelete,
          trxnLogIndex, isRatisEnabled);
  }

  protected OzoneLockStrategy getOzoneLockStrategy(OzoneManager ozoneManager) {
    return ozoneManager.getOzoneLockProvider()
        .createLockStrategy(getBucketLayout());
  }
}
