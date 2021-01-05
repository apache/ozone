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
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension
    .EncryptedKeyVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
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
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
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

  public OMKeyRequest(OMRequest omRequest) {
    super(omRequest);
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
      HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor,
      ExcludeList excludeList, long requestedSize, long scmBlockSize,
      int preallocateBlocksMax, boolean grpcBlockTokenEnabled, String omID)
      throws IOException {

    int numBlocks = Math.min((int) ((requestedSize - 1) / scmBlockSize + 1),
        preallocateBlocksMax);

    List<OmKeyLocationInfo> locationInfos = new ArrayList<>(numBlocks);
    String remoteUser = getRemoteUser().getShortUserName();
    List<AllocatedBlock> allocatedBlocks;
    try {
      allocatedBlocks = scmClient.getBlockClient()
          .allocateBlock(scmBlockSize, numBlocks, replicationType,
              replicationFactor, omID, excludeList);
    } catch (SCMException ex) {
      if (ex.getResult()
          .equals(SCMException.ResultCodes.SAFE_MODE_EXCEPTION)) {
        throw new OMException(ex.getMessage(),
            OMException.ResultCodes.SCM_IN_SAFE_MODE);
      }
      throw ex;
    }
    for (AllocatedBlock allocatedBlock : allocatedBlocks) {
      OmKeyLocationInfo.Builder builder = new OmKeyLocationInfo.Builder()
          .setBlockID(new BlockID(allocatedBlock.getBlockID()))
          .setLength(scmBlockSize)
          .setOffset(0)
          .setPipeline(allocatedBlock.getPipeline());
      if (grpcBlockTokenEnabled) {
        builder.setToken(secretManager
            .generateToken(remoteUser, allocatedBlock.getBlockID().toString(),
                EnumSet.of(READ, WRITE),
                scmBlockSize));
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

  /**
   * Create OmKeyInfo object.
   * @return OmKeyInfo
   */
  @SuppressWarnings("parameterNumber")
  protected OmKeyInfo createKeyInfo(@Nonnull KeyArgs keyArgs,
      @Nonnull List<OmKeyLocationInfo> locations,
      @Nonnull HddsProtos.ReplicationFactor factor,
      @Nonnull HddsProtos.ReplicationType type, long size,
      @Nullable FileEncryptionInfo encInfo,
      @Nonnull PrefixManager prefixManager,
      @Nullable OmBucketInfo omBucketInfo,
      long transactionLogIndex, long objectID) {
    return new OmKeyInfo.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, locations)))
        .setCreationTime(keyArgs.getModificationTime())
        .setModificationTime(keyArgs.getModificationTime())
        .setDataSize(size)
        .setReplicationType(type)
        .setReplicationFactor(factor)
        .setFileEncryptionInfo(encInfo)
        .setAcls(getAclsForKey(keyArgs, omBucketInfo, prefixManager))
        .addAllMetadata(KeyValueUtil.getFromProtobuf(keyArgs.getMetadataList()))
        .setObjectID(objectID)
        .setUpdateID(transactionLogIndex)
        .build();
  }

  private List< OzoneAcl > getAclsForKey(KeyArgs keyArgs,
      OmBucketInfo bucketInfo, PrefixManager prefixManager) {
    List<OzoneAcl> acls = new ArrayList<>();

    if(keyArgs.getAclsList() != null) {
      acls.addAll(OzoneAclUtil.fromProtobuf(keyArgs.getAclsList()));
    }

    // Inherit DEFAULT acls from prefix.
    if(prefixManager != null) {
      List< OmPrefixInfo > prefixList = prefixManager.getLongestPrefixPath(
          OZONE_URI_DELIMITER +
              keyArgs.getVolumeName() + OZONE_URI_DELIMITER +
              keyArgs.getBucketName() + OZONE_URI_DELIMITER +
              keyArgs.getKeyName());

      if(prefixList.size() > 0) {
        // Add all acls from direct parent to key.
        OmPrefixInfo prefixInfo = prefixList.get(prefixList.size() - 1);
        if(prefixInfo  != null) {
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
      long transactionLogIndex,
      @Nonnull long objectID,
      boolean isRatisEnabled)
      throws IOException {
    if (keyArgs.getIsMultipartKey()) {
      return prepareMultipartKeyInfo(omMetadataManager, keyArgs,
          size, locations, encInfo, prefixManager, omBucketInfo,
          transactionLogIndex, objectID);
      //TODO args.getMetadata
    }
    if (dbKeyInfo != null) {
      // TODO: Need to be fixed, as when key already exists, we are
      //  appending new blocks to existing key.
      // The key already exist, the new blocks will be added as new version
      // when locations.size = 0, the new version will have identical blocks
      // as its previous version
      dbKeyInfo.addNewVersion(locations, false);
      dbKeyInfo.setDataSize(size + dbKeyInfo.getDataSize());
      // The modification time is set in preExecute. Use the same
      // modification time.
      dbKeyInfo.setModificationTime(keyArgs.getModificationTime());
      dbKeyInfo.setUpdateID(transactionLogIndex, isRatisEnabled);
      return dbKeyInfo;
    }

    // the key does not exist, create a new object.
    // Blocks will be appended as version 0.
    return createKeyInfo(keyArgs, locations, keyArgs.getFactor(),
        keyArgs.getType(), keyArgs.getDataSize(), encInfo, prefixManager,
        omBucketInfo, transactionLogIndex, objectID);
  }

  /**
   * Prepare OmKeyInfo for multi-part upload part key which will be persisted
   * to openKeyTable.
   * @return OmKeyInfo
   * @throws IOException
   */
  @SuppressWarnings("parameternumber")
  private OmKeyInfo prepareMultipartKeyInfo(
      @Nonnull OMMetadataManager omMetadataManager,
      @Nonnull KeyArgs args, long size,
      @Nonnull List<OmKeyLocationInfo> locations,
      FileEncryptionInfo encInfo,  @Nonnull PrefixManager prefixManager,
      @Nullable OmBucketInfo omBucketInfo, @Nonnull long transactionLogIndex,
      @Nonnull long objectId)
      throws IOException {
    HddsProtos.ReplicationFactor factor;
    HddsProtos.ReplicationType type;

    Preconditions.checkArgument(args.getMultipartNumber() > 0,
        "PartNumber Should be greater than zero");
    // When key is multipart upload part key, we should take replication
    // type and replication factor from original key which has done
    // initiate multipart upload. If we have not found any such, we throw
    // error no such multipart upload.
    String uploadID = args.getMultipartUploadID();
    Preconditions.checkNotNull(uploadID);
    String multipartKey = omMetadataManager
        .getMultipartKey(args.getVolumeName(), args.getBucketName(),
            args.getKeyName(), uploadID);
    OmKeyInfo partKeyInfo = omMetadataManager.getOpenKeyTable().get(
        multipartKey);
    if (partKeyInfo == null) {
      throw new OMException("No such Multipart upload is with specified " +
          "uploadId " + uploadID,
          OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
    } else {
      factor = partKeyInfo.getFactor();
      type = partKeyInfo.getType();
    }
    // For this upload part we don't need to check in KeyTable. As this
    // is not an actual key, it is a part of the key.
    return createKeyInfo(args, locations, factor, type, size, encInfo,
        prefixManager, omBucketInfo, transactionLogIndex, objectId);
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
      checkAcls(ozoneManager, resourceType, OzoneObj.StoreType.OZONE, aclType,
          volume, bucket, key, volumeOwner);
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
            omMetadataManager.getBucketKey(volumeName, bucketName));

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
    if (omMetadataManager.getKeyTable().isExist(
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
    int keyFactor = omKeyInfo.getFactor().getNumber();
    OmKeyLocationInfoGroup keyLocationGroup =
        omKeyInfo.getLatestVersionLocations();

    for(OmKeyLocationInfo locationInfo: keyLocationGroup.getLocationList()) {
      bytesUsed += locationInfo.getLength() * keyFactor;
    }

    return bytesUsed;
  }

  /**
   * Return bucket info for the specified bucket.
   * @param omMetadataManager
   * @param volume
   * @param bucket
   * @return OmVolumeArgs
   * @throws IOException
   */
  protected OmBucketInfo getBucketInfo(OMMetadataManager omMetadataManager,
      String volume, String bucket) {
    return omMetadataManager.getBucketTable().getCacheValue(
        new CacheKey<>(omMetadataManager.getBucketKey(volume, bucket)))
        .getCacheValue();
  }
}
