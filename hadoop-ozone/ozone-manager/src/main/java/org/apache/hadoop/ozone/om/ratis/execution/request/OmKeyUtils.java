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

package org.apache.hadoop.ozone.om.ratis.execution.request;

import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.InvalidPathException;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.IOmMetadataReader;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataReader;
import org.apache.hadoop.ozone.om.OzoneAclUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.apache.hadoop.ozone.om.ScmClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.helpers.QuotaResource;
import org.apache.hadoop.ozone.om.protocolPB.grpc.GrpcClientConstants;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.READ;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.WRITE;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConsts.OBJECT_ID_RECLAIM_BLOCKS;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DETECTED_LOOP_IN_BUCKET_LINKS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.UNAUTHORIZED;
import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * key utils for key operations.
 */
public final class OmKeyUtils {
  private static final Logger LOG = LoggerFactory.getLogger(OmKeyUtils.class);

  private OmKeyUtils() {
  }

  /**
   * resolve bucket with direct cached value.
   * @param metadataManager ozone manager metadata instance
   * @param volName volume name
   * @param buckName bucket name
   * @param visited list of volume and bucket visited
   * @return OmBucketInfo
   * @throws OMException exception
   */
  public static OmBucketInfo resolveBucketLink(
      OMMetadataManager metadataManager, String volName, String buckName, Set<Pair<String, String>> visited)
      throws OMException {
    String bucketKey = metadataManager.getBucketKey(volName, buckName);
    CacheValue<OmBucketInfo> value = metadataManager.getBucketTable().getCacheValue(new CacheKey<>(bucketKey));
    if (value == null || value.getCacheValue() == null) {
      throw new OMException("Bucket not found: " + volName + "/" + buckName, OMException.ResultCodes.BUCKET_NOT_FOUND);
    }
    // If this is a link bucket, we fetch the BucketLayout from the source bucket.
    if (value.getCacheValue().isLink()) {
      // Check if this bucket was already visited - to avoid loops
      if (!visited.add(Pair.of(volName, buckName))) {
        throw new OMException("Detected loop in bucket links. Bucket name: " + buckName + ", Volume name: " + volName,
            DETECTED_LOOP_IN_BUCKET_LINKS);
      }
      return resolveBucketLink(metadataManager, value.getCacheValue().getSourceVolume(),
          value.getCacheValue().getSourceBucket(), visited);
    }
    return value.getCacheValue();
  }

  public static void checkKeyAcls(
      OzoneManager ozoneManager, String volume, String bucket, String key, IAccessAuthorizer.ACLType aclType,
      OzoneObj.ResourceType resourceType, OMRequest omRequest) throws IOException {
    if (ozoneManager.getAclsEnabled()) {
      // 1. validate all bucket in link
      try (ReferenceCounted<IOmMetadataReader> rcMetadataReader = ozoneManager.getOmMetadataReader()) {
        Set<Pair<String, String>> bucketSet = new HashSet<>();
        OmBucketInfo bucketInfo = resolveBucketLink(ozoneManager.getMetadataManager(), volume, bucket, bucketSet);
        bucketSet.add(Pair.of(bucketInfo.getVolumeName(), bucketInfo.getBucketName()));
        for (Pair<String, String> pair : bucketSet) {
          OzoneAclUtils.checkAllAcls((OmMetadataReader) rcMetadataReader.get(), OzoneObj.ResourceType.BUCKET,
              OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.READ, pair.getKey(), pair.getValue(), null,
              ozoneManager.getVolumeOwner(pair.getKey(), aclType, OzoneObj.ResourceType.BUCKET),
              ozoneManager.getBucketOwner(pair.getKey(), pair.getValue(), aclType, OzoneObj.ResourceType.BUCKET),
              createUGIForApi(omRequest), getRemoteAddress(omRequest), getHostName(omRequest));
        }
      }

      // 2. validate key
      checkAcls(ozoneManager, resourceType, OzoneObj.StoreType.OZONE, aclType, volume, bucket, key, omRequest);
    }
  }
  @SuppressWarnings("parameternumber")
  public static void checkOpenKeyAcls(
      OzoneManager ozoneManager, String volume, String bucket, String key, IAccessAuthorizer.ACLType aclType,
      OzoneObj.ResourceType resourceType, long clientId, OMRequest omRequest) throws IOException {
    if (ozoneManager.getAclsEnabled()) {
      // Native authorizer requires client id as part of key name to check
      // write ACL on key. Add client id to key name if ozone native
      // authorizer is configured.
      if (ozoneManager.getAccessAuthorizer().isNative()) {
        key = key + "/" + clientId;
      }
      checkAcls(ozoneManager, resourceType, OzoneObj.StoreType.OZONE, aclType,
          volume, bucket, key, omRequest);
    }
  }
  @SuppressWarnings("parameternumber")
  public static void checkAcls(
      OzoneManager ozoneManager, OzoneObj.ResourceType resType, OzoneObj.StoreType storeType,
      IAccessAuthorizer.ACLType aclType, String vol, String bucket, String key,
      OMRequest omRequest) throws IOException {
    checkAcls(ozoneManager, resType, storeType, aclType, vol, bucket, key,
        ozoneManager.getVolumeOwner(vol, aclType, resType),
        ozoneManager.getBucketOwner(vol, bucket, aclType, resType), omRequest);
  }
  @SuppressWarnings("parameternumber")
  public static void checkAcls(
      OzoneManager ozoneManager, OzoneObj.ResourceType resType, OzoneObj.StoreType storeType,
      IAccessAuthorizer.ACLType aclType, String vol, String bucket, String key, String volOwner, String bucketOwner,
      OMRequest omRequest) throws IOException {
    try (ReferenceCounted<IOmMetadataReader> rcMetadataReader = ozoneManager.getOmMetadataReader()) {
      OzoneAclUtils.checkAllAcls((OmMetadataReader) rcMetadataReader.get(), resType, storeType, aclType, vol, bucket,
          key, volOwner, bucketOwner, createUGIForApi(omRequest), getRemoteAddress(omRequest), getHostName(omRequest));
    }
  }

  /**
   * For non-rpc internal calls Server.getRemoteUser()
   * and Server.getRemoteIp() will be null.
   * Passing getCurrentUser() and Ip of the Om node that started it.
   * @return User Info.
   */
  public static OzoneManagerProtocolProtos.UserInfo getUserIfNotExists(
      OzoneManager ozoneManager, OMRequest omRequest) throws IOException {
    OzoneManagerProtocolProtos.UserInfo userInfo = getUserInfo(omRequest);
    if (!userInfo.hasRemoteAddress() || !userInfo.hasUserName()) {
      OzoneManagerProtocolProtos.UserInfo.Builder newuserInfo =
          OzoneManagerProtocolProtos.UserInfo.newBuilder();
      UserGroupInformation user;
      InetAddress remoteAddress;
      try {
        user = UserGroupInformation.getCurrentUser();
        remoteAddress = ozoneManager.getOmRpcServerAddr()
            .getAddress();
      } catch (Exception e) {
        LOG.debug("Couldn't get om Rpc server address", e);
        return getUserInfo(omRequest);
      }
      newuserInfo.setUserName(user.getUserName());
      newuserInfo.setHostName(remoteAddress.getHostName());
      newuserInfo.setRemoteAddress(remoteAddress.getHostAddress());
      return newuserInfo.build();
    }
    return getUserInfo(omRequest);
  }

  /**
   * Get User information which needs to be set in the OMRequest object.
   * @return User Info.
   */
  public static OzoneManagerProtocolProtos.UserInfo getUserInfo(
      OzoneManagerProtocolProtos.OMRequest omRequest) throws IOException {
    UserGroupInformation user = ProtobufRpcEngine.Server.getRemoteUser();
    InetAddress remoteAddress = ProtobufRpcEngine.Server.getRemoteIp();
    OzoneManagerProtocolProtos.UserInfo.Builder userInfo =
        OzoneManagerProtocolProtos.UserInfo.newBuilder();

    // If S3 Authentication is set, determine user based on access ID.
    if (omRequest.hasS3Authentication()) {
      String principal = OzoneAclUtils.accessIdToUserPrincipal(
          omRequest.getS3Authentication().getAccessId());
      userInfo.setUserName(principal);
    } else if (user != null) {
      // Added not null checks, as in UT's these values might be null.
      userInfo.setUserName(user.getUserName());
    }

    // for gRPC s3g omRequests that contain user name
    if (user == null && omRequest.hasUserInfo()) {
      userInfo.setUserName(omRequest.getUserInfo().getUserName());
    }

    String grpcContextClientIpAddress =
        GrpcClientConstants.CLIENT_IP_ADDRESS_CTX_KEY.get();
    String grpcContextClientHostname =
        GrpcClientConstants.CLIENT_HOSTNAME_CTX_KEY.get();
    if (remoteAddress != null) {
      userInfo.setHostName(remoteAddress.getHostName());
      userInfo.setRemoteAddress(remoteAddress.getHostAddress()).build();
    } else if (grpcContextClientHostname != null
        && grpcContextClientIpAddress != null) {
      userInfo.setHostName(grpcContextClientHostname);
      userInfo.setRemoteAddress(grpcContextClientIpAddress);
    }

    return userInfo.build();
  }
  /**
   * Return InetAddress created from OMRequest userInfo. If userInfo is not
   * set, returns null.
   * @return InetAddress
   * @throws IOException
   */
  public static InetAddress getRemoteAddress(OzoneManagerProtocolProtos.OMRequest omRequest) throws IOException {
    if (omRequest.hasUserInfo()) {
      return InetAddress.getByName(omRequest.getUserInfo()
          .getRemoteAddress());
    } else {
      return null;
    }
  }
  /**
   * Return String created from OMRequest userInfo. If userInfo is not
   * set, returns null.
   * @return String
   */
  public static String getHostName(OzoneManagerProtocolProtos.OMRequest omRequest) {
    if (omRequest.hasUserInfo()) {
      return omRequest.getUserInfo().getHostName();
    } else {
      return null;
    }
  }
  /**
   * Return UGI object created from OMRequest userInfo. If userInfo is not
   * set, returns null.
   * @return UserGroupInformation.
   */
  public static UserGroupInformation createUGI(OzoneManagerProtocolProtos.OMRequest omRequest)
      throws AuthenticationException {
    if (omRequest.hasUserInfo() &&
        !StringUtils.isBlank(omRequest.getUserInfo().getUserName())) {
      return UserGroupInformation.createRemoteUser(
          omRequest.getUserInfo().getUserName());
    } else {
      throw new AuthenticationException("User info is not set."
          + " Please check client auth credentials");
    }
  }

  /**
   * Crete a UGI from request and wrap the AuthenticationException
   * to OMException in case of empty credentials.
   * @return UserGroupInformation
   * @throws OMException exception about an empty user credential
   *                      (unauthorized request)
   */
  public static UserGroupInformation createUGIForApi(OzoneManagerProtocolProtos.OMRequest omRequest)
      throws OMException {
    UserGroupInformation ugi;
    try {
      ugi = createUGI(omRequest);
    } catch (AuthenticationException e) {
      throw new OMException(e, UNAUTHORIZED);
    }
    return ugi;
  }
  /* Optimize ugi lookup for RPC operations to avoid a trip through
   * UGI.getCurrentUser which is synch'ed.
   */
  private static UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = Server.getRemoteUser();
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
  }

  public static Optional<FileEncryptionInfo> getFileEncryptionInfo(
      OzoneManager ozoneManager, OmBucketInfo bucketInfo) throws IOException {
    Optional<FileEncryptionInfo> encInfo = Optional.empty();
    if (ozoneManager.getKmsProvider() == null) {
      return encInfo;
    }
    BucketEncryptionKeyInfo ezInfo = bucketInfo.getEncryptionKeyInfo();
    if (ezInfo != null) {
      final String ezKeyName = ezInfo.getKeyName();
      KeyProviderCryptoExtension.EncryptedKeyVersion edek = generateEDEK(ozoneManager, ezKeyName);
      encInfo = Optional.of(new FileEncryptionInfo(ezInfo.getSuite(),
          ezInfo.getVersion(),
          edek.getEncryptedKeyVersion().getMaterial(),
          edek.getEncryptedKeyIv(), ezKeyName,
          edek.getEncryptionKeyVersionName()));
    }
    return encInfo;
  }

  private static KeyProviderCryptoExtension.EncryptedKeyVersion generateEDEK(
      OzoneManager ozoneManager, String ezKeyName) throws IOException {
    if (ezKeyName == null) {
      return null;
    }
    long generateEDEKStartTime = monotonicNow();
    KeyProviderCryptoExtension.EncryptedKeyVersion edek = SecurityUtil.doAsLoginUser(
        new PrivilegedExceptionAction<KeyProviderCryptoExtension.EncryptedKeyVersion>() {
          @Override
          public KeyProviderCryptoExtension.EncryptedKeyVersion run() throws IOException {
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

  public static FileEncryptionInfo getFileEncryptionInfoForMpuKey(
      OzoneManagerProtocolProtos.KeyArgs keyArgs, OzoneManager ozoneManager, BucketLayout layout) throws IOException {
    if (ozoneManager.getKmsProvider() != null) {
      OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
      String dbMultipartOpenKey = omMetadataManager.getMultipartKey(keyArgs.getVolumeName(),
          keyArgs.getBucketName(), keyArgs.getKeyName(), keyArgs.getMultipartUploadID());
      OmKeyInfo omKeyInfo = omMetadataManager.getOpenKeyTable(layout).get(dbMultipartOpenKey);
      if (omKeyInfo != null) {
        return omKeyInfo.getFileEncryptionInfo();
      } else {
        LOG.warn("omKeyInfo not found. Key: " + dbMultipartOpenKey +
            ". The upload id " + keyArgs.getMultipartUploadID() + " may be invalid.");
      }
    }
    return null;
  }
  /**
   * Check bucket quota in bytes.
   * @param omBucketInfo
   * @param allocateSize
   * @throws IOException
   */
  public static void checkBucketQuotaInBytes(OmBucketInfo omBucketInfo, long allocateSize) throws IOException {
    if (omBucketInfo.getQuotaInBytes() > OzoneConsts.QUOTA_RESET) {
      long curUsedBytes = omBucketInfo.getUsedBytes() + allocateSize;
      long quotaInBytes = omBucketInfo.getQuotaInBytes();
      if (quotaInBytes < curUsedBytes) {
        throw new OMException("The DiskSpace quota of bucket:" + omBucketInfo.getBucketName()
            + " exceeded quotaInBytes: " + quotaInBytes + " Bytes but diskspace consumed: "
            + curUsedBytes + " Bytes.", OMException.ResultCodes.QUOTA_EXCEEDED);
      }
    }
  }
  /**
   * Check and update bucket quota in bytes.
   * @param omBucketInfo
   * @param allocateSize
   * @param allocateNamespace
   * @throws IOException
   */
  public static void checkUpdateBucketQuota(OmBucketInfo omBucketInfo, long allocateSize, long allocateNamespace)
      throws IOException {
    QuotaResource bucketQuota = QuotaResource.Factory.getQuotaResource(omBucketInfo.getObjectID());
    assert bucketQuota != null;
    long reservedSize = 0;
    if (omBucketInfo.getQuotaInBytes() > OzoneConsts.QUOTA_RESET) {
      bucketQuota.addUsedBytes(allocateSize);
      long curUsedBytes = omBucketInfo.getUsedBytes();
      if (omBucketInfo.getQuotaInBytes() < curUsedBytes) {
        bucketQuota.addUsedBytes(-allocateSize);
        throw new OMException("The DiskSpace quota of bucket:" + omBucketInfo.getBucketName()
            + " exceeded quotaInBytes: " + omBucketInfo.getQuotaInBytes() + " Bytes but diskspace consumed: "
            + curUsedBytes + " Bytes.", OMException.ResultCodes.QUOTA_EXCEEDED);
      }
      reservedSize = allocateSize;
    }
    if (omBucketInfo.getQuotaInNamespace() > OzoneConsts.QUOTA_RESET) {
      bucketQuota.addUsedNamespace(allocateNamespace);
      long curUsedNamespace = omBucketInfo.getUsedNamespace();
      if (omBucketInfo.getQuotaInNamespace() < curUsedNamespace) {
        bucketQuota.addUsedBytes(-reservedSize);
        bucketQuota.addUsedNamespace(-allocateNamespace);
        throw new OMException("The namespace quota of Bucket:" + omBucketInfo.getBucketName()
            + " exceeded: quotaInNamespace: " + omBucketInfo.getQuotaInNamespace() + " but namespace consumed: "
            + (curUsedNamespace + allocateNamespace) + ".", OMException.ResultCodes.QUOTA_EXCEEDED);
      }
    }
  }
  /**
   * Wrap the uncommitted blocks as pseudoKeyInfo.
   *
   * @param uncommitted Uncommitted OmKeyLocationInfo
   * @param omKeyInfo   Args for key block
   * @return pseudoKeyInfo
   */
  public static OmKeyInfo wrapUncommittedBlocksAsPseudoKey(List<OmKeyLocationInfo> uncommitted, OmKeyInfo omKeyInfo) {
    if (uncommitted.isEmpty()) {
      return null;
    }
    LOG.debug("Detect allocated but uncommitted blocks {} in key {}.", uncommitted, omKeyInfo.getKeyName());
    OmKeyInfo pseudoKeyInfo = omKeyInfo.copyObject();
    // This is a special marker to indicate that SnapshotDeletingService
    // can reclaim this key's blocks unconditionally.
    pseudoKeyInfo.setObjectID(OBJECT_ID_RECLAIM_BLOCKS);
    // TODO dataSize of pseudoKey is not real here
    List<OmKeyLocationInfoGroup> uncommittedGroups = new ArrayList<>();
    // version not matters in the current logic of keyDeletingService,
    // all versions of blocks will be deleted.
    uncommittedGroups.add(new OmKeyLocationInfoGroup(0, uncommitted));
    pseudoKeyInfo.setKeyLocationVersions(uncommittedGroups);
    return pseudoKeyInfo;
  }
  /**
   * Prepare OmKeyInfo which will be persisted to openKeyTable.
   * @return OmKeyInfo
   * @throws IOException
   */
  @SuppressWarnings("parameternumber")
  public static OmKeyInfo prepareKeyInfo(
      @Nonnull OMMetadataManager omMetadataManager,
      @Nonnull OzoneManagerProtocolProtos.KeyArgs keyArgs, OmKeyInfo dbKeyInfo, long size,
      @Nonnull List<OmKeyLocationInfo> locations,
      @Nullable FileEncryptionInfo encInfo,
      @Nonnull PrefixManager prefixManager,
      @Nullable OmBucketInfo omBucketInfo,
      OMFileRequest.OMPathInfo omPathInfo,
      long transactionLogIndex, long objectID, boolean isRatisEnabled,
      ReplicationConfig replicationConfig)
      throws IOException {

    return prepareFileInfo(omMetadataManager, keyArgs, dbKeyInfo, size,
        locations, encInfo, prefixManager, omBucketInfo, omPathInfo,
        transactionLogIndex, objectID, isRatisEnabled, replicationConfig);
  }

  /**
   * Prepare OmKeyInfo which will be persisted to openKeyTable.
   * @return OmKeyInfo
   * @throws IOException
   */
  @SuppressWarnings("parameternumber")
  private static OmKeyInfo prepareFileInfo(
      @Nonnull OMMetadataManager omMetadataManager,
      @Nonnull OzoneManagerProtocolProtos.KeyArgs keyArgs, OmKeyInfo dbKeyInfo, long size,
      @Nonnull List<OmKeyLocationInfo> locations,
      @Nullable FileEncryptionInfo encInfo,
      @Nonnull PrefixManager prefixManager,
      @Nullable OmBucketInfo omBucketInfo,
      OMFileRequest.OMPathInfo omPathInfo,
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

      // Construct a new metadata map from KeyArgs.
      dbKeyInfo.getMetadata().clear();
      dbKeyInfo.getMetadata().putAll(KeyValueUtil.getFromProtobuf(
          keyArgs.getMetadataList()));

      // Construct a new tags from KeyArgs
      // Clear the old one when the key is overwritten
      dbKeyInfo.getTags().clear();
      dbKeyInfo.getTags().putAll(KeyValueUtil.getFromProtobuf(
          keyArgs.getTagsList()));

      if (keyArgs.hasExpectedDataGeneration()) {
        dbKeyInfo.setExpectedDataGeneration(keyArgs.getExpectedDataGeneration());
      }

      dbKeyInfo.setFileEncryptionInfo(encInfo);
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
  public static OmKeyInfo createFileInfo(
      @Nonnull OzoneManagerProtocolProtos.KeyArgs keyArgs,
      @Nonnull List<OmKeyLocationInfo> locations,
      @Nonnull ReplicationConfig replicationConfig,
      long size,
      @Nullable FileEncryptionInfo encInfo,
      @Nonnull PrefixManager prefixManager,
      @Nullable OmBucketInfo omBucketInfo,
      OMFileRequest.OMPathInfo omPathInfo,
      long transactionLogIndex, long objectID) {
    OmKeyInfo.Builder builder = new OmKeyInfo.Builder();
    builder.setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setOmKeyLocationInfos(Collections.singletonList(new OmKeyLocationInfoGroup(0, locations)))
        .setCreationTime(keyArgs.getModificationTime())
        .setModificationTime(keyArgs.getModificationTime())
        .setDataSize(size)
        .setReplicationConfig(replicationConfig)
        .setFileEncryptionInfo(encInfo)
        .setAcls(getAclsForKey(keyArgs, omBucketInfo, omPathInfo, prefixManager))
        .addAllMetadata(KeyValueUtil.getFromProtobuf(keyArgs.getMetadataList()))
        .addAllTags(KeyValueUtil.getFromProtobuf(keyArgs.getTagsList()))
        .setUpdateID(transactionLogIndex)
        .setOwnerName(keyArgs.getOwnerName())
        .setFile(true);
    if (omPathInfo instanceof OMFileRequest.OMPathInfoWithFSO) {
      // FileTable metadata format
      OMFileRequest.OMPathInfoWithFSO omPathInfoFSO = (OMFileRequest.OMPathInfoWithFSO) omPathInfo;
      objectID = omPathInfoFSO.getLeafNodeObjectId();
      builder.setParentObjectID(omPathInfoFSO.getLastKnownParentId());
      builder.setFileName(omPathInfoFSO.getLeafNodeName());
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
  private static OmKeyInfo prepareMultipartFileInfo(
      @Nonnull OMMetadataManager omMetadataManager,
      @Nonnull OzoneManagerProtocolProtos.KeyArgs args, long size,
      @Nonnull List<OmKeyLocationInfo> locations,
      FileEncryptionInfo encInfo, @Nonnull PrefixManager prefixManager,
      @Nullable OmBucketInfo omBucketInfo,
      OMFileRequest.OMPathInfo omPathInfo,
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
    if (omPathInfo instanceof OMFileRequest.OMPathInfoWithFSO) {
      OMFileRequest.OMPathInfoWithFSO omPathInfoFSO
          = (OMFileRequest.OMPathInfoWithFSO) omPathInfo;
      final long volumeId = omMetadataManager.getVolumeId(
          args.getVolumeName());
      final long bucketId = omMetadataManager.getBucketId(
          args.getVolumeName(), args.getBucketName());
      // FileTable metadata format
      multipartKey = omMetadataManager.getMultipartKey(volumeId, bucketId,
          omPathInfoFSO.getLastKnownParentId(),
          omPathInfoFSO.getLeafNodeName(), uploadID);
    } else {
      multipartKey = omMetadataManager
          .getMultipartKey(args.getVolumeName(), args.getBucketName(),
              args.getKeyName(), uploadID);
    }
    OmKeyInfo partKeyInfo =
        omMetadataManager.getOpenKeyTable(omBucketInfo.getBucketLayout()).get(multipartKey);
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

  private static List<OzoneAcl> getAclsForKey(OzoneManagerProtocolProtos.KeyArgs keyArgs,
                                         OmBucketInfo bucketInfo, OMFileRequest.OMPathInfo omPathInfo,
                                         PrefixManager prefixManager) {

    List<OzoneAcl> acls = new ArrayList<>();
    if (keyArgs.getAclsList() != null) {
      acls.addAll(OzoneAclUtil.fromProtobuf(keyArgs.getAclsList()));
    }

    // Inherit DEFAULT acls from prefix.
    if (prefixManager != null) {
      List<OmPrefixInfo> prefixList = prefixManager.getLongestPrefixPath(
          OZONE_URI_DELIMITER +
              keyArgs.getVolumeName() + OZONE_URI_DELIMITER +
              keyArgs.getBucketName() + OZONE_URI_DELIMITER +
              keyArgs.getKeyName());

      if (prefixList.size() > 0) {
        // Add all acls from direct parent to key.
        OmPrefixInfo prefixInfo = prefixList.get(prefixList.size() - 1);
        if (prefixInfo  != null) {
          if (OzoneAclUtil.inheritDefaultAcls(acls, prefixInfo.getAcls(), ACCESS)) {
            return acls;
          }
        }
      }
    }

    // Inherit DEFAULT acls from parent-dir only if DEFAULT acls for
    // prefix are not set
    if (omPathInfo != null) {
      if (OzoneAclUtil.inheritDefaultAcls(acls, omPathInfo.getAcls(), ACCESS)) {
        return acls;
      }
    }

    // Inherit DEFAULT acls from bucket only if DEFAULT acls for
    // parent-dir are not set.
    if (bucketInfo != null) {
      if (OzoneAclUtil.inheritDefaultAcls(acls, bucketInfo.getAcls(), ACCESS)) {
        return acls;
      }
    }

    return acls;
  }

  /**
   * This methods avoids multiple rpc calls to SCM by allocating multiple blocks
   * in one rpc call.
   * @throws IOException
   */
  @SuppressWarnings("parameternumber")
  public static List< OmKeyLocationInfo > allocateBlock(
      ScmClient scmClient, OzoneBlockTokenSecretManager secretManager, ReplicationConfig replicationConfig,
      ExcludeList excludeList, long requestedSize, long scmBlockSize, int preallocateBlocksMax,
      boolean grpcBlockTokenEnabled, String serviceID, OMMetrics omMetrics, boolean shouldSortDatanodes,
      OzoneManagerProtocolProtos.UserInfo userInfo)
      throws IOException {
    int dataGroupSize = replicationConfig instanceof ECReplicationConfig
        ? ((ECReplicationConfig) replicationConfig).getData() : 1;
    int numBlocks = (int) Math.min(preallocateBlocksMax,
        (requestedSize - 1) / (scmBlockSize * dataGroupSize) + 1);

    String clientMachine = "";
    if (shouldSortDatanodes) {
      clientMachine = userInfo.getRemoteAddress();
    }

    List<OmKeyLocationInfo> locationInfos = new ArrayList<>(numBlocks);
    String remoteUser = getRemoteUser().getShortUserName();
    List<AllocatedBlock> allocatedBlocks;
    try {
      allocatedBlocks = scmClient.getBlockClient()
          .allocateBlock(scmBlockSize, numBlocks, replicationConfig, serviceID,
              excludeList, clientMachine);
    } catch (SCMException ex) {
      omMetrics.incNumBlockAllocateCallFails();
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

  /**
   * Set parameters needed for return error response to client.
   * @param omResponse
   * @param ex - IOException
   * @return error response need to be returned to client - OMResponse.
   */
  public static OzoneManagerProtocolProtos.OMResponse createErrorOMResponse(
      @Nonnull OzoneManagerProtocolProtos.OMResponse.Builder omResponse, @Nonnull Exception ex) {

    omResponse.setSuccess(false);
    String errorMsg = exceptionErrorMessage(ex);
    if (errorMsg != null) {
      omResponse.setMessage(errorMsg);
    }
    omResponse.setStatus(OzoneManagerRatisUtils.exceptionToResponseStatus(ex));
    return omResponse.build();
  }
  private static String exceptionErrorMessage(Exception ex) {
    if (ex instanceof OMException || ex instanceof InvalidPathException) {
      return ex.getMessage();
    } else {
      return org.apache.hadoop.util.StringUtils.stringifyException(ex);
    }
  }
}
