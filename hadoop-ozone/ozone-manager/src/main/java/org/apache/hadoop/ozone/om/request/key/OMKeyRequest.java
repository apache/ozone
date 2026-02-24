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

package org.apache.hadoop.ozone.om.request.key;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.READ;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.WRITE;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OBJECT_ID_RECLAIM_BLOCKS;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_KEY_NAME;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.om.helpers.OzoneAclUtil.getDefaultAclList;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.util.Time.monotonicNow;

import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.ScmClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.lock.OzoneLockStrategy;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.OMClientRequestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface for key write requests.
 */
public abstract class OMKeyRequest extends OMClientRequest {

  // The maximum number of directories which can be created through a single
  // transaction (recursive directory creations) is 2^8 - 1 as only 8
  // bits are set aside for this in ObjectID.
  private static final long MAX_NUM_OF_RECURSIVE_DIRS = 255;

  protected static final Logger LOG = LoggerFactory.getLogger(OMKeyRequest.class);

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

  protected KeyArgs resolveBucketLink(
      OzoneManager ozoneManager, KeyArgs keyArgs) throws IOException {
    ResolvedBucket bucket = ozoneManager.resolveBucketLink(keyArgs, this);
    keyArgs = bucket.update(keyArgs);
    return keyArgs;
  }

  protected KeyArgs resolveBucketAndCheckKeyAcls(KeyArgs keyArgs,
      OzoneManager ozoneManager, IAccessAuthorizer.ACLType aclType)
      throws IOException {
    KeyArgs resolvedArgs = resolveBucketLink(ozoneManager, keyArgs);
    // check Acl
    checkKeyAcls(ozoneManager, resolvedArgs.getVolumeName(),
        resolvedArgs.getBucketName(), keyArgs.getKeyName(),
        aclType, OzoneObj.ResourceType.KEY);
    return resolvedArgs;
  }

  protected KeyArgs resolveBucketAndCheckKeyAclsWithFSO(KeyArgs keyArgs,
      OzoneManager ozoneManager, IAccessAuthorizer.ACLType aclType)
      throws IOException {
    KeyArgs resolvedArgs = resolveBucketLink(ozoneManager, keyArgs);
    // check Acl
    checkACLsWithFSO(ozoneManager, resolvedArgs.getVolumeName(),
        resolvedArgs.getBucketName(), keyArgs.getKeyName(), aclType);
    return resolvedArgs;
  }

  protected KeyArgs resolveBucketAndCheckOpenKeyAcls(KeyArgs keyArgs,
      OzoneManager ozoneManager, IAccessAuthorizer.ACLType aclType,
      long clientId)
      throws IOException {
    KeyArgs resolvedArgs = resolveBucketLink(ozoneManager, keyArgs);
    // check Acl
    checkKeyAclsInOpenKeyTable(ozoneManager, resolvedArgs.getVolumeName(),
        resolvedArgs.getBucketName(), keyArgs.getKeyName(),
        aclType, clientId);
    return resolvedArgs;
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
      boolean grpcBlockTokenEnabled, String serviceID, OMMetrics omMetrics,
      boolean shouldSortDatanodes, UserInfo userInfo)
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

  /* Optimize ugi lookup for RPC operations to avoid a trip through
   * UGI.getCurrentUser which is synch'ed.
   */
  protected UserGroupInformation getRemoteUser() throws IOException {
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
    Optional<FileEncryptionInfo> encInfo = Optional.empty();
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
    Objects.requireNonNull(edek, "edek == null");
    return edek;
  }

  protected List<OzoneAcl> getAclsForKey(KeyArgs keyArgs,
      OmBucketInfo bucketInfo, OMFileRequest.OMPathInfo omPathInfo,
      PrefixManager prefixManager, OmConfig config) throws OMException {

    List<OzoneAcl> acls = new ArrayList<>();
    acls.addAll(getDefaultAclList(createUGIForApi(), config));
    if (!keyArgs.getAclsList().isEmpty() && !config.ignoreClientACLs()) {
      acls.addAll(OzoneAclUtil.fromProtobuf(keyArgs.getAclsList()));
    }

    // Inherit DEFAULT acls from prefix.
    if (prefixManager != null) {
      List< OmPrefixInfo > prefixList = prefixManager.getLongestPrefixPath(
          OZONE_URI_DELIMITER +
              keyArgs.getVolumeName() + OZONE_URI_DELIMITER +
              keyArgs.getBucketName() + OZONE_URI_DELIMITER +
              keyArgs.getKeyName());

      if (!prefixList.isEmpty()) {
        // Add all acls from direct parent to key.
        OmPrefixInfo prefixInfo = prefixList.get(prefixList.size() - 1);
        if (prefixInfo  != null) {
          if (OzoneAclUtil.inheritDefaultAcls(acls, prefixInfo.getAcls(), ACCESS)) {
            // Remove the duplicates
            acls = acls.stream().distinct().collect(Collectors.toList());
            return acls;
          }
        }
      }
    }

    // Inherit DEFAULT acls from parent-dir only if DEFAULT acls for
    // prefix are not set
    if (omPathInfo != null) {
      if (OzoneAclUtil.inheritDefaultAcls(acls, omPathInfo.getAcls(), ACCESS)) {
        acls = acls.stream().distinct().collect(Collectors.toList());
        return acls;
      }
    }

    // Inherit DEFAULT acls from bucket only if DEFAULT acls for
    // parent-dir are not set.
    if (bucketInfo != null) {
      if (OzoneAclUtil.inheritDefaultAcls(acls, bucketInfo.getAcls(), ACCESS)) {
        acls = acls.stream().distinct().collect(Collectors.toList());
        return acls;
      }
    }

    acls = acls.stream().distinct().collect(Collectors.toList());
    return acls;
  }

  /**
   * Inherit parent DEFAULT acls and generate its own ACCESS acls.
   * @param keyArgs
   * @param bucketInfo
   * @param omPathInfo
   * @param config
   * @return Acls which inherited parent DEFAULT and keyArgs ACCESS acls.
   */
  protected List<OzoneAcl> getAclsForDir(KeyArgs keyArgs, OmBucketInfo bucketInfo,
      OMFileRequest.OMPathInfo omPathInfo, OmConfig config) throws OMException {
    // Acls inherited from parent or bucket will convert to DEFAULT scope
    List<OzoneAcl> acls = new ArrayList<>();
    // add default ACLs
    acls.addAll(getDefaultAclList(createUGIForApi(), config));

    // Inherit DEFAULT acls from parent-dir
    if (omPathInfo != null) {
      OzoneAclUtil.inheritDefaultAcls(acls, omPathInfo.getAcls(), DEFAULT);
    }

    // Inherit DEFAULT acls from bucket only if DEFAULT acls for
    // parent-dir are not set.
    if (acls.isEmpty() && bucketInfo != null) {
      OzoneAclUtil.inheritDefaultAcls(acls, bucketInfo.getAcls(), DEFAULT);
    }

    // add acls from clients
    if (!keyArgs.getAclsList().isEmpty() && !config.ignoreClientACLs()) {
      acls.addAll(OzoneAclUtil.fromProtobuf(keyArgs.getAclsList()));
    }
    acls = acls.stream().distinct().collect(Collectors.toList());
    return acls;
  }

  /**
   * Construct OmDirectoryInfo for every parent directory in missing list.
   *
   * @param keyArgs      key arguments
   * @param pathInfo     list of parent directories to be created and its ACLs
   * @param trxnLogIndex transaction log index id
   * @return list of missing parent directories
   * @throws IOException DB failure
   */
  protected List<OmDirectoryInfo> getAllMissingParentDirInfo(
      OzoneManager ozoneManager, KeyArgs keyArgs, OmBucketInfo bucketInfo,
      OMFileRequest.OMPathInfoWithFSO pathInfo, long trxnLogIndex)
      throws IOException {
    List<OmDirectoryInfo> missingParentInfos = new ArrayList<>();

    // The base id is left shifted by 8 bits for creating space to
    // create (2^8 - 1) object ids in every request.
    // maxObjId represents the largest object id allocation possible inside
    // the transaction.
    long baseObjId = ozoneManager.getObjectIdFromTxId(trxnLogIndex);
    long maxObjId = baseObjId + MAX_NUM_OF_RECURSIVE_DIRS;
    long objectCount = 1;

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    long lastKnownParentId = pathInfo.getLastKnownParentId();
    List<String> missingParents = pathInfo.getMissingParents();
    for (String missingKey : missingParents) {
      long nextObjId = baseObjId + objectCount;
      if (nextObjId > maxObjId) {
        throw new OMException("Too many directories in path. Exceeds limit of "
            + MAX_NUM_OF_RECURSIVE_DIRS + ". Unable to create directory: "
            + keyName + " in volume/bucket: " + volumeName + "/" + bucketName,
            INVALID_KEY_NAME);
      }

      LOG.debug("missing parent {} getting added to DirectoryTable",
          missingKey);
      OmDirectoryInfo dirInfo = createDirectoryInfoWithACL(missingKey,
          keyArgs, nextObjId, lastKnownParentId, trxnLogIndex,
          bucketInfo, pathInfo, ozoneManager.getConfig());
      objectCount++;

      missingParentInfos.add(dirInfo);

      // updating id for the next sub-dir
      lastKnownParentId = nextObjId;
    }
    pathInfo.setLastKnownParentId(lastKnownParentId);
    pathInfo.setLeafNodeObjectId(baseObjId + objectCount);
    return missingParentInfos;
  }

  /**
   * Construct OmKeyInfo for every parent directory in missing list.
   * @param ozoneManager
   * @param keyArgs
   * @param missingParents list of parent directories to be created
   * @param bucketInfo
   * @param omPathInfo
   * @param trxnLogIndex
   * @return {@code List<OmKeyInfo>}
   * @throws IOException
   */
  protected List<OmKeyInfo> getAllParentInfo(OzoneManager ozoneManager,
      KeyArgs keyArgs, List<String> missingParents, OmBucketInfo bucketInfo,
      OMFileRequest.OMPathInfo omPathInfo, long trxnLogIndex)
      throws IOException {
    List<OmKeyInfo> missingParentInfos = new ArrayList<>();

    // The base id is left shifted by 8 bits for creating space to
    // create (2^8 - 1) object ids in every request.
    // maxObjId represents the largest object id allocation possible inside
    // the transaction.
    long baseObjId = ozoneManager.getObjectIdFromTxId(trxnLogIndex);
    long maxObjId = baseObjId + MAX_NUM_OF_RECURSIVE_DIRS;
    long objectCount = 1; // baseObjID is used by the leaf directory

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    for (String missingKey : missingParents) {
      long nextObjId = baseObjId + objectCount;
      if (nextObjId > maxObjId) {
        throw new OMException("Too many directories in path. Exceeds limit of "
            + MAX_NUM_OF_RECURSIVE_DIRS + ". Unable to create directory: "
            + keyName + " in volume/bucket: " + volumeName + "/" + bucketName,
            INVALID_KEY_NAME);
      }

      LOG.debug("missing parent {} getting added to KeyTable", missingKey);

      OmKeyInfo parentKeyInfo =
          createDirectoryKeyInfoWithACL(missingKey, keyArgs, nextObjId,
              bucketInfo, omPathInfo, trxnLogIndex,
              ozoneManager.getDefaultReplicationConfig(), ozoneManager.getConfig());
      objectCount++;

      missingParentInfos.add(parentKeyInfo);
    }

    return missingParentInfos;
  }

  /**
   * Fill in a DirectoryInfo for a new directory entry in OM database.
   * @param dirName
   * @param keyArgs
   * @param objectId
   * @param parentObjectId
   * @param bucketInfo
   * @param omPathInfo
   * @param config
   * @return the OmDirectoryInfo structure
   */
  @SuppressWarnings("parameternumber")
  protected OmDirectoryInfo createDirectoryInfoWithACL(
      String dirName, KeyArgs keyArgs, long objectId,
      long parentObjectId, long transactionIndex,
      OmBucketInfo bucketInfo, OMFileRequest.OMPathInfo omPathInfo,
      OmConfig config) throws OMException {
    return OmDirectoryInfo.newBuilder()
        .setName(dirName)
        .setOwner(keyArgs.getOwnerName())
        .setCreationTime(keyArgs.getModificationTime())
        .setModificationTime(keyArgs.getModificationTime())
        .setObjectID(objectId)
        .setUpdateID(transactionIndex)
        .setParentObjectID(parentObjectId).setAcls(getAclsForDir(keyArgs, bucketInfo, omPathInfo, config))
        .build();
  }

  /**
   * fill in a KeyInfo for a new directory entry in OM database.
   * without initializing ACLs from the KeyArgs - used for intermediate
   * directories which get created internally/recursively during file
   * and directory create.
   * @param keyName
   * @param keyArgs
   * @param objectId
   * @param bucketInfo
   * @param omPathInfo
   * @param transactionIndex
   * @param serverDefaultReplConfig
   * @param config
   * @return the OmKeyInfo structure
   */
  @SuppressWarnings("parameternumber")
  protected OmKeyInfo createDirectoryKeyInfoWithACL(String keyName,
      KeyArgs keyArgs, long objectId, OmBucketInfo bucketInfo,
      OMFileRequest.OMPathInfo omPathInfo, long transactionIndex,
      ReplicationConfig serverDefaultReplConfig, OmConfig config) throws OMException {
    return dirKeyInfoBuilderNoACL(keyName, keyArgs, objectId,
        serverDefaultReplConfig)
        .setAcls(getAclsForDir(keyArgs, bucketInfo, omPathInfo, config))
        .setUpdateID(transactionIndex).build();
  }

  protected OmKeyInfo.Builder dirKeyInfoBuilderNoACL(String keyName, KeyArgs keyArgs, long objectId,
      ReplicationConfig serverDefaultReplConfig) {
    String dirName = OzoneFSUtils.addTrailingSlashIfNeeded(keyName);

    OmKeyInfo.Builder keyInfoBuilder =
        new OmKeyInfo.Builder()
            .setVolumeName(keyArgs.getVolumeName())
            .setBucketName(keyArgs.getBucketName())
            .setKeyName(dirName)
            .setOwnerName(keyArgs.getOwnerName())
            .setOmKeyLocationInfos(Collections.singletonList(
                new OmKeyLocationInfoGroup(0, new ArrayList<>())))
            .setCreationTime(keyArgs.getModificationTime())
            .setModificationTime(keyArgs.getModificationTime())
            .setDataSize(0);
    if (keyArgs.getFactor() != null && keyArgs
        .getFactor() != HddsProtos.ReplicationFactor.ZERO && keyArgs
        .getType() != HddsProtos.ReplicationType.EC) {
      // Factor available and not an EC replication config.
      keyInfoBuilder.setReplicationConfig(ReplicationConfig
          .fromProtoTypeAndFactor(keyArgs.getType(), keyArgs.getFactor()));
    } else if (keyArgs.getType() == HddsProtos.ReplicationType.EC) {
      // Found EC type
      keyInfoBuilder.setReplicationConfig(
          new ECReplicationConfig(keyArgs.getEcReplicationConfig()));
    } else {
      // default type
      keyInfoBuilder.setReplicationConfig(serverDefaultReplConfig);
    }

    keyInfoBuilder.setObjectID(objectId);
    return keyInfoBuilder;
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
    if (ozoneManager.getAccessAuthorizer().isNative()) {
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
        mergeOmLockDetails(omMetadataManager.getLock().acquireReadLock(
            BUCKET_LOCK, volumeName, bucketName));
        acquireLock = getOmLockDetails().isLockAcquired();

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
          mergeOmLockDetails(omMetadataManager.getLock().releaseReadLock(
              BUCKET_LOCK, volumeName, bucketName));
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
      mergeOmLockDetails(omMetadataManager.getLock().acquireReadLock(
          BUCKET_LOCK, volumeName, bucketName));
      acquireLock = getOmLockDetails().isLockAcquired();
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

        if (omKeyInfo != null) {
          if (omKeyInfo.getFileEncryptionInfo() != null) {
            newKeyArgs.setFileEncryptionInfo(
                OMPBHelper.convert(omKeyInfo.getFileEncryptionInfo()));
          }
        } else {
          LOG.warn("omKeyInfo not found. Key: " + dbMultipartOpenKey +
              ". The upload id " + keyArgs.getMultipartUploadID() + " may be invalid.");
        }
      } finally {
        if (acquireLock) {
          mergeOmLockDetails(omMetadataManager.getLock()
              .releaseReadLock(BUCKET_LOCK, volumeName, bucketName));
        }
      }
    }
  }

  /**
   * Get FileEncryptionInfoProto from KeyArgs.
   * @param keyArgs
   * @return FileEncryptionInfo
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
   * @param metadataManager
   * @param omBucketInfo
   * @param allocateSize
   * @throws IOException
   */
  protected void checkBucketQuotaInBytes(
      OMMetadataManager metadataManager, OmBucketInfo omBucketInfo,
      long allocateSize) throws IOException {
    if (omBucketInfo.getQuotaInBytes() > OzoneConsts.QUOTA_RESET) {
      long usedBytes = omBucketInfo.getTotalBucketSpace();
      long quotaInBytes = omBucketInfo.getQuotaInBytes();
      if (quotaInBytes - usedBytes < allocateSize) {
        throw new OMException("The DiskSpace quota of bucket:"
            + omBucketInfo.getBucketName() + " exceeded quotaInBytes: "
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
      long usedNamespace = omBucketInfo.getTotalBucketNamespace();
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
   * @return the number of bytes (replicated size) used by blocks pointed to by {@code omKeyInfo}.
   */
  public static long sumBlockLengths(OmKeyInfo omKeyInfo) {
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
  public static OmBucketInfo getBucketInfo(OMMetadataManager omMetadataManager,
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
          OMFileRequest.OMPathInfo omPathInfo,
          long transactionLogIndex, long objectID,
          ReplicationConfig replicationConfig, OmConfig config)
          throws IOException {

    return prepareFileInfo(omMetadataManager, keyArgs, dbKeyInfo, size,
            locations, encInfo, prefixManager, omBucketInfo, omPathInfo,
            transactionLogIndex, objectID, replicationConfig, config);
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
          OMFileRequest.OMPathInfo omPathInfo,
          long transactionLogIndex, long objectID,
          ReplicationConfig replicationConfig,
          OmConfig config) throws IOException {
    if (keyArgs.getIsMultipartKey()) {
      return prepareMultipartFileInfo(omMetadataManager, keyArgs,
              size, locations, encInfo, prefixManager, omBucketInfo,
              omPathInfo, transactionLogIndex, objectID, config);
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
      // The modification time is set in preExecute. Use the same
      // modification time.
      // Construct a new metadata map from KeyArgs by rebuilding via toBuilder.
      // Construct a new tags from KeyArgs
      // Clear the old one when the key is overwritten
      final OmKeyInfo.Builder builder = dbKeyInfo.toBuilder()
          .setDataSize(newSize)
          .setModificationTime(keyArgs.getModificationTime())
          .setReplicationConfig(replicationConfig)
          .setMetadata(KeyValueUtil.getFromProtobuf(keyArgs.getMetadataList()))
          .setUpdateID(transactionLogIndex)
          .setTags(KeyValueUtil.getFromProtobuf(keyArgs.getTagsList()))
          .setFileEncryptionInfo(encInfo);

      if (keyArgs.hasExpectedDataGeneration()) {
        builder.setExpectedDataGeneration(keyArgs.getExpectedDataGeneration());
      }
      if (keyArgs.hasExpectedETag()) {
        builder.setExpectedETag(keyArgs.getExpectedETag());
      }

      return builder.build();
    }

    // the key does not exist, create a new object.
    // Blocks will be appended as version 0.
    return createFileInfo(keyArgs, locations, replicationConfig,
            keyArgs.getDataSize(), encInfo, prefixManager,
            omBucketInfo, omPathInfo, transactionLogIndex, objectID, config);
  }

  /**
   * Create OmKeyInfo object.
   * @return OmKeyInfo
   */
  @SuppressWarnings("parameterNumber")
  protected OmKeyInfo createFileInfo(
      @Nonnull KeyArgs keyArgs,
      @Nonnull List<OmKeyLocationInfo> locations,
      @Nonnull ReplicationConfig replicationConfig,
      long size,
      @Nullable FileEncryptionInfo encInfo,
      @Nonnull PrefixManager prefixManager,
      @Nullable OmBucketInfo omBucketInfo,
      OMFileRequest.OMPathInfo omPathInfo,
      long transactionLogIndex, long objectID,
      OmConfig config) throws OMException {
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
            .setAcls(getAclsForKey(
                keyArgs, omBucketInfo, omPathInfo, prefixManager, config))
            .addAllMetadata(KeyValueUtil.getFromProtobuf(
                    keyArgs.getMetadataList()))
            .addAllTags(KeyValueUtil.getFromProtobuf(
                    keyArgs.getTagsList()))
            .setUpdateID(transactionLogIndex)
            .setOwnerName(keyArgs.getOwnerName())
            .setFile(true);
    if (omPathInfo instanceof OMFileRequest.OMPathInfoWithFSO) {
      // FileTable metadata format
      OMFileRequest.OMPathInfoWithFSO omPathInfoFSO
          = (OMFileRequest.OMPathInfoWithFSO) omPathInfo;
      objectID = omPathInfoFSO.getLeafNodeObjectId();
      builder.setParentObjectID(omPathInfoFSO.getLastKnownParentId());
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
          OMFileRequest.OMPathInfo omPathInfo,
          @Nonnull long transactionLogIndex, long objectID,
          OmConfig configuration) throws IOException {

    Preconditions.checkArgument(args.getMultipartNumber() > 0,
            "PartNumber Should be greater than zero");
    // When key is multipart upload part key, we should take replication
    // type and replication factor from original key which has done
    // initiate multipart upload. If we have not found any such, we throw
    // error no such multipart upload.
    String uploadID = args.getMultipartUploadID();
    Objects.requireNonNull(uploadID, "uploadID == null");
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
            transactionLogIndex, objectID, configuration);
  }

  /**
   * Returns the DB key name of a multipart open key in OM metadata store.
   *
   * @param volumeName        - volume name.
   * @param bucketName        - bucket name.
   * @param keyName           - key name.
   * @param uploadID          - Multi part upload ID for this key.
   * @param omMetadataManager
   * @return {@code String}
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
   * @param keyToDelete OmKeyInfo of a key to be in deleteTable
   * @param bucketId
   * @param trxnLogIndex
   * @return Old keys eligible for deletion.
   * @throws IOException
   */
  protected RepeatedOmKeyInfo getOldVersionsToCleanUp(
      @Nonnull OmKeyInfo keyToDelete, long bucketId, long trxnLogIndex) throws IOException {
    return OmUtils.prepareKeyForDelete(bucketId, keyToDelete, trxnLogIndex);
  }

  protected OzoneLockStrategy getOzoneLockStrategy(OzoneManager ozoneManager) {
    return ozoneManager.getOzoneLockProvider()
        .createLockStrategy(getBucketLayout());
  }

  /**
   * Wrap the uncommitted blocks as pseudoKeyInfo.
   *
   * @param uncommitted Uncommitted OmKeyLocationInfo
   * @param omKeyInfo   Args for key block
   * @return pseudoKeyInfo
   */
  protected OmKeyInfo wrapUncommittedBlocksAsPseudoKey(
      List<OmKeyLocationInfo> uncommitted, OmKeyInfo omKeyInfo) {
    if (uncommitted.isEmpty() || omKeyInfo.getDataSize() == 0) {
      return null;
    }
    LOG.debug("Detect allocated but uncommitted blocks {} in key {}.",
        uncommitted, omKeyInfo.getKeyName());
    OmKeyInfo pseudoKeyInfo = omKeyInfo.toBuilder()
        .setObjectID(OBJECT_ID_RECLAIM_BLOCKS)
        .build();
    // This is a special marker to indicate that SnapshotDeletingService
    // can reclaim this key's blocks unconditionally.
    // TODO dataSize of pseudoKey is not real here
    List<OmKeyLocationInfoGroup> uncommittedGroups = new ArrayList<>();
    // version not matters in the current logic of keyDeletingService,
    // all versions of blocks will be deleted.
    uncommittedGroups.add(new OmKeyLocationInfoGroup(0, uncommitted));
    pseudoKeyInfo.setKeyLocationVersions(uncommittedGroups);
    return pseudoKeyInfo;
  }

  protected static Map<String, RepeatedOmKeyInfo> addKeyInfoToDeleteMap(OzoneManager om,
      long trxnLogIndex, String ozoneKey, long bucketId, OmKeyInfo keyInfo, Map<String, RepeatedOmKeyInfo> deleteMap) {
    if (keyInfo == null) {
      return deleteMap;
    }
    final long pseudoObjId = om.getObjectIdFromTxId(trxnLogIndex);
    final String delKeyName = om.getMetadataManager().getOzoneDeletePathKey(pseudoObjId, ozoneKey);
    if (deleteMap == null) {
      deleteMap = new HashMap<>();
    }
    deleteMap.computeIfAbsent(delKeyName, key -> new RepeatedOmKeyInfo(bucketId))
        .addOmKeyInfo(keyInfo);
    return deleteMap;
  }

  /**
   * Remove blocks in-place from keysToBeFiltered that exist in referenceKey.
   * <p>
   * keysToBeFiltered.getOmKeyInfoList() becomes an empty list when all blocks
   * are filtered out.
   *
   * @param referenceKey OmKeyInfo
   * @param keysToBeFiltered RepeatedOmKeyInfo
   */
  protected Pair<Map<OmKeyInfo, List<OmKeyLocationInfo>>, Integer> filterOutBlocksStillInUse(OmKeyInfo referenceKey,
      RepeatedOmKeyInfo keysToBeFiltered) {

    LOG.debug("Before block filtering, keysToBeFiltered = {}",
        keysToBeFiltered);

    // A HashSet for fast lookup. Gathers all ContainerBlockID entries inside
    // the referenceKey.
    Map<ContainerBlockID, OmKeyLocationInfo> cbIdSet = referenceKey.getKeyLocationVersions()
        .stream()
        .flatMap(e -> e.getLocationList().stream())
        .collect(Collectors.toMap(omKeyLocationInfo -> omKeyLocationInfo.getBlockID().getContainerBlockID(),
            Function.identity()));
    Map<OmKeyInfo, List<OmKeyLocationInfo>> filteredOutBlocks = new HashMap<>();

    // Pardon the nested loops. ContainerBlockID is 9-layer deep from:
    // keysToBeFiltered               // Layer 0. RepeatedOmKeyInfo
    //     .getOmKeyInfoList()        // 1. List<OmKeyInfo>
    //     .get(0)                    // 2. OmKeyInfo
    //     .getKeyLocationVersions()  // 3. List<OmKeyLocationInfoGroup>
    //     .get(0)                    // 4. OmKeyLocationInfoGroup
    //     .getLocationVersionMap()   // 5. Map<Long, List<OmKeyLocationInfo>>
    //     .get(version)              // 6. List<OmKeyLocationInfo>
    //     .get(0)                    // 7. OmKeyLocationInfo
    //     .getBlockID()              // 8. BlockID
    //     .getContainerBlockID();    // 9. ContainerBlockID

    // Using iterator instead of `for` or `forEach` for in-place entry removal

    // Layer 1: List<OmKeyInfo>
    Iterator<OmKeyInfo> iterOmKeyInfo = keysToBeFiltered
        .getOmKeyInfoList().iterator();
    int emptyKeyRemovedCount = 0;
    while (iterOmKeyInfo.hasNext()) {
      // Note with HDDS-8462, each RepeatedOmKeyInfo should have only one entry,
      // so this outer most loop should never be entered twice in each call.

      // But for completeness sake I shall put it here.
      // Remove only when RepeatedOmKeyInfo is no longer used.

      // Layer 2: OmKeyInfo
      OmKeyInfo oldOmKeyInfo = iterOmKeyInfo.next();
      // Layer 3: List<OmKeyLocationInfoGroup>
      Iterator<OmKeyLocationInfoGroup> iterKeyLocInfoGroup = oldOmKeyInfo
          .getKeyLocationVersions().iterator();
      while (iterKeyLocInfoGroup.hasNext()) {
        // Layer 4: OmKeyLocationInfoGroup
        OmKeyLocationInfoGroup keyLocInfoGroup = iterKeyLocInfoGroup.next();
        // Layer 5: Map<Long, List<OmKeyLocationInfo>>
        Iterator<Map.Entry<Long, List<OmKeyLocationInfo>>> iterVerMap =
            keyLocInfoGroup.getLocationVersionMap().entrySet().iterator();

        while (iterVerMap.hasNext()) {
          Map.Entry<Long, List<OmKeyLocationInfo>> mapEntry = iterVerMap.next();
          // Layer 6: List<OmKeyLocationInfo>
          List<OmKeyLocationInfo> omKeyLocationInfoList = mapEntry.getValue();

          Iterator<OmKeyLocationInfo> iterKeyLocInfo =
              omKeyLocationInfoList.iterator();
          while (iterKeyLocInfo.hasNext()) {
            // Layer 7: OmKeyLocationInfo
            OmKeyLocationInfo keyLocationInfo = iterKeyLocInfo.next();
            // Layer 8: BlockID. Then Layer 9: ContainerBlockID
            ContainerBlockID cbId = keyLocationInfo
                .getBlockID().getContainerBlockID();

            if (cbIdSet.containsKey(cbId)) {
              // Remove this block from oldVerKeyInfo because it is referenced.
              filteredOutBlocks.computeIfAbsent(oldOmKeyInfo, (k) -> new ArrayList<>()).add(keyLocationInfo);
              iterKeyLocInfo.remove();
              LOG.debug("Filtered out block: {}", cbId);
            }
          }

          // Cleanup when Layer 6 is an empty list
          if (omKeyLocationInfoList.isEmpty()) {
            iterVerMap.remove();
          }
        }

        // Cleanup when Layer 5 is an empty map
        if (keyLocInfoGroup.getLocationVersionMap().isEmpty()) {
          iterKeyLocInfoGroup.remove();
        }
      }

      // Cleanup when Layer 3 is an empty list
      if (oldOmKeyInfo.getKeyLocationVersions().isEmpty()) {
        emptyKeyRemovedCount++;
        iterOmKeyInfo.remove();
      }
    }

    // Intentional extra space for alignment
    LOG.debug("After block filtering,  keysToBeFiltered = {}",
        keysToBeFiltered);
    return Pair.of(filteredOutBlocks, emptyKeyRemovedCount);
  }

  protected void validateEncryptionKeyInfo(OmBucketInfo bucketInfo, KeyArgs keyArgs) throws OMException {
    if (bucketInfo.getEncryptionKeyInfo() != null && !keyArgs.hasFileEncryptionInfo()) {
      throw new OMException("Attempting to create unencrypted file " +
          keyArgs.getKeyName() + " in encrypted bucket " + keyArgs.getBucketName(), INVALID_REQUEST);
    }
  }
}
