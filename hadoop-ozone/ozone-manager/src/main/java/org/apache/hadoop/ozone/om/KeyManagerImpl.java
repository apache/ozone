/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.UniqueId;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUpload;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED_DEFAULT;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.READ;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.WRITE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_LIST_TRASH_KEYS_MAX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_LIST_TRASH_KEYS_MAX_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.ClientVersions.CURRENT_VERSION;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DIRECTORY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_KMS_PROVIDER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.SCM_GET_PIPELINE_EXCEPTION;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.KEY;
import static org.apache.hadoop.util.Time.monotonicNow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of keyManager.
 */
public class KeyManagerImpl implements KeyManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(KeyManagerImpl.class);

  /**
   * A SCM block client, used to talk to SCM to allocate block during putKey.
   */
  private final OzoneManager ozoneManager;
  private final ScmClient scmClient;
  private final OMMetadataManager metadataManager;
  private final long scmBlockSize;
  private final boolean useRatis;

  private final int preallocateBlocksMax;
  private final int listTrashKeysMax;
  private final String omId;
  private final OzoneBlockTokenSecretManager secretManager;
  private final boolean grpcBlockTokenEnabled;

  private BackgroundService keyDeletingService;

  private final KeyProviderCryptoExtension kmsProvider;
  private final PrefixManager prefixManager;

  private final boolean enableFileSystemPaths;


  @VisibleForTesting
  public KeyManagerImpl(ScmBlockLocationProtocol scmBlockClient,
      OMMetadataManager metadataManager, OzoneConfiguration conf, String omId,
      OzoneBlockTokenSecretManager secretManager) {
    this(null, new ScmClient(scmBlockClient, null), metadataManager,
        conf, omId, secretManager, null, null);
  }

  @VisibleForTesting
  public KeyManagerImpl(ScmBlockLocationProtocol scmBlockClient,
      StorageContainerLocationProtocol scmContainerClient,
      OMMetadataManager metadataManager, OzoneConfiguration conf, String omId,
      OzoneBlockTokenSecretManager secretManager) {
    this(null, new ScmClient(scmBlockClient, scmContainerClient),
        metadataManager, conf, omId, secretManager, null, null);
  }

  public KeyManagerImpl(OzoneManager om, ScmClient scmClient,
      OzoneConfiguration conf, String omId) {
    this (om, scmClient, om.getMetadataManager(), conf, omId,
        om.getBlockTokenMgr(), om.getKmsProvider(), om.getPrefixManager());
  }

  @SuppressWarnings("parameternumber")
  public KeyManagerImpl(OzoneManager om, ScmClient scmClient,
      OMMetadataManager metadataManager, OzoneConfiguration conf, String omId,
      OzoneBlockTokenSecretManager secretManager,
      KeyProviderCryptoExtension kmsProvider, PrefixManager prefixManager) {
    this.scmBlockSize = (long) conf
        .getStorageSize(OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT,
            StorageUnit.BYTES);
    this.useRatis = conf.getBoolean(DFS_CONTAINER_RATIS_ENABLED_KEY,
        DFS_CONTAINER_RATIS_ENABLED_DEFAULT);
    this.preallocateBlocksMax = conf.getInt(
        OZONE_KEY_PREALLOCATION_BLOCKS_MAX,
        OZONE_KEY_PREALLOCATION_BLOCKS_MAX_DEFAULT);
    this.grpcBlockTokenEnabled = conf.getBoolean(
        HDDS_BLOCK_TOKEN_ENABLED,
        HDDS_BLOCK_TOKEN_ENABLED_DEFAULT);
    this.listTrashKeysMax = conf.getInt(
      OZONE_CLIENT_LIST_TRASH_KEYS_MAX,
      OZONE_CLIENT_LIST_TRASH_KEYS_MAX_DEFAULT);
    this.enableFileSystemPaths =
        conf.getBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
            OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS_DEFAULT);

    this.ozoneManager = om;
    this.omId = omId;
    this.scmClient = scmClient;
    this.metadataManager = metadataManager;
    this.prefixManager = prefixManager;
    this.secretManager = secretManager;
    this.kmsProvider = kmsProvider;

  }

  @Override
  public void start(OzoneConfiguration configuration) {
    if (keyDeletingService == null) {
      long blockDeleteInterval = configuration.getTimeDuration(
          OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
          OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);
      long serviceTimeout = configuration.getTimeDuration(
          OZONE_BLOCK_DELETING_SERVICE_TIMEOUT,
          OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT,
          TimeUnit.MILLISECONDS);
      keyDeletingService = new KeyDeletingService(ozoneManager,
          scmClient.getBlockClient(), this, blockDeleteInterval,
          serviceTimeout, configuration);
      keyDeletingService.start();
    }
  }

  KeyProviderCryptoExtension getKMSProvider() {
    return kmsProvider;
  }

  @Override
  public void stop() throws IOException {
    if (keyDeletingService != null) {
      keyDeletingService.shutdown();
      keyDeletingService = null;
    }
  }

  private OmBucketInfo getBucketInfo(String volumeName, String bucketName)
      throws IOException {
    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
    return metadataManager.getBucketTable().get(bucketKey);
  }

  private void validateBucket(String volumeName, String bucketName)
      throws IOException {
    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
    // Check if bucket exists
    if (metadataManager.getBucketTable().get(bucketKey) == null) {
      String volumeKey = metadataManager.getVolumeKey(volumeName);
      // If the volume also does not exist, we should throw volume not found
      // exception
      if (metadataManager.getVolumeTable().get(volumeKey) == null) {
        LOG.error("volume not found: {}", volumeName);
        throw new OMException("Volume not found",
            VOLUME_NOT_FOUND);
      }

      // if the volume exists but bucket does not exist, throw bucket not found
      // exception
      LOG.error("bucket not found: {}/{} ", volumeName, bucketName);
      throw new OMException("Bucket not found",
          BUCKET_NOT_FOUND);
    }
  }

  /**
   * Check S3 bucket exists or not.
   * @param volumeName
   * @param bucketName
   * @throws IOException
   */
  private OmBucketInfo validateS3Bucket(String volumeName, String bucketName)
      throws IOException {

    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo = metadataManager.getBucketTable().
        get(bucketKey);
    //Check if bucket already exists
    if (omBucketInfo == null) {
      LOG.error("bucket not found: {}/{} ", volumeName, bucketName);
      throw new OMException("Bucket not found",
          BUCKET_NOT_FOUND);
    }
    return omBucketInfo;
  }

  @Override
  public OmKeyLocationInfo allocateBlock(OmKeyArgs args, long clientID,
      ExcludeList excludeList) throws IOException {
    Preconditions.checkNotNull(args);


    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    validateBucket(volumeName, bucketName);
    String openKey = metadataManager.getOpenKey(
        volumeName, bucketName, keyName, clientID);

    OmKeyInfo keyInfo = metadataManager.getOpenKeyTable().get(openKey);
    if (keyInfo == null) {
      LOG.error("Allocate block for a key not in open status in meta store" +
          " /{}/{}/{} with ID {}", volumeName, bucketName, keyName, clientID);
      throw new OMException("Open Key not found",
          KEY_NOT_FOUND);
    }

    // current version not committed, so new blocks coming now are added to
    // the same version
    List<OmKeyLocationInfo> locationInfos =
        allocateBlock(keyInfo, excludeList, scmBlockSize);

    keyInfo.appendNewBlocks(locationInfos, true);
    keyInfo.updateModifcationTime();
    metadataManager.getOpenKeyTable().put(openKey, keyInfo);

    return locationInfos.get(0);

  }

  /**
   * This methods avoids multiple rpc calls to SCM by allocating multiple blocks
   * in one rpc call.
   * @param keyInfo - key info for key to be allocated.
   * @param requestedSize requested length for allocation.
   * @param excludeList exclude list while allocating blocks.
   * @param requestedSize requested size to be allocated.
   * @return
   * @throws IOException
   */
  private List<OmKeyLocationInfo> allocateBlock(OmKeyInfo keyInfo,
      ExcludeList excludeList, long requestedSize) throws IOException {
    int numBlocks = Math.min((int) ((requestedSize - 1) / scmBlockSize + 1),
        preallocateBlocksMax);
    List<OmKeyLocationInfo> locationInfos = new ArrayList<>(numBlocks);
    String remoteUser = getRemoteUser().getShortUserName();
    List<AllocatedBlock> allocatedBlocks;
    try {
      allocatedBlocks = scmClient.getBlockClient()
          .allocateBlock(scmBlockSize, numBlocks, keyInfo.getType(),
              keyInfo.getFactor(), omId, excludeList);
    } catch (SCMException ex) {
      if (ex.getResult()
          .equals(SCMException.ResultCodes.SAFE_MODE_EXCEPTION)) {
        throw new OMException(ex.getMessage(), ResultCodes.SCM_IN_SAFE_MODE);
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
        builder.setToken(secretManager
            .generateToken(remoteUser, blockID,
                EnumSet.of(READ, WRITE), scmBlockSize));
      }
      locationInfos.add(builder.build());
    }
    return locationInfos;
  }

  /* Optimize ugi lookup for RPC operations to avoid a trip through
   * UGI.getCurrentUser which is synch'ed.
   */
  public static UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = Server.getRemoteUser();
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
  }

  private EncryptedKeyVersion generateEDEK(
      final String ezKeyName) throws IOException {
    if (ezKeyName == null) {
      return null;
    }
    long generateEDEKStartTime = monotonicNow();
    EncryptedKeyVersion edek = SecurityUtil.doAsLoginUser(
        new PrivilegedExceptionAction<EncryptedKeyVersion>() {
          @Override
          public EncryptedKeyVersion run() throws IOException {
            try {
              return getKMSProvider().generateEncryptedKey(ezKeyName);
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

  @Override
  public OpenKeySession openKey(OmKeyArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    Preconditions.checkNotNull(args.getAcls(), "Default acls " +
        "should be set.");

    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    validateBucket(volumeName, bucketName);

    long currentTime = UniqueId.next();
    OmKeyInfo keyInfo;
    long openVersion;
    // NOTE size of a key is not a hard limit on anything, it is a value that
    // client should expect, in terms of current size of key. If client sets
    // a value, then this value is used, otherwise, we allocate a single
    // block which is the current size, if read by the client.
    final long size = args.getDataSize() > 0 ?
        args.getDataSize() : scmBlockSize;
    final List<OmKeyLocationInfo> locations = new ArrayList<>();

    ReplicationFactor factor = args.getFactor();
    if (factor == null) {
      factor = useRatis ? ReplicationFactor.THREE : ReplicationFactor.ONE;
    }

    ReplicationType type = args.getType();
    if (type == null) {
      type = useRatis ? ReplicationType.RATIS : ReplicationType.STAND_ALONE;
    }

    String dbKeyName = metadataManager.getOzoneKey(
        args.getVolumeName(), args.getBucketName(), args.getKeyName());

    FileEncryptionInfo encInfo;
    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
        bucketName);
    OmBucketInfo bucketInfo;
    try {
      bucketInfo = getBucketInfo(volumeName, bucketName);
      encInfo = getFileEncryptionInfo(bucketInfo);
      keyInfo = prepareKeyInfo(args, dbKeyName, size, locations, encInfo);
    } catch (OMException e) {
      throw e;
    } catch (IOException ex) {
      LOG.error("Key open failed for volume:{} bucket:{} key:{}",
          volumeName, bucketName, keyName, ex);
      throw new OMException(ex.getMessage(), ResultCodes.KEY_ALLOCATION_ERROR);
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
          bucketName);
    }
    if (keyInfo == null) {
      // the key does not exist, create a new object, the new blocks are the
      // version 0
      keyInfo = createKeyInfo(args, locations, factor, type, size,
          encInfo, bucketInfo);
    }
    openVersion = keyInfo.getLatestVersionLocations().getVersion();
    LOG.debug("Key {} allocated in volume {} bucket {}",
        keyName, volumeName, bucketName);
    allocateBlockInKey(keyInfo, size, currentTime);
    return new OpenKeySession(currentTime, keyInfo, openVersion);
  }

  private void allocateBlockInKey(OmKeyInfo keyInfo, long size, long sessionId)
      throws IOException {
    String openKey = metadataManager
        .getOpenKey(keyInfo.getVolumeName(), keyInfo.getBucketName(),
            keyInfo.getKeyName(), sessionId);
    // requested size is not required but more like a optimization:
    // SCM looks at the requested, if it 0, no block will be allocated at
    // the point, if client needs more blocks, client can always call
    // allocateBlock. But if requested size is not 0, OM will preallocate
    // some blocks and piggyback to client, to save RPC calls.
    if (size > 0) {
      List<OmKeyLocationInfo> locationInfos =
          allocateBlock(keyInfo, new ExcludeList(), size);
      keyInfo.appendNewBlocks(locationInfos, true);
    }

    metadataManager.getOpenKeyTable().put(openKey, keyInfo);

  }

  private OmKeyInfo prepareKeyInfo(
      OmKeyArgs keyArgs, String dbKeyName, long size,
      List<OmKeyLocationInfo> locations, FileEncryptionInfo encInfo)
      throws IOException {
    OmKeyInfo keyInfo = null;
    if (keyArgs.getIsMultipartKey()) {
      keyInfo = prepareMultipartKeyInfo(keyArgs, size, locations, encInfo);
    } else if (metadataManager.getKeyTable().isExist(dbKeyName)) {
      keyInfo = metadataManager.getKeyTable().get(dbKeyName);
      // the key already exist, the new blocks will be added as new version
      // when locations.size = 0, the new version will have identical blocks
      // as its previous version
      keyInfo.addNewVersion(locations, true);
      keyInfo.setDataSize(size + keyInfo.getDataSize());
    }
    if(keyInfo != null) {
      keyInfo.setMetadata(keyArgs.getMetadata());
    }
    return keyInfo;
  }

  private OmKeyInfo prepareMultipartKeyInfo(OmKeyArgs args, long size,
      List<OmKeyLocationInfo> locations, FileEncryptionInfo encInfo)
      throws IOException {
    ReplicationFactor factor;
    ReplicationType type;

    Preconditions.checkArgument(args.getMultipartUploadPartNumber() > 0,
        "PartNumber Should be greater than zero");
    // When key is multipart upload part key, we should take replication
    // type and replication factor from original key which has done
    // initiate multipart upload. If we have not found any such, we throw
    // error no such multipart upload.
    String uploadID = args.getMultipartUploadID();
    Preconditions.checkNotNull(uploadID);
    String multipartKey = metadataManager
        .getMultipartKey(args.getVolumeName(), args.getBucketName(),
            args.getKeyName(), uploadID);
    OmKeyInfo partKeyInfo = metadataManager.getOpenKeyTable().get(
        multipartKey);
    if (partKeyInfo == null) {
      throw new OMException("No such Multipart upload is with specified " +
          "uploadId " + uploadID,
          ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
    } else {
      factor = partKeyInfo.getFactor();
      type = partKeyInfo.getType();
    }
    // For this upload part we don't need to check in KeyTable. As this
    // is not an actual key, it is a part of the key.
    return createKeyInfo(args, locations, factor, type, size, encInfo,
        getBucketInfo(args.getVolumeName(), args.getBucketName()));
  }

  /**
   * Create OmKeyInfo object.
   * @param keyArgs
   * @param locations
   * @param factor
   * @param type
   * @param size
   * @param encInfo
   * @param omBucketInfo
   * @return
   */
  private OmKeyInfo createKeyInfo(OmKeyArgs keyArgs,
      List<OmKeyLocationInfo> locations,
      ReplicationFactor factor,
      ReplicationType type, long size,
      FileEncryptionInfo encInfo,
      OmBucketInfo omBucketInfo) {
    OmKeyInfo.Builder builder = new OmKeyInfo.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, locations)))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(size)
        .setReplicationType(type)
        .setReplicationFactor(factor)
        .setFileEncryptionInfo(encInfo)
        .addAllMetadata(keyArgs.getMetadata());
    builder.setAcls(getAclsForKey(keyArgs, omBucketInfo));

    if(Boolean.valueOf(omBucketInfo.getMetadata().get(OzoneConsts.GDPR_FLAG))) {
      builder.addMetadata(OzoneConsts.GDPR_FLAG, Boolean.TRUE.toString());
    }
    return builder.build();
  }

  @Override
  public void commitKey(OmKeyArgs args, long clientID) throws IOException {
    Preconditions.checkNotNull(args);
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    List<OmKeyLocationInfo> locationInfoList = args.getLocationInfoList();
    String objectKey = metadataManager
        .getOzoneKey(volumeName, bucketName, keyName);
    String openKey = metadataManager
        .getOpenKey(volumeName, bucketName, keyName, clientID);
    Preconditions.checkNotNull(locationInfoList);
    try {
      metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
          bucketName);
      validateBucket(volumeName, bucketName);
      OmKeyInfo keyInfo = metadataManager.getOpenKeyTable().get(openKey);
      if (keyInfo == null) {
        throw new OMException("Failed to commit key, as " + openKey + "entry " +
            "is not found in the openKey table", KEY_NOT_FOUND);
      }
      keyInfo.setDataSize(args.getDataSize());
      keyInfo.setModificationTime(Time.now());

      //update the block length for each block
      keyInfo.updateLocationInfoList(locationInfoList, false);
      metadataManager.getStore().move(
          openKey,
          objectKey,
          keyInfo,
          metadataManager.getOpenKeyTable(),
          metadataManager.getKeyTable());
    } catch (OMException e) {
      throw e;
    } catch (IOException ex) {
      LOG.error("Key commit failed for volume:{} bucket:{} key:{}",
          volumeName, bucketName, keyName, ex);
      throw new OMException(ex.getMessage(),
          ResultCodes.KEY_ALLOCATION_ERROR);
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
          bucketName);
    }
  }

  @Override
  public OmKeyInfo lookupKey(OmKeyArgs args, String clientAddress)
      throws IOException {
    Preconditions.checkNotNull(args);
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = OMClientRequest.validateAndNormalizeKey(
        enableFileSystemPaths, args.getKeyName());
    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volumeName,
        bucketName);
    OmKeyInfo value = null;
    try {
      String keyBytes = metadataManager.getOzoneKey(
          volumeName, bucketName, keyName);
      value = metadataManager.getKeyTable().get(keyBytes);
    } catch (IOException ex) {
      if (ex instanceof OMException) {
        throw ex;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Get key failed for volume:{} bucket:{} key:{}", volumeName,
                bucketName, keyName, ex);
      }
      throw new OMException(ex.getMessage(), KEY_NOT_FOUND);
    } finally {
      metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volumeName,
          bucketName);
    }

    if (value == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("volume:{} bucket:{} Key:{} not found", volumeName,
                bucketName, keyName);
      }
      throw new OMException("Key not found", KEY_NOT_FOUND);
    }

    // add block token for read.
    addBlockToken4Read(value);

    // Refresh container pipeline info from SCM
    // based on OmKeyArgs.refreshPipeline flag
    // value won't be null as the check is done inside try/catch block.
    refresh(value);

    if (args.getSortDatanodes()) {
      sortDatanodes(clientAddress, value);
    }
    return value;
  }

  private void addBlockToken4Read(OmKeyInfo value) throws IOException {
    Preconditions.checkNotNull(value, "OMKeyInfo cannot be null");
    if (grpcBlockTokenEnabled) {
      String remoteUser = getRemoteUser().getShortUserName();
      for (OmKeyLocationInfoGroup key : value.getKeyLocationVersions()) {
        key.getLocationList().forEach(k -> {
          k.setToken(secretManager.generateToken(remoteUser, k.getBlockID(),
              EnumSet.of(READ), k.getLength()));
        });
      }
    }
  }
  /**
   * Refresh pipeline info in OM by asking SCM.
   * @param keyList a list of OmKeyInfo
   */
  @VisibleForTesting
  protected void refreshPipeline(List<OmKeyInfo> keyList) throws IOException {
    if (keyList == null || keyList.isEmpty()) {
      return;
    }

    Set<Long> containerIDs = new HashSet<>();
    for (OmKeyInfo keyInfo : keyList) {
      List<OmKeyLocationInfoGroup> locationInfoGroups =
          keyInfo.getKeyLocationVersions();

      for (OmKeyLocationInfoGroup key : locationInfoGroups) {
        for (OmKeyLocationInfo k : key.getLocationList()) {
          containerIDs.add(k.getContainerID());
        }
      }
    }

    Map<Long, ContainerWithPipeline> containerWithPipelineMap =
        refreshPipeline(containerIDs);

    for (OmKeyInfo keyInfo : keyList) {
      List<OmKeyLocationInfoGroup> locationInfoGroups =
          keyInfo.getKeyLocationVersions();
      for (OmKeyLocationInfoGroup key : locationInfoGroups) {
        for (OmKeyLocationInfo k : key.getLocationList()) {
          ContainerWithPipeline cp =
              containerWithPipelineMap.get(k.getContainerID());
          if (cp != null && !cp.getPipeline().equals(k.getPipeline())) {
            k.setPipeline(cp.getPipeline());
          }
        }
      }
    }
  }

  /**
   * Refresh pipeline info in OM by asking SCM.
   * @param containerIDs a set of containerIDs
   */
  @VisibleForTesting
  protected Map<Long, ContainerWithPipeline> refreshPipeline(
      Set<Long> containerIDs) throws IOException {
    // TODO: fix Some tests that may not initialize container client
    // The production should always have containerClient initialized.
    if (scmClient.getContainerClient() == null ||
        containerIDs == null || containerIDs.isEmpty()) {
      return Collections.EMPTY_MAP;
    }

    Map<Long, ContainerWithPipeline> containerWithPipelineMap = new HashMap<>();

    try {
      List<ContainerWithPipeline> cpList = scmClient.getContainerClient().
          getContainerWithPipelineBatch(new ArrayList<>(containerIDs));
      for (ContainerWithPipeline cp : cpList) {
        containerWithPipelineMap.put(
            cp.getContainerInfo().getContainerID(), cp);
      }
      return containerWithPipelineMap;
    } catch (IOException ioEx) {
      LOG.debug("Get containerPipeline failed for {}",
          containerIDs.toString(), ioEx);
      throw new OMException(ioEx.getMessage(), SCM_GET_PIPELINE_EXCEPTION);
    }
  }

  @Override
  public void renameKey(OmKeyArgs args, String toKeyName) throws IOException {
    Preconditions.checkNotNull(args);
    Preconditions.checkNotNull(toKeyName);
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String fromKeyName = args.getKeyName();
    if (toKeyName.length() == 0 || fromKeyName.length() == 0) {
      LOG.error("Rename key failed for volume:{} bucket:{} fromKey:{} toKey:{}",
          volumeName, bucketName, fromKeyName, toKeyName);
      throw new OMException("Key name is empty",
          ResultCodes.INVALID_KEY_NAME);
    }

    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
        bucketName);
    try {
      // fromKeyName should exist
      String fromKey = metadataManager.getOzoneKey(
          volumeName, bucketName, fromKeyName);
      OmKeyInfo fromKeyValue = metadataManager.getKeyTable().get(fromKey);
      if (fromKeyValue == null) {
        // TODO: Add support for renaming open key
        LOG.error(
            "Rename key failed for volume:{} bucket:{} fromKey:{} toKey:{}. "
                + "Key: {} not found.", volumeName, bucketName, fromKeyName,
            toKeyName, fromKeyName);
        throw new OMException("Key not found",
            KEY_NOT_FOUND);
      }

      // A rename is a no-op if the target and source name is same.
      // TODO: Discuss if we need to throw?.
      if (fromKeyName.equals(toKeyName)) {
        return;
      }

      // toKeyName should not exist
      String toKey =
          metadataManager.getOzoneKey(volumeName, bucketName, toKeyName);
      OmKeyInfo toKeyValue = metadataManager.getKeyTable().get(toKey);
      if (toKeyValue != null) {
        LOG.error(
            "Rename key failed for volume:{} bucket:{} fromKey:{} toKey:{}. "
                + "Key: {} already exists.", volumeName, bucketName,
            fromKeyName, toKeyName, toKeyName);
        throw new OMException("Key already exists",
            OMException.ResultCodes.KEY_ALREADY_EXISTS);
      }

      fromKeyValue.setKeyName(toKeyName);
      fromKeyValue.updateModifcationTime();
      DBStore store = metadataManager.getStore();
      try (BatchOperation batch = store.initBatchOperation()) {
        metadataManager.getKeyTable().deleteWithBatch(batch, fromKey);
        metadataManager.getKeyTable().putWithBatch(batch, toKey,
            fromKeyValue);
        store.commitBatchOperation(batch);
      }
    } catch (IOException ex) {
      if (ex instanceof OMException) {
        throw ex;
      }
      LOG.error("Rename key failed for volume:{} bucket:{} fromKey:{} toKey:{}",
          volumeName, bucketName, fromKeyName, toKeyName, ex);
      throw new OMException(ex.getMessage(),
          ResultCodes.KEY_RENAME_ERROR);
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
          bucketName);
    }
  }

  @Override
  public void deleteKey(OmKeyArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
        bucketName);
    try {
      String objectKey = metadataManager.getOzoneKey(
          volumeName, bucketName, keyName);
      OmKeyInfo keyInfo = metadataManager.getKeyTable().get(objectKey);
      if (keyInfo == null) {
        throw new OMException("Key not found",
            KEY_NOT_FOUND);
      } else {
        // directly delete key with no blocks from db. This key need not be
        // moved to deleted table.
        if (isKeyEmpty(keyInfo)) {
          metadataManager.getKeyTable().delete(objectKey);
          LOG.debug("Key {} deleted from OM DB", keyName);
          return;
        }
      }
      RepeatedOmKeyInfo repeatedOmKeyInfo =
          metadataManager.getDeletedTable().get(objectKey);
      repeatedOmKeyInfo = OmUtils.prepareKeyForDelete(keyInfo,
          repeatedOmKeyInfo, 0L, false);
      metadataManager.getKeyTable().delete(objectKey);
      metadataManager.getDeletedTable().put(objectKey, repeatedOmKeyInfo);
    } catch (OMException ex) {
      throw ex;
    } catch (IOException ex) {
      LOG.error(String.format("Delete key failed for volume:%s "
          + "bucket:%s key:%s", volumeName, bucketName, keyName), ex);
      throw new OMException(ex.getMessage(), ex,
          ResultCodes.KEY_DELETION_ERROR);
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
          bucketName);
    }
  }

  private boolean isKeyEmpty(OmKeyInfo keyInfo) {
    for (OmKeyLocationInfoGroup keyLocationList : keyInfo
        .getKeyLocationVersions()) {
      if (keyLocationList.getLocationListCount() != 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public List<OmKeyInfo> listKeys(String volumeName, String bucketName,
      String startKey, String keyPrefix,
      int maxKeys) throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);

    // We don't take a lock in this path, since we walk the
    // underlying table using an iterator. That automatically creates a
    // snapshot of the data, so we don't need these locks at a higher level
    // when we iterate.

    if (enableFileSystemPaths) {
      startKey = OmUtils.normalizeKey(startKey, true);
      keyPrefix = OmUtils.normalizeKey(keyPrefix, true);
    }

    List<OmKeyInfo> keyList = metadataManager.listKeys(volumeName, bucketName,
        startKey, keyPrefix, maxKeys);

    return keyList;
  }

  @Override
  public List<RepeatedOmKeyInfo> listTrash(String volumeName,
      String bucketName, String startKeyName, String keyPrefix,
      int maxKeys) throws IOException {

    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkArgument(maxKeys <= listTrashKeysMax,
        "The max keys limit specified is not less than the cluster " +
          "allowed maximum limit.");

    return metadataManager.listTrash(volumeName, bucketName,
     startKeyName, keyPrefix, maxKeys);
  }

  @Override
  public List<BlockGroup> getPendingDeletionKeys(final int count)
      throws IOException {
    return  metadataManager.getPendingDeletionKeys(count);
  }

  @Override
  public List<String> getExpiredOpenKeys(int count) throws IOException {
    return metadataManager.getExpiredOpenKeys(count);
  }

  @Override
  public void deleteExpiredOpenKey(String objectKeyName) throws IOException {
    Preconditions.checkNotNull(objectKeyName);
    // TODO: Fix this in later patches.
  }

  @Override
  public OMMetadataManager getMetadataManager() {
    return metadataManager;
  }

  @Override
  public BackgroundService getDeletingService() {
    return keyDeletingService;
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(OmKeyArgs omKeyArgs) throws
      IOException {
    Preconditions.checkNotNull(omKeyArgs);
    String uploadID = UUID.randomUUID().toString() + "-" + UniqueId.next();
    return createMultipartInfo(omKeyArgs, uploadID);
  }

  private OmMultipartInfo createMultipartInfo(OmKeyArgs keyArgs,
      String multipartUploadID) throws IOException {
    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
        bucketName);
    OmBucketInfo bucketInfo = validateS3Bucket(volumeName, bucketName);
    try {

      // We are adding uploadId to key, because if multiple users try to
      // perform multipart upload on the same key, each will try to upload, who
      // ever finally commit the key, we see that key in ozone. Suppose if we
      // don't add id, and use the same key /volume/bucket/key, when multiple
      // users try to upload the key, we update the parts of the key's from
      // multiple users to same key, and the key output can be a mix of the
      // parts from multiple users.

      // So on same key if multiple time multipart upload is initiated we
      // store multiple entries in the openKey Table.
      // Checked AWS S3, when we try to run multipart upload, each time a
      // new uploadId is returned.

      String multipartKey = metadataManager.getMultipartKey(volumeName,
          bucketName, keyName, multipartUploadID);

      // Not checking if there is an already key for this in the keyTable, as
      // during final complete multipart upload we take care of this.

      long currentTime = Time.now();
      Map<Integer, PartKeyInfo> partKeyInfoMap = new HashMap<>();
      OmMultipartKeyInfo multipartKeyInfo = new OmMultipartKeyInfo.Builder()
          .setUploadID(multipartUploadID)
          .setCreationTime(currentTime)
          .setReplicationType(keyArgs.getType())
          .setReplicationFactor(keyArgs.getFactor())
          .setPartKeyInfoList(partKeyInfoMap)
          .build();
      Map<Long, List<OmKeyLocationInfo>> locations = new HashMap<>();
      OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
          .setVolumeName(keyArgs.getVolumeName())
          .setBucketName(keyArgs.getBucketName())
          .setKeyName(keyArgs.getKeyName())
          .setCreationTime(currentTime)
          .setModificationTime(currentTime)
          .setReplicationType(keyArgs.getType())
          .setReplicationFactor(keyArgs.getFactor())
          .setOmKeyLocationInfos(Collections.singletonList(
              new OmKeyLocationInfoGroup(0, locations)))
          .setAcls(getAclsForKey(keyArgs, bucketInfo))
          .build();
      DBStore store = metadataManager.getStore();
      try (BatchOperation batch = store.initBatchOperation()) {
        // Create an entry in open key table and multipart info table for
        // this key.
        metadataManager.getMultipartInfoTable().putWithBatch(batch,
            multipartKey, multipartKeyInfo);
        metadataManager.getOpenKeyTable().putWithBatch(batch,
            multipartKey, omKeyInfo);
        store.commitBatchOperation(batch);
        return new OmMultipartInfo(volumeName, bucketName, keyName,
            multipartUploadID);
      }
    } catch (IOException ex) {
      LOG.error("Initiate Multipart upload Failed for volume:{} bucket:{} " +
          "key:{}", volumeName, bucketName, keyName, ex);
      throw new OMException(ex.getMessage(),
          ResultCodes.INITIATE_MULTIPART_UPLOAD_ERROR);
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
          bucketName);
    }
  }

  private List<OzoneAcl> getAclsForKey(OmKeyArgs keyArgs,
      OmBucketInfo bucketInfo) {
    List<OzoneAcl> acls = new ArrayList<>();

    if(keyArgs.getAcls() != null) {
      acls.addAll(keyArgs.getAcls());
    }

    // Inherit DEFAULT acls from prefix.
    if(prefixManager != null) {
      List<OmPrefixInfo> prefixList = prefixManager.getLongestPrefixPath(
          OZONE_URI_DELIMITER +
              keyArgs.getVolumeName() + OZONE_URI_DELIMITER +
              keyArgs.getBucketName() + OZONE_URI_DELIMITER +
              keyArgs.getKeyName());

      if (!prefixList.isEmpty()) {
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

    // TODO: do we need to further fallback to volume default ACL
    return acls;
  }

  @Override
  public OmMultipartCommitUploadPartInfo commitMultipartUploadPart(
      OmKeyArgs omKeyArgs, long clientID) throws IOException {
    Preconditions.checkNotNull(omKeyArgs);
    String volumeName = omKeyArgs.getVolumeName();
    String bucketName = omKeyArgs.getBucketName();
    String keyName = omKeyArgs.getKeyName();
    String uploadID = omKeyArgs.getMultipartUploadID();
    int partNumber = omKeyArgs.getMultipartUploadPartNumber();

    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
        bucketName);
    validateS3Bucket(volumeName, bucketName);
    String partName;
    try {
      String multipartKey = metadataManager.getMultipartKey(volumeName,
          bucketName, keyName, uploadID);
      OmMultipartKeyInfo multipartKeyInfo = metadataManager
          .getMultipartInfoTable().get(multipartKey);

      String openKey = metadataManager.getOpenKey(
          volumeName, bucketName, keyName, clientID);
      OmKeyInfo keyInfo = metadataManager.getOpenKeyTable().get(
          openKey);

      // set the data size and location info list
      keyInfo.setDataSize(omKeyArgs.getDataSize());
      keyInfo.updateLocationInfoList(omKeyArgs.getLocationInfoList(), true);

      partName = metadataManager.getOzoneKey(volumeName, bucketName, keyName)
          + clientID;
      if (multipartKeyInfo == null) {
        // This can occur when user started uploading part by the time commit
        // of that part happens, in between the user might have requested
        // abort multipart upload. If we just throw exception, then the data
        // will not be garbage collected, so move this part to delete table
        // and throw error
        // Move this part to delete table.
        RepeatedOmKeyInfo repeatedOmKeyInfo =
            metadataManager.getDeletedTable().get(partName);
        repeatedOmKeyInfo = OmUtils.prepareKeyForDelete(
            keyInfo, repeatedOmKeyInfo, 0L, false);
        metadataManager.getDeletedTable().put(partName, repeatedOmKeyInfo);
        throw new OMException("No such Multipart upload is with specified " +
            "uploadId " + uploadID, ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
      } else {
        PartKeyInfo oldPartKeyInfo =
            multipartKeyInfo.getPartKeyInfo(partNumber);
        PartKeyInfo.Builder partKeyInfo = PartKeyInfo.newBuilder();
        partKeyInfo.setPartName(partName);
        partKeyInfo.setPartNumber(partNumber);
        // TODO remove unused write code path
        partKeyInfo.setPartKeyInfo(keyInfo.getProtobuf(CURRENT_VERSION));
        multipartKeyInfo.addPartKeyInfo(partNumber, partKeyInfo.build());
        if (oldPartKeyInfo == null) {
          // This is the first time part is being added.
          DBStore store = metadataManager.getStore();
          try (BatchOperation batch = store.initBatchOperation()) {
            metadataManager.getOpenKeyTable().deleteWithBatch(batch, openKey);
            metadataManager.getMultipartInfoTable().putWithBatch(batch,
                multipartKey, multipartKeyInfo);
            store.commitBatchOperation(batch);
          }
        } else {
          // If we have this part already, that means we are overriding it.
          // We need to 3 steps.
          // Add the old entry to delete table.
          // Remove the new entry from openKey table.
          // Add the new entry in to the list of part keys.
          DBStore store = metadataManager.getStore();
          try (BatchOperation batch = store.initBatchOperation()) {
            OmKeyInfo partKey = OmKeyInfo.getFromProtobuf(
                oldPartKeyInfo.getPartKeyInfo());

            RepeatedOmKeyInfo repeatedOmKeyInfo =
                metadataManager.getDeletedTable()
                    .get(oldPartKeyInfo.getPartName());

            repeatedOmKeyInfo = OmUtils.prepareKeyForDelete(
                partKey, repeatedOmKeyInfo, 0L, false);

            metadataManager.getDeletedTable().put(partName, repeatedOmKeyInfo);
            metadataManager.getDeletedTable().putWithBatch(batch,
                oldPartKeyInfo.getPartName(),
                repeatedOmKeyInfo);
            metadataManager.getOpenKeyTable().deleteWithBatch(batch, openKey);
            metadataManager.getMultipartInfoTable().putWithBatch(batch,
                multipartKey, multipartKeyInfo);
            store.commitBatchOperation(batch);
          }
        }
      }
    } catch (IOException ex) {
      LOG.error("Upload part Failed: volume:{} bucket:{} " +
          "key:{} PartNumber: {}", volumeName, bucketName, keyName,
          partNumber, ex);
      throw new OMException(ex.getMessage(),
          ResultCodes.MULTIPART_UPLOAD_PARTFILE_ERROR);
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
          bucketName);
    }

    return new OmMultipartCommitUploadPartInfo(partName);

  }

  @Override
  @SuppressWarnings("methodlength")
  public OmMultipartUploadCompleteInfo completeMultipartUpload(
      OmKeyArgs omKeyArgs, OmMultipartUploadCompleteList multipartUploadList)
      throws IOException {
    Preconditions.checkNotNull(omKeyArgs);
    Preconditions.checkNotNull(multipartUploadList);
    String volumeName = omKeyArgs.getVolumeName();
    String bucketName = omKeyArgs.getBucketName();
    String keyName = omKeyArgs.getKeyName();
    String uploadID = omKeyArgs.getMultipartUploadID();
    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
        bucketName);
    validateS3Bucket(volumeName, bucketName);
    try {
      String multipartKey = metadataManager.getMultipartKey(volumeName,
          bucketName, keyName, uploadID);

      OmMultipartKeyInfo multipartKeyInfo = metadataManager
          .getMultipartInfoTable().get(multipartKey);
      if (multipartKeyInfo == null) {
        throw new OMException("Complete Multipart Upload Failed: volume: " +
            volumeName + "bucket: " + bucketName + "key: " + keyName,
            ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
      }
      //TODO: Actual logic has been removed from this, and the old code has a
      // bug. New code for this is in S3MultipartUploadCompleteRequest.
      // This code will be cleaned up as part of HDDS-2353.

      return new OmMultipartUploadCompleteInfo(omKeyArgs.getVolumeName(),
          omKeyArgs.getBucketName(), omKeyArgs.getKeyName(), DigestUtils
              .sha256Hex(keyName));
    } catch (OMException ex) {
      throw ex;
    } catch (IOException ex) {
      LOG.error("Complete Multipart Upload Failed: volume: " + volumeName +
          "bucket: " + bucketName + "key: " + keyName, ex);
      throw new OMException(ex.getMessage(), ResultCodes
          .COMPLETE_MULTIPART_UPLOAD_ERROR);
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
          bucketName);
    }
  }

  @Override
  public void abortMultipartUpload(OmKeyArgs omKeyArgs) throws IOException {

    Preconditions.checkNotNull(omKeyArgs);
    String volumeName = omKeyArgs.getVolumeName();
    String bucketName = omKeyArgs.getBucketName();
    String keyName = omKeyArgs.getKeyName();
    String uploadID = omKeyArgs.getMultipartUploadID();
    Preconditions.checkNotNull(uploadID, "uploadID cannot be null");
    validateS3Bucket(volumeName, bucketName);
    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
        bucketName);
    OmBucketInfo bucketInfo;
    try {
      String multipartKey = metadataManager.getMultipartKey(volumeName,
          bucketName, keyName, uploadID);
      OmMultipartKeyInfo multipartKeyInfo = metadataManager
          .getMultipartInfoTable().get(multipartKey);
      OmKeyInfo openKeyInfo = metadataManager.getOpenKeyTable().get(
          multipartKey);

      // If there is no entry in openKeyTable, then there is no multipart
      // upload initiated for this key.
      if (openKeyInfo == null) {
        LOG.error("Abort Multipart Upload Failed: volume: {} bucket: {} "
                + "key: {} with error no such uploadID: {}", volumeName,
                bucketName, keyName, uploadID);
        throw new OMException("Abort Multipart Upload Failed: volume: " +
            volumeName + "bucket: " + bucketName + "key: " + keyName,
            ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
      } else {
        // Move all the parts to delete table
        TreeMap<Integer, PartKeyInfo> partKeyInfoMap = multipartKeyInfo
            .getPartKeyInfoMap();
        DBStore store = metadataManager.getStore();
        try (BatchOperation batch = store.initBatchOperation()) {
          for (Map.Entry<Integer, PartKeyInfo> partKeyInfoEntry : partKeyInfoMap
              .entrySet()) {
            PartKeyInfo partKeyInfo = partKeyInfoEntry.getValue();
            OmKeyInfo currentKeyPartInfo = OmKeyInfo.getFromProtobuf(
                partKeyInfo.getPartKeyInfo());

            RepeatedOmKeyInfo repeatedOmKeyInfo =
                metadataManager.getDeletedTable()
                    .get(partKeyInfo.getPartName());

            repeatedOmKeyInfo = OmUtils.prepareKeyForDelete(
                currentKeyPartInfo, repeatedOmKeyInfo, 0L, false);

            metadataManager.getDeletedTable().putWithBatch(batch,
                partKeyInfo.getPartName(), repeatedOmKeyInfo);
          }
          // Finally delete the entry from the multipart info table and open
          // key table
          metadataManager.getMultipartInfoTable().deleteWithBatch(batch,
              multipartKey);
          metadataManager.getOpenKeyTable().deleteWithBatch(batch,
              multipartKey);
          store.commitBatchOperation(batch);
        }
      }
    } catch (OMException ex) {
      throw ex;
    } catch (IOException ex) {
      LOG.error("Abort Multipart Upload Failed: volume: " + volumeName +
          "bucket: " + bucketName + "key: " + keyName, ex);
      throw new OMException(ex.getMessage(), ResultCodes
          .ABORT_MULTIPART_UPLOAD_FAILED);
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
          bucketName);
    }

  }

  @Override
  public OmMultipartUploadList listMultipartUploads(String volumeName,
      String bucketName, String prefix) throws OMException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);

    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volumeName,
        bucketName);
    try {

      Set<String> multipartUploadKeys =
          metadataManager
              .getMultipartUploadKeys(volumeName, bucketName, prefix);

      List<OmMultipartUpload> collect = multipartUploadKeys.stream()
          .map(OmMultipartUpload::from)
          .peek(upload -> {
            try {
              Table<String, OmMultipartKeyInfo> keyInfoTable =
                  metadataManager.getMultipartInfoTable();

              OmMultipartKeyInfo multipartKeyInfo =
                  keyInfoTable.get(upload.getDbKey());

              upload.setCreationTime(
                  Instant.ofEpochMilli(multipartKeyInfo.getCreationTime()));
              upload.setReplicationType(
                  multipartKeyInfo.getReplicationType());
              upload.setReplicationFactor(
                  multipartKeyInfo.getReplicationFactor());
            } catch (IOException e) {
              LOG.warn(
                  "Open key entry for multipart upload record can be read  {}",
                  metadataManager.getOzoneKey(upload.getVolumeName(),
                          upload.getBucketName(), upload.getKeyName()));
            }
          })
          .collect(Collectors.toList());

      return new OmMultipartUploadList(collect);

    } catch (IOException ex) {
      LOG.error("List Multipart Uploads Failed: volume: " + volumeName +
          "bucket: " + bucketName + "prefix: " + prefix, ex);
      throw new OMException(ex.getMessage(), ResultCodes
          .LIST_MULTIPART_UPLOAD_PARTS_FAILED);
    } finally {
      metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volumeName,
          bucketName);
    }
  }

  @Override
  public OmMultipartUploadListParts listParts(String volumeName,
      String bucketName, String keyName, String uploadID,
      int partNumberMarker, int maxParts)  throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkNotNull(keyName);
    Preconditions.checkNotNull(uploadID);
    boolean isTruncated = false;
    int nextPartNumberMarker = 0;

    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volumeName,
        bucketName);
    try {
      String multipartKey = metadataManager.getMultipartKey(volumeName,
          bucketName, keyName, uploadID);

      OmMultipartKeyInfo multipartKeyInfo =
          metadataManager.getMultipartInfoTable().get(multipartKey);

      if (multipartKeyInfo == null) {
        throw new OMException("No Such Multipart upload exists for this key.",
            ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
      } else {
        TreeMap<Integer, PartKeyInfo> partKeyInfoMap =
            multipartKeyInfo.getPartKeyInfoMap();
        Iterator<Map.Entry<Integer, PartKeyInfo>> partKeyInfoMapIterator =
            partKeyInfoMap.entrySet().iterator();

        HddsProtos.ReplicationType replicationType = null;
        HddsProtos.ReplicationFactor replicationFactor = null;

        int count = 0;
        List<OmPartInfo> omPartInfoList = new ArrayList<>();

        while (count < maxParts && partKeyInfoMapIterator.hasNext()) {
          Map.Entry<Integer, PartKeyInfo> partKeyInfoEntry =
              partKeyInfoMapIterator.next();
          nextPartNumberMarker = partKeyInfoEntry.getKey();
          // As we should return only parts with part number greater
          // than part number marker
          if (partKeyInfoEntry.getKey() > partNumberMarker) {
            PartKeyInfo partKeyInfo = partKeyInfoEntry.getValue();
            OmPartInfo omPartInfo = new OmPartInfo(partKeyInfo.getPartNumber(),
                partKeyInfo.getPartName(),
                partKeyInfo.getPartKeyInfo().getModificationTime(),
                partKeyInfo.getPartKeyInfo().getDataSize());
            omPartInfoList.add(omPartInfo);

            //if there are parts, use replication type from one of the parts
            replicationType = partKeyInfo.getPartKeyInfo().getType();
            replicationFactor = partKeyInfo.getPartKeyInfo().getFactor();
            count++;
          }
        }

        if (replicationType == null) {
          //if there are no parts, use the replicationType from the open key.

          OmKeyInfo omKeyInfo =
              metadataManager.getOpenKeyTable().get(multipartKey);

          if (omKeyInfo == null) {
            throw new IllegalStateException(
                "Open key is missing for multipart upload " + multipartKey);
          }

          replicationType = omKeyInfo.getType();
          replicationFactor = omKeyInfo.getFactor();
        }
        Preconditions.checkNotNull(replicationType,
            "Replication type can't be identified");
        Preconditions.checkNotNull(replicationFactor,
            "Replication factor can't be identified");

        if (partKeyInfoMapIterator.hasNext()) {
          Map.Entry<Integer, PartKeyInfo> partKeyInfoEntry =
              partKeyInfoMapIterator.next();
          isTruncated = true;
        } else {
          isTruncated = false;
          nextPartNumberMarker = 0;
        }
        OmMultipartUploadListParts omMultipartUploadListParts =
            new OmMultipartUploadListParts(replicationType, replicationFactor,
                nextPartNumberMarker, isTruncated);
        omMultipartUploadListParts.addPartList(omPartInfoList);
        return omMultipartUploadListParts;
      }
    } catch (OMException ex) {
      throw ex;
    } catch (IOException ex){
      LOG.error(
          "List Multipart Upload Parts Failed: volume: {}, bucket: {}, ,key: "
              + "{} ",
          volumeName, bucketName, keyName, ex);
      throw new OMException(ex.getMessage(), ResultCodes
              .LIST_MULTIPART_UPLOAD_PARTS_FAILED);
    } finally {
      metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volumeName,
          bucketName);
    }
  }

  /**
   * Add acl for Ozone object. Return true if acl is added successfully else
   * false.
   *
   * @param obj Ozone object for which acl should be added.
   * @param acl ozone acl to be added.
   * @throws IOException if there is error.
   */
  @Override
  public boolean addAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    validateOzoneObj(obj);
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    String keyName = obj.getKeyName();
    boolean changed = false;


    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volume, bucket);
    try {
      validateBucket(volume, bucket);
      String objectKey = metadataManager.getOzoneKey(volume, bucket, keyName);
      OmKeyInfo keyInfo = metadataManager.getKeyTable().get(objectKey);
      if (keyInfo == null) {
        throw new OMException("Key not found. Key:" + objectKey, KEY_NOT_FOUND);
      }

      if (keyInfo.getAcls() == null) {
        keyInfo.setAcls(new ArrayList<>());
      }
      changed = keyInfo.addAcl(acl);
      if (changed) {
        metadataManager.getKeyTable().put(objectKey, keyInfo);
      }
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Add acl operation failed for key:{}/{}/{}", volume,
            bucket, keyName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volume, bucket);
    }
    return changed;
  }

  /**
   * Remove acl for Ozone object. Return true if acl is removed successfully
   * else false.
   *
   * @param obj Ozone object.
   * @param acl Ozone acl to be removed.
   * @throws IOException if there is error.
   */
  @Override
  public boolean removeAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    validateOzoneObj(obj);
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    String keyName = obj.getKeyName();
    boolean changed = false;

    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volume, bucket);
    try {
      validateBucket(volume, bucket);
      String objectKey = metadataManager.getOzoneKey(volume, bucket, keyName);
      OmKeyInfo keyInfo = metadataManager.getKeyTable().get(objectKey);
      if (keyInfo == null) {
        throw new OMException("Key not found. Key:" + objectKey, KEY_NOT_FOUND);
      }

      changed = keyInfo.removeAcl(acl);
      if (changed) {
        metadataManager.getKeyTable().put(objectKey, keyInfo);
      }
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Remove acl operation failed for key:{}/{}/{}", volume,
            bucket, keyName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volume, bucket);
    }
    return changed;
  }

  /**
   * Acls to be set for given Ozone object. This operations reset ACL for given
   * object to list of ACLs provided in argument.
   *
   * @param obj Ozone object.
   * @param acls List of acls.
   * @throws IOException if there is error.
   */
  @Override
  public boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException {
    validateOzoneObj(obj);
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    String keyName = obj.getKeyName();
    boolean changed = false;

    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volume, bucket);
    try {
      validateBucket(volume, bucket);
      String objectKey = metadataManager.getOzoneKey(volume, bucket, keyName);
      OmKeyInfo keyInfo = metadataManager.getKeyTable().get(objectKey);
      if (keyInfo == null) {
        throw new OMException("Key not found. Key:" + objectKey, KEY_NOT_FOUND);
      }

      changed = keyInfo.setAcls(acls);

      if (changed) {
        metadataManager.getKeyTable().put(objectKey, keyInfo);
      }
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Set acl operation failed for key:{}/{}/{}", volume,
            bucket, keyName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volume, bucket);
    }
    return changed;
  }

  /**
   * Returns list of ACLs for given Ozone object.
   *
   * @param obj Ozone object.
   * @throws IOException if there is error.
   */
  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    validateOzoneObj(obj);
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    String keyName = obj.getKeyName();

    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volume, bucket);
    try {
      validateBucket(volume, bucket);
      String objectKey = metadataManager.getOzoneKey(volume, bucket, keyName);
      OmKeyInfo keyInfo = metadataManager.getKeyTable().get(objectKey);
      if (keyInfo == null) {
        throw new OMException("Key not found. Key:" + objectKey, KEY_NOT_FOUND);
      }

      return keyInfo.getAcls();
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Get acl operation failed for key:{}/{}/{}", volume,
            bucket, keyName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volume, bucket);
    }
  }

  /**
   * Check access for given ozoneObject.
   *
   * @param ozObject object for which access needs to be checked.
   * @param context Context object encapsulating all user related information.
   * @return true if user has access else false.
   */
  @Override
  public boolean checkAccess(OzoneObj ozObject, RequestContext context)
      throws OMException {
    Objects.requireNonNull(ozObject);
    Objects.requireNonNull(context);
    Objects.requireNonNull(context.getClientUgi());

    String volume = ozObject.getVolumeName();
    String bucket = ozObject.getBucketName();
    String keyName = ozObject.getKeyName();
    String objectKey = metadataManager.getOzoneKey(volume, bucket, keyName);
    OmKeyArgs args = new OmKeyArgs.Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName(keyName)
        .build();

    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volume, bucket);
    try {
      validateBucket(volume, bucket);
      OmKeyInfo keyInfo;

      // For Acl Type "WRITE", the key can only be found in
      // OpenKeyTable since appends to existing keys are not supported.
      if (context.getAclRights() == IAccessAuthorizer.ACLType.WRITE) {
        keyInfo = metadataManager.getOpenKeyTable().get(objectKey);
      } else {
        try {
          OzoneFileStatus fileStatus = getFileStatus(args);
          keyInfo = fileStatus.getKeyInfo();
        } catch (IOException e) {
          // OzoneFS will check whether the key exists when write a new key.
          // For Acl Type "READ", when the key is not exist return true.
          // To Avoid KEY_NOT_FOUND Exception.
          if (context.getAclRights() == IAccessAuthorizer.ACLType.READ) {
            return true;
          } else {
            throw new OMException("Key not found, checkAccess failed. Key:" +
                objectKey, KEY_NOT_FOUND);
          }
        }
      }

      if (keyInfo == null) {
        // the key does not exist, but it is a parent "dir" of some key
        // let access be determined based on volume/bucket/prefix ACL
        LOG.debug("key:{} is non-existent parent, permit access to user:{}",
            keyName, context.getClientUgi());
        return true;
      }

      boolean hasAccess = OzoneAclUtil.checkAclRight(
          keyInfo.getAcls(), context);
      if (LOG.isDebugEnabled()) {
        LOG.debug("user:{} has access rights for key:{} :{} ",
            context.getClientUgi(), ozObject.getKeyName(), hasAccess);
      }
      return hasAccess;
    } catch (IOException ex) {
      if(ex instanceof OMException) {
        throw (OMException) ex;
      }
      LOG.error("CheckAccess operation failed for key:{}/{}/{}", volume,
          bucket, keyName, ex);
      throw new OMException("Check access operation failed for " +
          "key:" + keyName, ex, INTERNAL_ERROR);
    } finally {
      metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volume, bucket);
    }
  }

  /**
   * Helper method to validate ozone object.
   * @param obj
   * */
  private void validateOzoneObj(OzoneObj obj) throws OMException {
    Objects.requireNonNull(obj);

    if (!obj.getResourceType().equals(KEY)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "KeyManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    String keyName = obj.getKeyName();

    if (Strings.isNullOrEmpty(volume)) {
      throw new OMException("Volume name is required.", VOLUME_NOT_FOUND);
    }
    if (Strings.isNullOrEmpty(bucket)) {
      throw new OMException("Bucket name is required.", BUCKET_NOT_FOUND);
    }
    if (Strings.isNullOrEmpty(keyName)) {
      throw new OMException("Key name is required.", KEY_NOT_FOUND);
    }
  }

  /**
   * OzoneFS api to get file status for an entry.
   *
   * @param args Key args
   * @throws OMException if file does not exist
   *                     if bucket does not exist
   *                     if volume does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  @Override
  public OzoneFileStatus getFileStatus(OmKeyArgs args) throws IOException {
    Preconditions.checkNotNull(args, "Key args can not be null");
    return getFileStatus(args, null);
  }

  /**
   * OzoneFS api to get file status for an entry.
   *
   * @param args Key args
   * @param clientAddress a hint to key manager, order the datanode in returned
   *                      pipeline by distance between client and datanode.
   * @throws OMException if file does not exist
   *                     if bucket does not exist
   *                     if volume does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  @Override
  public OzoneFileStatus getFileStatus(OmKeyArgs args, String clientAddress)
          throws IOException {
    Preconditions.checkNotNull(args, "Key args can not be null");
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();

    return getOzoneFileStatus(volumeName, bucketName, keyName,
            args.getRefreshPipeline(), args.getSortDatanodes(), clientAddress);
  }

  private OzoneFileStatus getOzoneFileStatus(String volumeName,
                                             String bucketName,
                                             String keyName,
                                             boolean refreshPipeline,
                                             boolean sortDatanodes,
                                             String clientAddress)
      throws IOException {
    OmKeyInfo fileKeyInfo = null;
    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volumeName,
        bucketName);
    try {
      // Check if this is the root of the filesystem.
      if (keyName.length() == 0) {
        validateBucket(volumeName, bucketName);
        return new OzoneFileStatus();
      }

      // Check if the key is a file.
      String fileKeyBytes = metadataManager.getOzoneKey(
              volumeName, bucketName, keyName);
      fileKeyInfo = metadataManager.getKeyTable().get(fileKeyBytes);

      // Check if the key is a directory.
      if (fileKeyInfo == null) {
        String dirKey = OzoneFSUtils.addTrailingSlashIfNeeded(keyName);
        String dirKeyBytes = metadataManager.getOzoneKey(
                volumeName, bucketName, dirKey);
        OmKeyInfo dirKeyInfo = metadataManager.getKeyTable().get(dirKeyBytes);
        if (dirKeyInfo != null) {
          return new OzoneFileStatus(dirKeyInfo, scmBlockSize, true);
        }
      }
    } finally {
      metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volumeName,
              bucketName);

      // if the key is a file then do refresh pipeline info in OM by asking SCM
      if (fileKeyInfo != null) {
        // refreshPipeline flag check has been removed as part of
        // https://issues.apache.org/jira/browse/HDDS-3658.
        // Please refer this jira for more details.
        refresh(fileKeyInfo);
        if (sortDatanodes) {
          sortDatanodes(clientAddress, fileKeyInfo);
        }
        return new OzoneFileStatus(fileKeyInfo, scmBlockSize, false);
      }
    }

    // Key is not found, throws exception
    if (LOG.isDebugEnabled()) {
      LOG.debug("Unable to get file status for the key: volume: {}, bucket:" +
                      " {}, key: {}, with error: No such file exists.",
              volumeName, bucketName, keyName);
    }
    throw new OMException("Unable to get file status: volume: " +
            volumeName + " bucket: " + bucketName + " key: " + keyName,
            FILE_NOT_FOUND);
  }

  /**
   * Ozone FS api to create a directory. Parent directories if do not exist
   * are created for the input directory.
   *
   * @param args Key args
   * @throws OMException if any entry in the path exists as a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  @Override
  public void createDirectory(OmKeyArgs args) throws IOException {
    Preconditions.checkNotNull(args, "Key args can not be null");
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();

    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
        bucketName);
    try {

      // Check if this is the root of the filesystem.
      if (keyName.length() == 0) {
        return;
      }

      Path keyPath = Paths.get(keyName);
      OzoneFileStatus status =
          verifyNoFilesInPath(volumeName, bucketName, keyPath, false);
      if (status != null && status.getTrimmedName()
          .equals(keyName)) {
        // if directory already exists
        return;
      }
      OmKeyInfo dirDbKeyInfo =
          createDirectoryKey(volumeName, bucketName, keyName, args.getAcls());
      String dirDbKey = metadataManager
          .getOzoneKey(volumeName, bucketName, dirDbKeyInfo.getKeyName());
      metadataManager.getKeyTable().put(dirDbKey, dirDbKeyInfo);
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
          bucketName);
    }
  }

  private OmKeyInfo createDirectoryKey(String volumeName, String bucketName,
      String keyName, List<OzoneAcl> acls) throws IOException {
    // verify bucket exists
    OmBucketInfo bucketInfo = getBucketInfo(volumeName, bucketName);

    String dir = OzoneFSUtils.addTrailingSlashIfNeeded(keyName);
    FileEncryptionInfo encInfo = getFileEncryptionInfo(bucketInfo);
    return new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(dir)
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(0)
        .setReplicationType(ReplicationType.RATIS)
        .setReplicationFactor(ReplicationFactor.ONE)
        .setFileEncryptionInfo(encInfo)
        .setAcls(acls)
        .build();
  }

  /**
   * OzoneFS api to creates an output stream for a file.
   *
   * @param args        Key args
   * @param isOverWrite if true existing file at the location will be
   *                    overwritten
   * @param isRecursive if true file would be created even if parent
   *                    directories do not exist
   * @throws OMException if given key is a directory
   *                     if file exists and isOverwrite flag is false
   *                     if an ancestor exists as a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  @Override
  public OpenKeySession createFile(OmKeyArgs args, boolean isOverWrite,
      boolean isRecursive) throws IOException {
    Preconditions.checkNotNull(args, "Key args can not be null");
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    OpenKeySession keySession;

    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
        bucketName);
    try {
      OzoneFileStatus fileStatus;
      try {
        fileStatus = getFileStatus(args);
        if (fileStatus.isDirectory()) {
          throw new OMException("Can not write to directory: " + keyName,
              ResultCodes.NOT_A_FILE);
        } else if (fileStatus.isFile()) {
          if (!isOverWrite) {
            throw new OMException("File " + keyName + " already exists",
                ResultCodes.FILE_ALREADY_EXISTS);
          }
        }
      } catch (OMException ex) {
        if (ex.getResult() != FILE_NOT_FOUND) {
          throw ex;
        }
      }

      verifyNoFilesInPath(volumeName, bucketName,
          Paths.get(keyName).getParent(), !isRecursive);
      // TODO: Optimize call to openKey as keyInfo is already available in the
      // filestatus. We can avoid some operations in openKey call.
      keySession = openKey(args);
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
          bucketName);
    }

    return keySession;
  }

  /**
   * OzoneFS api to lookup for a file.
   *
   * @param args Key args
   * @throws OMException if given key is not found or it is not a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  @Override
  public OmKeyInfo lookupFile(OmKeyArgs args, String clientAddress)
      throws IOException {
    Preconditions.checkNotNull(args, "Key args can not be null");
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    OzoneFileStatus fileStatus = getOzoneFileStatus(volumeName, bucketName,
            keyName, args.getRefreshPipeline(), args.getSortDatanodes(),
            clientAddress);
      //if key is not of type file or if key is not found we throw an exception
    if (fileStatus.isFile()) {
      // add block token for read.
      addBlockToken4Read(fileStatus.getKeyInfo());
      return fileStatus.getKeyInfo();
    }
    throw new OMException("Can not write to directory: " + keyName,
        ResultCodes.NOT_A_FILE);
  }

  /**
   * Refresh the key block location information by get latest info from SCM.
   * @param key
   */
  @Override
  public void refresh(OmKeyInfo key) throws IOException {
    Preconditions.checkNotNull(key, "Key info can not be null");
    refreshPipeline(Arrays.asList(key));
  }

  /**
   * Helper function for listStatus to find key in TableCache.
   */
  private void listStatusFindKeyInTableCache(
      Iterator<Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>>> cacheIter,
      String keyArgs, String startCacheKey, boolean recursive,
      TreeMap<String, OzoneFileStatus> cacheKeyMap, Set<String> deletedKeySet) {

    while (cacheIter.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>> entry =
          cacheIter.next();
      String cacheKey = entry.getKey().getCacheKey();
      if (cacheKey.equals(keyArgs)) {
        continue;
      }
      OmKeyInfo cacheOmKeyInfo = entry.getValue().getCacheValue();
      // cacheOmKeyInfo is null if an entry is deleted in cache
      if (cacheOmKeyInfo != null) {
        if (cacheKey.startsWith(startCacheKey) &&
            cacheKey.compareTo(startCacheKey) >= 0) {
          if (!recursive) {
            String remainingKey = StringUtils.stripEnd(cacheKey.substring(
                startCacheKey.length()), OZONE_URI_DELIMITER);
            // For non-recursive, the remaining part of key can't have '/'
            if (remainingKey.contains(OZONE_URI_DELIMITER)) {
              continue;
            }
          }
          OzoneFileStatus fileStatus = new OzoneFileStatus(
              cacheOmKeyInfo, scmBlockSize, !OzoneFSUtils.isFile(cacheKey));
          cacheKeyMap.put(cacheKey, fileStatus);
        }
      } else {
        deletedKeySet.add(cacheKey);
      }
    }
  }

  /**
   * List the status for a file or a directory and its contents.
   *
   * @param args       Key args
   * @param recursive  For a directory if true all the descendants of a
   *                   particular directory are listed
   * @param startKey   Key from which listing needs to start. If startKey exists
   *                   its status is included in the final list.
   * @param numEntries Number of entries to list from the start key
   * @return list of file status
   */
  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
                                          String startKey, long numEntries)
          throws IOException {
    return listStatus(args, recursive, startKey, numEntries, null);
  }

  /**
   * List the status for a file or a directory and its contents.
   *
   * @param args       Key args
   * @param recursive  For a directory if true all the descendants of a
   *                   particular directory are listed
   * @param startKey   Key from which listing needs to start. If startKey exists
   *                   its status is included in the final list.
   * @param numEntries Number of entries to list from the start key
   * @param clientAddress a hint to key manager, order the datanode in returned
   *                      pipeline by distance between client and datanode.
   * @return list of file status
   */
  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
      String startKey, long numEntries, String clientAddress)
          throws IOException {
    Preconditions.checkNotNull(args, "Key args can not be null");

    List<OzoneFileStatus> fileStatusList = new ArrayList<>();
    if (numEntries <= 0) {
      return fileStatusList;
    }

    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    // A map sorted by OmKey to combine results from TableCache and DB.
    TreeMap<String, OzoneFileStatus> cacheKeyMap = new TreeMap<>();
    // A set to keep track of keys deleted in cache but not flushed to DB.
    Set<String> deletedKeySet = new TreeSet<>();

    if (Strings.isNullOrEmpty(startKey)) {
      OzoneFileStatus fileStatus = getFileStatus(args, clientAddress);
      if (fileStatus.isFile()) {
        return Collections.singletonList(fileStatus);
      }
      // keyName is a directory
      startKey = OzoneFSUtils.addTrailingSlashIfNeeded(keyName);
    }

    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volumeName,
        bucketName);
    try {
      Table keyTable = metadataManager.getKeyTable();
      Iterator<Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>>>
          cacheIter = keyTable.cacheIterator();
      String startCacheKey = OZONE_URI_DELIMITER + volumeName +
          OZONE_URI_DELIMITER + bucketName + OZONE_URI_DELIMITER +
          ((startKey.equals(OZONE_URI_DELIMITER)) ? "" : startKey);
      // Note: eliminating the case where startCacheKey could end with '//'
      String keyArgs = OzoneFSUtils.addTrailingSlashIfNeeded(
          metadataManager.getOzoneKey(volumeName, bucketName, keyName));

      // First, find key in TableCache
      listStatusFindKeyInTableCache(cacheIter, keyArgs, startCacheKey,
          recursive, cacheKeyMap, deletedKeySet);
      // Then, find key in DB
      String seekKeyInDb =
          metadataManager.getOzoneKey(volumeName, bucketName, startKey);
      TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
          iterator = keyTable.iterator();
      iterator.seek(seekKeyInDb);
      int countEntries = 0;
      if (iterator.hasNext()) {
        if (iterator.key().equals(keyArgs)) {
          // Skip the key itself, since we are listing inside the directory
          iterator.next();
        }
        // Iterate through seek results
        while (iterator.hasNext() && numEntries - countEntries > 0) {
          String entryInDb = iterator.key();
          OmKeyInfo omKeyInfo = iterator.value().getValue();
          if (entryInDb.startsWith(keyArgs)) {
            String entryKeyName = omKeyInfo.getKeyName();
            if (recursive) {
              // for recursive list all the entries
              if (!deletedKeySet.contains(entryInDb)) {
                cacheKeyMap.put(entryInDb, new OzoneFileStatus(omKeyInfo,
                    scmBlockSize, !OzoneFSUtils.isFile(entryKeyName)));
                countEntries++;
              }
              iterator.next();
            } else {
              // get the child of the directory to list from the entry. For
              // example if directory to list is /a and entry is /a/b/c where
              // c is a file. The immediate child is b which is a directory. c
              // should not be listed as child of a.
              String immediateChild = OzoneFSUtils
                  .getImmediateChild(entryKeyName, keyName);
              boolean isFile = OzoneFSUtils.isFile(immediateChild);
              if (isFile) {
                if (!deletedKeySet.contains(entryInDb)) {
                  cacheKeyMap.put(entryInDb,
                      new OzoneFileStatus(omKeyInfo, scmBlockSize, !isFile));
                  countEntries++;
                }
                iterator.next();
              } else {
                // if entry is a directory
                if (!deletedKeySet.contains(entryInDb)) {
                  if (!entryKeyName.equals(immediateChild)) {
                    OmKeyInfo fakeDirEntry = createDirectoryKey(
                        omKeyInfo.getVolumeName(),
                        omKeyInfo.getBucketName(),
                        immediateChild,
                        omKeyInfo.getAcls());
                    cacheKeyMap.put(entryInDb,
                        new OzoneFileStatus(fakeDirEntry, scmBlockSize, true));
                  } else {
                    // If entryKeyName matches dir name, we have the info
                    cacheKeyMap.put(entryInDb,
                        new OzoneFileStatus(omKeyInfo, 0, true));
                  }
                  countEntries++;
                }
                // skip the other descendants of this child directory.
                iterator.seek(getNextGreaterString(
                    volumeName, bucketName, immediateChild));
              }
            }
          } else {
            break;
          }
        }
      }

      countEntries = 0;
      // Convert results in cacheKeyMap to List
      for (OzoneFileStatus fileStatus : cacheKeyMap.values()) {
        // No need to check if a key is deleted or not here, this is handled
        // when adding entries to cacheKeyMap from DB.
        fileStatusList.add(fileStatus);
        countEntries++;
        if (countEntries >= numEntries) {
          break;
        }
      }
      // Clean up temp map and set
      cacheKeyMap.clear();
      deletedKeySet.clear();
    } finally {
      metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volumeName,
          bucketName);
    }

    List<OmKeyInfo> keyInfoList = new ArrayList<>(fileStatusList.size());
    for (OzoneFileStatus fileStatus : fileStatusList) {
      keyInfoList.add(fileStatus.getKeyInfo());
    }
    refreshPipeline(keyInfoList);

    if (args.getSortDatanodes()) {
      sortDatanodes(clientAddress, keyInfoList.toArray(new OmKeyInfo[0]));
    }

    return fileStatusList;
  }

  private String getNextGreaterString(String volumeName, String bucketName,
      String keyPrefix) throws IOException {
    // Increment the last character of the string and return the new ozone key.
    Preconditions.checkArgument(!Strings.isNullOrEmpty(keyPrefix),
        "Key prefix is null or empty");
    CodecRegistry codecRegistry =
        ((RDBStore) metadataManager.getStore()).getCodecRegistry();
    byte[] keyPrefixInBytes = codecRegistry.asRawData(keyPrefix);
    keyPrefixInBytes[keyPrefixInBytes.length - 1]++;
    String nextPrefix = codecRegistry.asObject(keyPrefixInBytes, String.class);
    return metadataManager.getOzoneKey(volumeName, bucketName, nextPrefix);
  }

  /**
   * Verify that none of the parent path exists as file in the filesystem.
   *
   * @param volumeName         Volume name
   * @param bucketName         Bucket name
   * @param path               Directory path. This is the absolute path of the
   *                           directory for the ozone filesystem.
   * @param directoryMustExist throws exception if true and given path does not
   *                           exist as directory
   * @return OzoneFileStatus of the first directory found in path in reverse
   * order
   * @throws OMException if ancestor exists as file in the filesystem
   *                     if directoryMustExist flag is true and parent does
   *                     not exist
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  private OzoneFileStatus verifyNoFilesInPath(String volumeName,
      String bucketName, Path path, boolean directoryMustExist)
      throws IOException {
    OmKeyArgs.Builder argsBuilder = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName);
    while (path != null) {
      String keyName = path.toString();
      try {
        OzoneFileStatus fileStatus =
            getFileStatus(argsBuilder.setKeyName(keyName).build());
        if (fileStatus.isFile()) {
          LOG.error("Unable to create directory (File already exists): "
                  + "volume: {} bucket: {} key: {}", volumeName, bucketName,
                  keyName);
          throw new OMException(
              "Unable to create directory at : volume: " + volumeName
                  + "bucket: " + bucketName + "key: " + keyName,
              ResultCodes.FILE_ALREADY_EXISTS);
        } else if (fileStatus.isDirectory()) {
          return fileStatus;
        }
      } catch (OMException ex) {
        if (ex.getResult() != FILE_NOT_FOUND) {
          throw ex;
        } else if (ex.getResult() == FILE_NOT_FOUND) {
          if (directoryMustExist) {
            throw new OMException("Parent directory does not exist",
                ex.getCause(), DIRECTORY_NOT_FOUND);
          }
        }
      }
      path = path.getParent();
    }
    return null;
  }

  private FileEncryptionInfo getFileEncryptionInfo(OmBucketInfo bucketInfo)
      throws IOException {
    FileEncryptionInfo encInfo = null;
    BucketEncryptionKeyInfo ezInfo = bucketInfo.getEncryptionKeyInfo();
    if (ezInfo != null) {
      if (getKMSProvider() == null) {
        throw new OMException("Invalid KMS provider, check configuration " +
            HADOOP_SECURITY_KEY_PROVIDER_PATH,
            INVALID_KMS_PROVIDER);
      }

      final String ezKeyName = ezInfo.getKeyName();
      EncryptedKeyVersion edek = generateEDEK(ezKeyName);
      encInfo = new FileEncryptionInfo(ezInfo.getSuite(), ezInfo.getVersion(),
          edek.getEncryptedKeyVersion().getMaterial(),
          edek.getEncryptedKeyIv(),
          ezKeyName, edek.getEncryptionKeyVersionName());
    }
    return encInfo;
  }

  @VisibleForTesting
  void sortDatanodes(String clientMachine, OmKeyInfo... keyInfos) {
    if (keyInfos != null && clientMachine != null && !clientMachine.isEmpty()) {
      Map<Set<String>, List<DatanodeDetails>> sortedPipelines = new HashMap<>();
      for (OmKeyInfo keyInfo : keyInfos) {
        OmKeyLocationInfoGroup key = keyInfo.getLatestVersionLocations();
        if (key == null) {
          LOG.warn("No location for key {}", keyInfo);
          continue;
        }
        for (OmKeyLocationInfo k : key.getLocationList()) {
          Pipeline pipeline = k.getPipeline();
          List<DatanodeDetails> nodes = pipeline.getNodes();
          List<String> uuidList = toNodeUuid(nodes);
          Set<String> uuidSet = new HashSet<>(uuidList);
          List<DatanodeDetails> sortedNodes = sortedPipelines.get(uuidSet);
          if (sortedNodes == null) {
            if (nodes.isEmpty()) {
              LOG.warn("No datanodes in pipeline {}", pipeline.getId());
              continue;
            }
            sortedNodes = sortDatanodes(clientMachine, nodes, keyInfo,
                uuidList);
            if (sortedNodes != null) {
              sortedPipelines.put(uuidSet, sortedNodes);
            }
          } else if (LOG.isDebugEnabled()) {
            LOG.debug("Found sorted datanodes for pipeline {} and client {} "
                + "in cache", pipeline.getId(), clientMachine);
          }
          pipeline.setNodesInOrder(sortedNodes);
        }
      }
    }
  }

  private List<DatanodeDetails> sortDatanodes(String clientMachine,
      List<DatanodeDetails> nodes, OmKeyInfo keyInfo, List<String> nodeList) {
    List<DatanodeDetails> sortedNodes = null;
    try {
      sortedNodes = scmClient.getBlockClient()
          .sortDatanodes(nodeList, clientMachine);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sorted datanodes {} for client {}, result: {}", nodes,
            clientMachine, sortedNodes);
      }
    } catch (IOException e) {
      LOG.warn("Unable to sort datanodes based on distance to client, "
          + " volume={}, bucket={}, key={}, client={}, datanodes={}, "
          + " exception={}",
          keyInfo.getVolumeName(), keyInfo.getBucketName(),
          keyInfo.getKeyName(), clientMachine, nodeList, e.getMessage());
    }
    return sortedNodes;
  }

  private static List<String> toNodeUuid(Collection<DatanodeDetails> nodes) {
    List<String> nodeSet = new ArrayList<>(nodes.size());
    for (DatanodeDetails node : nodes) {
      nodeSet.add(node.getUuidString());
    }
    return nodeSet;
  }
}
