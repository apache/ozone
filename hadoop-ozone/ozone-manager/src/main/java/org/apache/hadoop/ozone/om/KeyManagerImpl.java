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
import java.time.Duration;
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
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUpload;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmPartInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.service.DirectoryDeletingService;
import org.apache.hadoop.ozone.om.service.KeyDeletingService;
import org.apache.hadoop.ozone.om.service.OpenKeyCleanupService;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKeyBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED_DEFAULT;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.READ;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_LIST_TRASH_KEYS_MAX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_LIST_TRASH_KEYS_MAX_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OzoneManagerUtils.getBucketLayout;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_KMS_PROVIDER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.SCM_GET_PIPELINE_EXCEPTION;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.util.MetricUtil.captureLatencyNs;
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
  private final int listTrashKeysMax;
  private final OzoneBlockTokenSecretManager secretManager;
  private final boolean grpcBlockTokenEnabled;

  private BackgroundService keyDeletingService;

  private final KeyProviderCryptoExtension kmsProvider;
  private final boolean enableFileSystemPaths;
  private BackgroundService dirDeletingService;
  private final OMPerformanceMetrics metrics;

  private BackgroundService openKeyCleanupService;

  @VisibleForTesting
  public KeyManagerImpl(ScmBlockLocationProtocol scmBlockClient,
      OMMetadataManager metadataManager, OzoneConfiguration conf,
      OzoneBlockTokenSecretManager secretManager,
      OMPerformanceMetrics metrics) {
    this(null, new ScmClient(scmBlockClient, null, conf), metadataManager,
        conf, secretManager, null, metrics);
  }

  @VisibleForTesting
  public KeyManagerImpl(ScmBlockLocationProtocol scmBlockClient,
      StorageContainerLocationProtocol scmContainerClient,
      OMMetadataManager metadataManager, OzoneConfiguration conf, String omId,
      OzoneBlockTokenSecretManager secretManager,
      OMPerformanceMetrics metrics) {
    this(null, new ScmClient(scmBlockClient, scmContainerClient, conf),
        metadataManager, conf, secretManager, null,
        metrics);
  }

  public KeyManagerImpl(OzoneManager om, ScmClient scmClient,
      OzoneConfiguration conf, OMPerformanceMetrics metrics) {
    this (om, scmClient, om.getMetadataManager(), conf,
        om.getBlockTokenMgr(), om.getKmsProvider(), metrics);
  }

  public KeyManagerImpl(OzoneManager om, ScmClient scmClient,
      OMMetadataManager metadataManager, OzoneConfiguration conf,
      OzoneBlockTokenSecretManager secretManager,
      KeyProviderCryptoExtension kmsProvider,
      OMPerformanceMetrics metrics) {
    this.scmBlockSize = (long) conf
        .getStorageSize(OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT,
            StorageUnit.BYTES);
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
    this.scmClient = scmClient;
    this.metadataManager = metadataManager;
    this.secretManager = secretManager;
    this.kmsProvider = kmsProvider;
    this.metrics = metrics;
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

    // Start directory deletion service for FSO buckets.
    if (dirDeletingService == null) {
      long dirDeleteInterval = configuration.getTimeDuration(
          OZONE_DIR_DELETING_SERVICE_INTERVAL,
          OZONE_DIR_DELETING_SERVICE_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);
      long serviceTimeout = configuration.getTimeDuration(
          OZONE_BLOCK_DELETING_SERVICE_TIMEOUT,
          OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT,
          TimeUnit.MILLISECONDS);
      dirDeletingService = new DirectoryDeletingService(dirDeleteInterval,
          TimeUnit.MILLISECONDS, serviceTimeout, ozoneManager, configuration);
      dirDeletingService.start();
    }

    if (openKeyCleanupService == null) {
      long serviceInterval = configuration.getTimeDuration(
          OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL,
          OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);
      long serviceTimeout = configuration.getTimeDuration(
          OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_TIMEOUT,
          OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_TIMEOUT_DEFAULT,
          TimeUnit.MILLISECONDS);
      openKeyCleanupService = new OpenKeyCleanupService(serviceInterval,
          TimeUnit.MILLISECONDS, serviceTimeout, ozoneManager, configuration);
      openKeyCleanupService.start();
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
    if (dirDeletingService != null) {
      dirDeletingService.shutdown();
      dirDeletingService = null;
    }
    if (openKeyCleanupService != null) {
      openKeyCleanupService.shutdown();
      openKeyCleanupService = null;
    }
  }

  private OmBucketInfo getBucketInfo(String volumeName, String bucketName)
      throws IOException {
    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
    return metadataManager.getBucketTable().get(bucketKey);
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
  public OmKeyInfo lookupKey(OmKeyArgs args, String clientAddress)
      throws IOException {
    Preconditions.checkNotNull(args);

    OmKeyInfo value = captureLatencyNs(metrics.getLookupReadKeyInfoLatencyNs(),
        () -> readKeyInfo(args));

    // If operation is head, do not perform any additional steps based on flags.
    // As head operation does not need any of those details.
    if (!args.isHeadOp()) {

      // add block token for read.
      captureLatencyNs(metrics.getLookupGenerateBlockTokenLatencyNs(),
          () -> addBlockToken4Read(value));

      // Refresh container pipeline info from SCM
      // based on OmKeyArgs.refreshPipeline flag
      // value won't be null as the check is done inside try/catch block.
      captureLatencyNs(metrics.getLookupRefreshLocationLatencyNs(),
          () -> refresh(value));

      if (args.getSortDatanodes()) {
        sortDatanodes(clientAddress, value);
      }
    }

    return value;
  }

  private OmKeyInfo readKeyInfo(OmKeyArgs args) throws IOException {
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volumeName,
        bucketName);

    BucketLayout bucketLayout =
        getBucketLayout(metadataManager, args.getVolumeName(),
            args.getBucketName());
    keyName = OMClientRequest
        .validateAndNormalizeKey(enableFileSystemPaths, keyName,
            bucketLayout);

    OmKeyInfo value = null;
    try {
      if (bucketLayout.isFileSystemOptimized()) {
        value = getOmKeyInfoFSO(volumeName, bucketName, keyName);
      } else {
        value = getOmKeyInfoDirectoryAware(volumeName, bucketName, keyName);
      }
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
      throw new OMException("Key:" + keyName + " not found", KEY_NOT_FOUND);
    }
    if (args.getLatestVersionLocation()) {
      slimLocationVersion(value);
    }
    return value;
  }

  private OmKeyInfo getOmKeyInfoDirectoryAware(String volumeName,
            String bucketName, String keyName) throws IOException {
    OmKeyInfo keyInfo = getOmKeyInfo(volumeName, bucketName, keyName);

    // Check if the key is a directory.
    if (keyInfo != null) {
      keyInfo.setFile(true);
      return keyInfo;
    }

    String dirKey = OzoneFSUtils.addTrailingSlashIfNeeded(keyName);
    OmKeyInfo dirKeyInfo = getOmKeyInfo(volumeName, bucketName, dirKey);
    if (dirKeyInfo != null) {
      dirKeyInfo.setFile(false);
    }
    return dirKeyInfo;
  }

  private OmKeyInfo getOmKeyInfo(String volumeName, String bucketName,
                                 String keyName) throws IOException {
    String keyBytes =
        metadataManager.getOzoneKey(volumeName, bucketName, keyName);
    BucketLayout bucketLayout = getBucketLayout(metadataManager, volumeName,
        bucketName);
    return metadataManager
        .getKeyTable(bucketLayout)
        .get(keyBytes);
  }

  /**
   * Look up will return only closed fileInfo. This will return null if the
   * keyName is a directory or if the keyName is still open for writing.
   */
  private OmKeyInfo getOmKeyInfoFSO(String volumeName, String bucketName,
                                   String keyName) throws IOException {
    OzoneFileStatus fileStatus =
            OMFileRequest.getOMKeyInfoIfExists(metadataManager,
                    volumeName, bucketName, keyName, scmBlockSize);
    if (fileStatus == null) {
      return null;
    }
    // Appended trailing slash to represent directory to the user
    if (fileStatus.isDirectory()) {
      String keyPath = OzoneFSUtils.addTrailingSlashIfNeeded(
          fileStatus.getKeyInfo().getKeyName());
      fileStatus.getKeyInfo().setKeyName(keyPath);
    }
    fileStatus.getKeyInfo().setFile(fileStatus.isFile());
    return fileStatus.getKeyInfo();
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
        for (List<OmKeyLocationInfo> omKeyLocationInfoList :
            key.getLocationLists()) {
          for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfoList) {
            containerIDs.add(omKeyLocationInfo.getContainerID());
          }
        }
      }
    }

    Map<Long, ContainerWithPipeline> containerWithPipelineMap =
        refreshPipeline(containerIDs);

    for (OmKeyInfo keyInfo : keyList) {
      List<OmKeyLocationInfoGroup> locationInfoGroups =
          keyInfo.getKeyLocationVersions();
      for (OmKeyLocationInfoGroup key : locationInfoGroups) {
        for (List<OmKeyLocationInfo> omKeyLocationInfoList :
            key.getLocationLists()) {
          for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfoList) {
            ContainerWithPipeline cp = containerWithPipelineMap.get(
                omKeyLocationInfo.getContainerID());
            if (cp != null &&
                !cp.getPipeline().equals(omKeyLocationInfo.getPipeline())) {
              omKeyLocationInfo.setPipeline(cp.getPipeline());
            }
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
          containerIDs, ioEx);
      throw new OMException(ioEx.getMessage(), SCM_GET_PIPELINE_EXCEPTION);
    }
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

    // For listKeys, we return the latest Key Location by default
    for (OmKeyInfo omKeyInfo : keyList) {
      slimLocationVersion(omKeyInfo);
    }

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
  public List<OpenKeyBucket> getExpiredOpenKeys(Duration expireThreshold,
      int count, BucketLayout bucketLayout) throws IOException {
    return metadataManager.getExpiredOpenKeys(expireThreshold, count,
        bucketLayout);
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
  public BackgroundService getDirDeletingService() {
    return dirDeletingService;
  }

  public BackgroundService getOpenKeyCleanupService() {
    return openKeyCleanupService;
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
              upload.setReplicationConfig(
                      multipartKeyInfo.getReplicationConfig());
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
    BucketLayout bucketLayout = BucketLayout.DEFAULT;
    if (ozoneManager != null) {
      String buckKey = ozoneManager.getMetadataManager()
          .getBucketKey(volumeName, bucketName);
      OmBucketInfo buckInfo =
          ozoneManager.getMetadataManager().getBucketTable().get(buckKey);
      bucketLayout = buckInfo.getBucketLayout();
    }

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

        ReplicationConfig replicationConfig = null;

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
            String partName = getPartName(partKeyInfo, volumeName, bucketName,
                keyName);
            OmPartInfo omPartInfo = new OmPartInfo(partKeyInfo.getPartNumber(),
                partName,
                partKeyInfo.getPartKeyInfo().getModificationTime(),
                partKeyInfo.getPartKeyInfo().getDataSize());
            omPartInfoList.add(omPartInfo);

            //if there are parts, use replication type from one of the parts
            replicationConfig = ReplicationConfig.fromProto(
                partKeyInfo.getPartKeyInfo().getType(),
                partKeyInfo.getPartKeyInfo().getFactor(),
                partKeyInfo.getPartKeyInfo().getEcReplicationConfig());
            count++;
          }
        }

        if (replicationConfig == null) {
          //if there are no parts, use the replicationType from the open key.
          if (isBucketFSOptimized(volumeName, bucketName)) {
            multipartKey =
                getMultipartOpenKeyFSO(volumeName, bucketName, keyName,
                    uploadID);
          }
          OmKeyInfo omKeyInfo =
              metadataManager.getOpenKeyTable(bucketLayout)
                  .get(multipartKey);

          if (omKeyInfo == null) {
            throw new IllegalStateException(
                "Open key is missing for multipart upload " + multipartKey);
          }

          replicationConfig = omKeyInfo.getReplicationConfig();
        }
        Preconditions.checkNotNull(replicationConfig,
            "ReplicationConfig can't be identified");

        if (partKeyInfoMapIterator.hasNext()) {
          isTruncated = true;
        } else {
          isTruncated = false;
          nextPartNumberMarker = 0;
        }
        OmMultipartUploadListParts omMultipartUploadListParts =
            new OmMultipartUploadListParts(replicationConfig,
                nextPartNumberMarker, isTruncated);
        omMultipartUploadListParts.addPartList(omPartInfoList);
        return omMultipartUploadListParts;
      }
    } catch (OMException ex) {
      throw ex;
    } catch (IOException ex) {
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

  private String getPartName(PartKeyInfo partKeyInfo, String volName,
                             String buckName, String keyName)
      throws IOException {

    String partName = partKeyInfo.getPartName();

    if (isBucketFSOptimized(volName, buckName)) {
      String parentDir = OzoneFSUtils.getParentDir(keyName);
      String partFileName = OzoneFSUtils.getFileName(partKeyInfo.getPartName());

      StringBuilder fullKeyPartName = new StringBuilder();
      fullKeyPartName.append(OZONE_URI_DELIMITER);
      fullKeyPartName.append(volName);
      fullKeyPartName.append(OZONE_URI_DELIMITER);
      fullKeyPartName.append(buckName);
      if (StringUtils.isNotEmpty(parentDir)) {
        fullKeyPartName.append(OZONE_URI_DELIMITER);
        fullKeyPartName.append(parentDir);
      }
      fullKeyPartName.append(OZONE_URI_DELIMITER);
      fullKeyPartName.append(partFileName);

      return fullKeyPartName.toString();
    }
    return partName;
  }

  private String getMultipartOpenKeyFSO(String volumeName, String bucketName,
      String keyName, String uploadID) throws IOException {
    OMMetadataManager metaMgr = ozoneManager.getMetadataManager();
    String fileName = OzoneFSUtils.getFileName(keyName);
    Iterator<Path> pathComponents = Paths.get(keyName).iterator();
    final long volumeId = metaMgr.getVolumeId(volumeName);
    final long bucketId = metaMgr.getBucketId(volumeName, bucketName);
    long parentID =
        OMFileRequest.getParentID(volumeId, bucketId, pathComponents,
                keyName, metaMgr);

    String multipartKey = metaMgr.getMultipartKey(volumeId, bucketId,
            parentID, fileName, uploadID);

    return multipartKey;
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
      OMFileRequest.validateBucket(metadataManager, volume, bucket);
      String objectKey = metadataManager.getOzoneKey(volume, bucket, keyName);
      BucketLayout bucketLayout =
          getBucketLayout(metadataManager, volume, bucket);
      OmKeyInfo keyInfo = metadataManager
          .getKeyTable(bucketLayout)
          .get(objectKey);
      if (keyInfo == null) {
        throw new OMException("Key not found. Key:" + objectKey, KEY_NOT_FOUND);
      }

      if (keyInfo.getAcls() == null) {
        keyInfo.setAcls(new ArrayList<>());
      }
      changed = keyInfo.addAcl(acl);
      if (changed) {
        metadataManager
            .getKeyTable(getBucketLayout(metadataManager, volume, bucket))
            .put(objectKey, keyInfo);
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
      OMFileRequest.validateBucket(metadataManager, volume, bucket);
      String objectKey = metadataManager.getOzoneKey(volume, bucket, keyName);
      BucketLayout bucketLayout =
          getBucketLayout(metadataManager, volume, bucket);
      OmKeyInfo keyInfo = metadataManager
          .getKeyTable(bucketLayout)
          .get(objectKey);
      if (keyInfo == null) {
        throw new OMException("Key not found. Key:" + objectKey, KEY_NOT_FOUND);
      }

      changed = keyInfo.removeAcl(acl);
      if (changed) {
        metadataManager
            .getKeyTable(getBucketLayout(metadataManager, volume, bucket))
            .put(objectKey, keyInfo);
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
      OMFileRequest.validateBucket(metadataManager, volume, bucket);
      String objectKey = metadataManager.getOzoneKey(volume, bucket, keyName);
      BucketLayout bucketLayout =
          getBucketLayout(metadataManager, volume, bucket);
      OmKeyInfo keyInfo = metadataManager
          .getKeyTable(bucketLayout)
          .get(objectKey);
      if (keyInfo == null) {
        throw new OMException("Key not found. Key:" + objectKey, KEY_NOT_FOUND);
      }

      changed = keyInfo.setAcls(acls);

      if (changed) {
        metadataManager
            .getKeyTable(getBucketLayout(metadataManager, volume, bucket))
            .put(objectKey, keyInfo);
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
    OmKeyInfo keyInfo;
    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volume, bucket);
    try {
      OMFileRequest.validateBucket(metadataManager, volume, bucket);
      String objectKey = metadataManager.getOzoneKey(volume, bucket, keyName);
      if (isBucketFSOptimized(volume, bucket)) {
        keyInfo = getOmKeyInfoFSO(volume, bucket, keyName);
      } else {
        keyInfo = getOmKeyInfo(volume, bucket, keyName);
      }
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
        .setHeadOp(true)
        .build();

    BucketLayout bucketLayout = BucketLayout.DEFAULT;
    if (ozoneManager != null) {
      String buckKey =
          ozoneManager.getMetadataManager().getBucketKey(volume, bucket);
      OmBucketInfo buckInfo = null;
      try {
        buckInfo =
            ozoneManager.getMetadataManager().getBucketTable().get(buckKey);
        bucketLayout = buckInfo.getBucketLayout();
      } catch (IOException e) {
        LOG.error("Failed to get bucket for the key: " + buckKey, e);
      }
    }

    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volume, bucket);
    try {
      OMFileRequest.validateBucket(metadataManager, volume, bucket);
      OmKeyInfo keyInfo;

      // For Acl Type "WRITE", the key can only be found in
      // OpenKeyTable since appends to existing keys are not supported.
      if (context.getAclRights() == IAccessAuthorizer.ACLType.WRITE) {
        keyInfo =
            metadataManager.getOpenKeyTable(bucketLayout).get(objectKey);
      } else {
        // Recursive check is done only for ACL_TYPE DELETE
        // Rename and delete operations will send ACL_TYPE DELETE
        if (context.isRecursiveAccessCheck()
            && context.getAclRights() == IAccessAuthorizer.ACLType.DELETE) {
          return checkChildrenAcls(ozObject, context);
        }
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
            throw new OMException(
                "Key not found, checkAccess failed. Key:" + objectKey,
                KEY_NOT_FOUND);
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

      boolean hasAccess = OzoneAclUtil.checkAclRights(
          keyInfo.getAcls(), context);
      if (LOG.isDebugEnabled()) {
        LOG.debug("user:{} has access rights for key:{} :{} ",
            context.getClientUgi(), ozObject.getKeyName(), hasAccess);
      }
      return hasAccess;
    } catch (IOException ex) {
      if (ex instanceof OMException) {
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
   * check acls for all subpaths of a directory.
   *
   * @param ozObject
   * @param context
   * @return
   * @throws IOException
   */
  private boolean checkChildrenAcls(OzoneObj ozObject, RequestContext context)
      throws IOException {
    OmKeyInfo keyInfo;
    OzoneFileStatus ozoneFileStatus =
        ozObject.getOzonePrefixPathViewer().getOzoneFileStatus();
    keyInfo = ozoneFileStatus.getKeyInfo();
    // Using stack to check acls for subpaths
    Stack<OzoneFileStatus> directories = new Stack<>();
    // check whether given file/dir  has access
    boolean hasAccess = OzoneAclUtil.checkAclRights(keyInfo.getAcls(), context);
    if (LOG.isDebugEnabled()) {
      LOG.debug("user:{} has access rights for key:{} :{} ",
          context.getClientUgi(), ozObject.getKeyName(), hasAccess);
    }
    if (ozoneFileStatus.isDirectory() && hasAccess) {
      directories.add(ozoneFileStatus);
    }
    while (!directories.isEmpty() && hasAccess) {
      ozoneFileStatus = directories.pop();
      String keyPath = ozoneFileStatus.getTrimmedName();
      Iterator<? extends OzoneFileStatus> children =
          ozObject.getOzonePrefixPathViewer().getChildren(keyPath);
      while (hasAccess && children.hasNext()) {
        ozoneFileStatus = children.next();
        keyInfo = ozoneFileStatus.getKeyInfo();
        hasAccess = OzoneAclUtil.checkAclRights(keyInfo.getAcls(), context);
        if (LOG.isDebugEnabled()) {
          LOG.debug("user:{} has access rights for key:{} :{} ",
              context.getClientUgi(), keyInfo.getKeyName(), hasAccess);
        }
        if (hasAccess && ozoneFileStatus.isDirectory()) {
          directories.add(ozoneFileStatus);
        }
      }
    }
    return hasAccess;
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

    if (isBucketFSOptimized(volumeName, bucketName)) {
      return getOzoneFileStatusFSO(args, clientAddress, false);
    }
    return getOzoneFileStatus(args, clientAddress);
  }

  private OzoneFileStatus getOzoneFileStatus(OmKeyArgs args,
      String clientAddress) throws IOException {

    Preconditions.checkNotNull(args, "Key args can not be null");
    final String volumeName = args.getVolumeName();
    final String bucketName = args.getBucketName();
    final String keyName = args.getKeyName();

    OmKeyInfo fileKeyInfo = null;
    OmKeyInfo dirKeyInfo = null;
    OmKeyInfo fakeDirKeyInfo = null;
    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volumeName,
        bucketName);
    try {
      // Check if this is the root of the filesystem.
      if (keyName.length() == 0) {
        OMFileRequest.validateBucket(metadataManager, volumeName, bucketName);
        return new OzoneFileStatus();
      }

      // Check if the key is a file.
      String fileKeyBytes = metadataManager.getOzoneKey(
              volumeName, bucketName, keyName);
      BucketLayout layout =
          getBucketLayout(metadataManager, volumeName, bucketName);
      fileKeyInfo = metadataManager.getKeyTable(layout).get(fileKeyBytes);
      String dirKey = OzoneFSUtils.addTrailingSlashIfNeeded(keyName);

      // Check if the key is a directory.
      if (fileKeyInfo == null) {
        String dirKeyBytes = metadataManager.getOzoneKey(
                volumeName, bucketName, dirKey);
        dirKeyInfo = metadataManager.getKeyTable(layout).get(dirKeyBytes);
        if (dirKeyInfo == null) {
          fakeDirKeyInfo =
              createFakeDirIfShould(volumeName, bucketName, keyName, layout);
        }
      }
    } finally {
      metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volumeName,
              bucketName);
      if (fileKeyInfo != null) {
        // if the key is a file
        // then do refresh pipeline info in OM by asking SCM
        if (args.getLatestVersionLocation()) {
          slimLocationVersion(fileKeyInfo);
        }
        // If operation is head, do not perform any additional steps
        // As head operation does not need any of those details.
        if (!args.isHeadOp()) {
          // refreshPipeline flag check has been removed as part of
          // https://issues.apache.org/jira/browse/HDDS-3658.
          // Please refer this jira for more details.
          refresh(fileKeyInfo);
          if (args.getSortDatanodes()) {
            sortDatanodes(clientAddress, fileKeyInfo);
          }
        }
      }
    }

    if (fileKeyInfo != null) {
      return new OzoneFileStatus(fileKeyInfo, scmBlockSize, false);
    }

    if (dirKeyInfo != null) {
      return new OzoneFileStatus(dirKeyInfo, scmBlockSize, true);
    }

    if (fakeDirKeyInfo != null) {
      return new OzoneFileStatus(fakeDirKeyInfo, scmBlockSize, true);
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
   * Create a fake directory if the key is a path prefix,
   * otherwise returns null.
   * Some keys may contain '/' Ozone will treat '/' as directory separator
   * such as : key name is 'a/b/c', 'a' and 'b' may not really exist,
   * but Ozone treats 'a' and 'b' as a directory.
   * we need create a fake directory 'a' or 'a/b'
   *
   * @return OmKeyInfo if the key is a path prefix, otherwise returns null.
   */
  private OmKeyInfo createFakeDirIfShould(String volume, String bucket,
      String keyName, BucketLayout layout) throws IOException {
    OmKeyInfo fakeDirKeyInfo = null;
    String dirKey = OzoneFSUtils.addTrailingSlashIfNeeded(keyName);
    String fileKeyBytes = metadataManager.getOzoneKey(volume, bucket, keyName);
    Table.KeyValue<String, OmKeyInfo> keyValue =
            metadataManager.getKeyTable(layout).iterator()
                .seek(OzoneFSUtils.addTrailingSlashIfNeeded(fileKeyBytes));

    if (keyValue != null) {
      Path fullPath = Paths.get(keyValue.getValue().getKeyName());
      Path subPath = Paths.get(dirKey);
      OmKeyInfo omKeyInfo = keyValue.getValue();
      if (fullPath.startsWith(subPath)) {
        // create fake directory
        fakeDirKeyInfo = createDirectoryKey(omKeyInfo, dirKey);
      }
    }

    return fakeDirKeyInfo;
  }


  private OzoneFileStatus getOzoneFileStatusFSO(OmKeyArgs args,
      String clientAddress, boolean skipFileNotFoundError) throws IOException {
    final String volumeName = args.getVolumeName();
    final String bucketName = args.getBucketName();
    final String keyName = args.getKeyName();
    OzoneFileStatus fileStatus = null;
    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volumeName,
            bucketName);
    try {
      // Check if this is the root of the filesystem.
      if (keyName.length() == 0) {
        OMFileRequest.validateBucket(metadataManager, volumeName, bucketName);
        return new OzoneFileStatus();
      }

      fileStatus = OMFileRequest.getOMKeyInfoIfExists(metadataManager,
              volumeName, bucketName, keyName, scmBlockSize);

    } finally {
      metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volumeName,
              bucketName);
    }

    if (fileStatus != null) {
      // if the key is a file then do refresh pipeline info in OM by asking SCM
      if (fileStatus.isFile()) {
        OmKeyInfo fileKeyInfo = fileStatus.getKeyInfo();
        if (args.getLatestVersionLocation()) {
          slimLocationVersion(fileKeyInfo);
        }

        if (!args.isHeadOp()) {
          // refreshPipeline flag check has been removed as part of
          // https://issues.apache.org/jira/browse/HDDS-3658.
          // Please refer this jira for more details.
          refresh(fileKeyInfo);

          if (args.getSortDatanodes()) {
            sortDatanodes(clientAddress, fileKeyInfo);
          }
        }
        return new OzoneFileStatus(fileKeyInfo, scmBlockSize, false);
      } else {
        return fileStatus;
      }
    }

    // Key not found.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Unable to get file status for the key: volume: {}, bucket:" +
                      " {}, key: {}, with error: No such file exists.",
              volumeName, bucketName, keyName);
    }

    // don't throw exception if this flag is true.
    if (skipFileNotFoundError) {
      return fileStatus;
    }

    throw new OMException("Unable to get file status: volume: " +
            volumeName + " bucket: " + bucketName + " key: " + keyName,
            FILE_NOT_FOUND);
  }

  private OmKeyInfo createDirectoryKey(OmKeyInfo keyInfo, String keyName)
          throws IOException {
    // verify bucket exists
    OmBucketInfo bucketInfo = getBucketInfo(keyInfo.getVolumeName(),
            keyInfo.getBucketName());

    String dir = OzoneFSUtils.addTrailingSlashIfNeeded(keyName);
    FileEncryptionInfo encInfo = getFileEncryptionInfo(bucketInfo);
    return new OmKeyInfo.Builder()
        .setVolumeName(keyInfo.getVolumeName())
        .setBucketName(keyInfo.getBucketName())
        .setKeyName(dir)
        .setFileName(OzoneFSUtils.getFileName(keyName))
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(0)
        .setReplicationConfig(keyInfo.getReplicationConfig())
        .setFileEncryptionInfo(encInfo)
        .setAcls(keyInfo.getAcls())
        .build();
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
    OzoneFileStatus fileStatus;
    if (isBucketFSOptimized(volumeName, bucketName)) {
      fileStatus = getOzoneFileStatusFSO(args, clientAddress, false);
    } else {
      fileStatus = getOzoneFileStatus(args, clientAddress);
    }
    //if key is not of type file or if key is not found we throw an exception
    if (fileStatus.isFile()) {
      // add block token for read.
      if (!args.isHeadOp()) {
        addBlockToken4Read(fileStatus.getKeyInfo());
      }
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

  public static boolean isKeyDeleted(String key, Table keyTable) {
    CacheValue<OmKeyInfo> omKeyInfoCacheValue
        = keyTable.getCacheValue(new CacheKey(key));
    return omKeyInfoCacheValue != null
        && omKeyInfoCacheValue.getCacheValue() == null;
  }

  /**
   * Helper function for listStatus to find key in TableCache.
   */
  private void listStatusFindKeyInTableCache(
      Iterator<Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>>> cacheIter,
      String keyArgs, String startCacheKey, boolean recursive,
      TreeMap<String, OzoneFileStatus> cacheKeyMap) {

    while (cacheIter.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>> entry =
          cacheIter.next();
      String cacheKey = entry.getKey().getCacheKey();
      if (cacheKey.equals(keyArgs)) {
        continue;
      }
      OmKeyInfo cacheOmKeyInfo = entry.getValue().getCacheValue();
      // cacheOmKeyInfo is null if an entry is deleted in cache
      if (cacheOmKeyInfo != null
          && cacheKey.startsWith(startCacheKey)
          && cacheKey.compareTo(startCacheKey) >= 0) {
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

  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
                                          String startKey, long numEntries,
                                          String clientAddress)
      throws IOException {
    return listStatus(args, recursive, startKey, numEntries,
        clientAddress, false);
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
  @SuppressWarnings("methodlength")
  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
      String startKey, long numEntries, String clientAddress,
      boolean allowPartialPrefixes) throws IOException {
    Preconditions.checkNotNull(args, "Key args can not be null");
    String volName = args.getVolumeName();
    String buckName = args.getBucketName();
    List<OzoneFileStatus> fileStatusList = new ArrayList<>();
    if (numEntries <= 0) {
      return fileStatusList;
    }

    if (isBucketFSOptimized(volName, buckName)) {
      Preconditions.checkArgument(!recursive);
      OzoneListStatusHelper statusHelper =
          new OzoneListStatusHelper(metadataManager, scmBlockSize,
              this::getOzoneFileStatusFSO);
      Collection<OzoneFileStatus> statuses =
          statusHelper.listStatusFSO(args, startKey, numEntries,
          clientAddress, allowPartialPrefixes);
      return buildFinalStatusList(statuses, args, clientAddress);
    }

    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    // A map sorted by OmKey to combine results from TableCache and DB.
    TreeMap<String, OzoneFileStatus> cacheKeyMap = new TreeMap<>();

    if (Strings.isNullOrEmpty(startKey)) {
      OzoneFileStatus fileStatus = getFileStatus(args, clientAddress);
      if (fileStatus.isFile()) {
        return Collections.singletonList(fileStatus);
      }
      // keyName is a directory
      startKey = OzoneFSUtils.addTrailingSlashIfNeeded(keyName);
    }

    // Note: eliminating the case where startCacheKey could end with '//'
    String keyArgs = OzoneFSUtils.addTrailingSlashIfNeeded(
        metadataManager.getOzoneKey(volumeName, bucketName, keyName));

    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volumeName,
        bucketName);
    Table<String, OmKeyInfo> keyTable = metadataManager
        .getKeyTable(getBucketLayout(metadataManager, volName, buckName));
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        iterator = getIteratorForKeyInTableCache(recursive, startKey,
        volumeName, bucketName, cacheKeyMap, keyArgs, keyTable)) {
      findKeyInDbWithIterator(recursive, startKey, numEntries, volumeName,
          bucketName, keyName, cacheKeyMap, keyArgs, keyTable, iterator);
    }
    int countEntries;

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

    List<OmKeyInfo> keyInfoList = new ArrayList<>(fileStatusList.size());
    fileStatusList.stream().map(s -> s.getKeyInfo()).forEach(keyInfoList::add);
    if (args.getLatestVersionLocation()) {
      slimLocationVersion(keyInfoList.toArray(new OmKeyInfo[0]));
    }
    refreshPipeline(keyInfoList);

    if (args.getSortDatanodes()) {
      sortDatanodes(clientAddress, keyInfoList.toArray(new OmKeyInfo[0]));
    }
    return fileStatusList;
  }

  private TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
      getIteratorForKeyInTableCache(
      boolean recursive, String startKey, String volumeName, String bucketName,
      TreeMap<String, OzoneFileStatus> cacheKeyMap, String keyArgs,
      Table<String, OmKeyInfo> keyTable) throws IOException {
    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> iterator;
    try {
      Iterator<Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>>>
          cacheIter = keyTable.cacheIterator();
      String startCacheKey = OZONE_URI_DELIMITER + volumeName +
          OZONE_URI_DELIMITER + bucketName + OZONE_URI_DELIMITER +
          ((startKey.equals(OZONE_URI_DELIMITER)) ? "" : startKey);

      // First, find key in TableCache
      listStatusFindKeyInTableCache(cacheIter, keyArgs, startCacheKey,
          recursive, cacheKeyMap);
      iterator = keyTable.iterator();
    } finally {
      metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volumeName,
          bucketName);
    }
    return iterator;
  }

  @SuppressWarnings("parameternumber")
  private void findKeyInDbWithIterator(boolean recursive, String startKey,
      long numEntries, String volumeName, String bucketName, String keyName,
      TreeMap<String, OzoneFileStatus> cacheKeyMap, String keyArgs,
      Table<String, OmKeyInfo> keyTable,
      TableIterator<String,
          ? extends Table.KeyValue<String, OmKeyInfo>> iterator)
      throws IOException {
    // Then, find key in DB
    String seekKeyInDb =
        metadataManager.getOzoneKey(volumeName, bucketName, startKey);
    Table.KeyValue<String, OmKeyInfo> entry = iterator.seek(seekKeyInDb);
    int countEntries = 0;
    if (iterator.hasNext()) {
      if (entry.getKey().equals(keyArgs)) {
        // Skip the key itself, since we are listing inside the directory
        iterator.next();
      }
      // Iterate through seek results
      while (iterator.hasNext() && numEntries - countEntries > 0) {
        entry = iterator.next();
        String entryInDb = entry.getKey();
        OmKeyInfo omKeyInfo = entry.getValue();
        if (entryInDb.startsWith(keyArgs)) {
          String entryKeyName = omKeyInfo.getKeyName();
          if (recursive) {
            // for recursive list all the entries

            if (!isKeyDeleted(entryInDb, keyTable)) {
              cacheKeyMap.put(entryInDb, new OzoneFileStatus(omKeyInfo,
                  scmBlockSize, !OzoneFSUtils.isFile(entryKeyName)));
              countEntries++;
            }
          } else {
            // get the child of the directory to list from the entry. For
            // example if directory to list is /a and entry is /a/b/c where
            // c is a file. The immediate child is b which is a directory. c
            // should not be listed as child of a.
            String immediateChild = OzoneFSUtils
                .getImmediateChild(entryKeyName, keyName);
            boolean isFile = OzoneFSUtils.isFile(immediateChild);
            if (isFile) {
              if (!isKeyDeleted(entryInDb, keyTable)) {
                cacheKeyMap.put(entryInDb,
                    new OzoneFileStatus(omKeyInfo, scmBlockSize, !isFile));
                countEntries++;
              }
            } else {
              // if entry is a directory
              if (!isKeyDeleted(entryInDb, keyTable)) {
                if (!entryKeyName.equals(immediateChild)) {
                  OmKeyInfo fakeDirEntry = createDirectoryKey(
                      omKeyInfo, immediateChild);
                  cacheKeyMap.put(entryInDb,
                      new OzoneFileStatus(fakeDirEntry,
                          scmBlockSize, true));
                } else {
                  // If entryKeyName matches dir name, we have the info
                  cacheKeyMap.put(entryInDb,
                      new OzoneFileStatus(omKeyInfo, 0, true));
                }
                countEntries++;
              }
              // skip the other descendants of this child directory.
              iterator.seek(getNextGreaterString(volumeName, bucketName,
                  immediateChild));
            }
          }
        } else {
          break;
        }
      }
    }
  }

  private List<OzoneFileStatus> buildFinalStatusList(
      Collection<OzoneFileStatus> statusesCollection, OmKeyArgs omKeyArgs,
      String clientAddress)
      throws IOException {
    List<OzoneFileStatus> fileStatusFinalList = new ArrayList<>();
    List<OmKeyInfo> keyInfoList = new ArrayList<>();

    for (OzoneFileStatus fileStatus : statusesCollection) {
      if (fileStatus.isFile()) {
        keyInfoList.add(fileStatus.getKeyInfo());
      }
      fileStatusFinalList.add(fileStatus);
    }

    return sortPipelineInfo(fileStatusFinalList, keyInfoList,
        omKeyArgs, clientAddress);
  }


  private List<OzoneFileStatus> sortPipelineInfo(
      List<OzoneFileStatus> fileStatusFinalList, List<OmKeyInfo> keyInfoList,
      OmKeyArgs omKeyArgs, String clientAddress) throws IOException {


    if (omKeyArgs.getLatestVersionLocation()) {
      slimLocationVersion(keyInfoList.toArray(new OmKeyInfo[0]));
    }
    // refreshPipeline flag check has been removed as part of
    // https://issues.apache.org/jira/browse/HDDS-3658.
    // Please refer this jira for more details.
    refreshPipeline(keyInfoList);

    if (omKeyArgs.getSortDatanodes()) {
      sortDatanodes(clientAddress, keyInfoList.toArray(new OmKeyInfo[0]));
    }

    return fileStatusFinalList;
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
  private void slimLocationVersion(OmKeyInfo... keyInfos) {
    if (keyInfos != null) {
      for (OmKeyInfo keyInfo : keyInfos) {
        OmKeyLocationInfoGroup key = keyInfo.getLatestVersionLocations();
        if (key == null) {
          LOG.warn("No location version for key {}", keyInfo);
          continue;
        }
        int keyLocationVersionLength = keyInfo.getKeyLocationVersions().size();
        if (keyLocationVersionLength <= 1) {
          continue;
        }
        keyInfo.setKeyLocationVersions(keyInfo.getKeyLocationVersions()
            .subList(keyLocationVersionLength - 1, keyLocationVersionLength));
      }
    }
  }

  @Override
  public Table.KeyValue<String, OmKeyInfo> getPendingDeletionDir()
          throws IOException {
    // TODO: Make the return type as OmDirectoryInfo after adding
    //  volumeId and bucketId to OmDirectoryInfo
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
             deletedDirItr = metadataManager.getDeletedDirTable().iterator()) {
      if (deletedDirItr.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> keyValue = deletedDirItr.next();
        if (keyValue != null) {
          return keyValue;
        }
      }
    }
    return null;
  }

  @Override
  public List<OmKeyInfo> getPendingDeletionSubDirs(long volumeId, long bucketId,
      OmKeyInfo parentInfo, long numEntries) throws IOException {
    String seekDirInDB = metadataManager.getOzonePathKey(volumeId, bucketId,
        parentInfo.getObjectID(), "");
    long countEntries = 0;

    Table dirTable = metadataManager.getDirectoryTable();
    try (TableIterator<String,
        ? extends Table.KeyValue<String, OmDirectoryInfo>>
        iterator = dirTable.iterator()) {
      return gatherSubDirsWithIterator(parentInfo, numEntries,
          seekDirInDB, countEntries, iterator);
    }

  }

  private List<OmKeyInfo> gatherSubDirsWithIterator(OmKeyInfo parentInfo,
      long numEntries, String seekDirInDB,
      long countEntries,
      TableIterator<String,
          ? extends Table.KeyValue<String, OmDirectoryInfo>> iterator)
      throws IOException {
    List<OmKeyInfo> directories = new ArrayList<>();
    iterator.seek(seekDirInDB);

    while (iterator.hasNext() && numEntries - countEntries > 0) {
      Table.KeyValue<String, OmDirectoryInfo> entry = iterator.next();
      OmDirectoryInfo dirInfo = entry.getValue();
      if (!OMFileRequest.isImmediateChild(dirInfo.getParentObjectID(),
          parentInfo.getObjectID())) {
        break;
      }
      String dirName = OMFileRequest.getAbsolutePath(parentInfo.getKeyName(),
          dirInfo.getName());
      OmKeyInfo omKeyInfo = OMFileRequest.getOmKeyInfo(
          parentInfo.getVolumeName(), parentInfo.getBucketName(), dirInfo,
          dirName);
      directories.add(omKeyInfo);
      countEntries++;
    }

    return directories;
  }

  @Override
  public List<OmKeyInfo> getPendingDeletionSubFiles(long volumeId,
      long bucketId, OmKeyInfo parentInfo, long numEntries)
          throws IOException {
    List<OmKeyInfo> files = new ArrayList<>();
    String seekFileInDB = metadataManager.getOzonePathKey(volumeId, bucketId,
        parentInfo.getObjectID(), "");
    long countEntries = 0;

    Table fileTable = metadataManager.getFileTable();
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        iterator = fileTable.iterator()) {

      iterator.seek(seekFileInDB);

      while (iterator.hasNext() && numEntries - countEntries > 0) {
        Table.KeyValue<String, OmKeyInfo> entry = iterator.next();
        OmKeyInfo fileInfo = entry.getValue();
        if (!OMFileRequest.isImmediateChild(fileInfo.getParentObjectID(),
            parentInfo.getObjectID())) {
          break;
        }
        fileInfo.setFileName(fileInfo.getKeyName());
        String fullKeyPath = OMFileRequest.getAbsolutePath(
            parentInfo.getKeyName(), fileInfo.getKeyName());
        fileInfo.setKeyName(fullKeyPath);

        files.add(fileInfo);
        countEntries++;
      }
    }

    return files;
  }

  public boolean isBucketFSOptimized(String volName, String buckName)
      throws IOException {
    // This will never be null in reality but can be null in unit test cases.
    // Added safer check for unit testcases.
    if (ozoneManager == null) {
      return false;
    }
    String buckKey =
        ozoneManager.getMetadataManager().getBucketKey(volName, buckName);
    OmBucketInfo buckInfo =
        ozoneManager.getMetadataManager().getBucketTable().get(buckKey);
    if (buckInfo != null) {
      return buckInfo.getBucketLayout().isFileSystemOptimized();
    }
    return false;
  }

  @Override
  public OmKeyInfo getKeyInfo(OmKeyArgs args, String clientAddress)
      throws IOException {
    Preconditions.checkNotNull(args);

    OmKeyInfo value = captureLatencyNs(
        metrics.getGetKeyInfoReadKeyInfoLatencyNs(),
        () -> readKeyInfo(args));

    // If operation is head, do not perform any additional steps based on flags.
    // As head operation does not need any of those details.
    if (!args.isHeadOp()) {

      // add block token for read.
      captureLatencyNs(metrics.getGetKeyInfoGenerateBlockTokenLatencyNs(),
          () -> addBlockToken4Read(value));

      // get container pipeline info from cache.
      captureLatencyNs(metrics.getGetKeyInfoRefreshLocationLatencyNs(),
          () -> refreshPipelineFromCache(value,
              args.isForceUpdateContainerCacheFromSCM()));

      if (args.getSortDatanodes()) {
        sortDatanodes(clientAddress, value);
      }
    }
    return value;
  }

  protected void refreshPipelineFromCache(OmKeyInfo keyInfo,
                                          boolean forceRefresh)
      throws IOException {
    Set<Long> containerIds = keyInfo.getKeyLocationVersions().stream()
        .flatMap(v -> v.getLocationList().stream())
        .map(BlockLocationInfo::getContainerID)
        .collect(Collectors.toSet());

    metrics.setForceContainerCacheRefresh(forceRefresh);
    Map<Long, Pipeline> containerLocations =
        scmClient.getContainerLocations(containerIds, forceRefresh);

    for (OmKeyLocationInfoGroup key : keyInfo.getKeyLocationVersions()) {
      for (List<OmKeyLocationInfo> omKeyLocationInfoList :
          key.getLocationLists()) {
        for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfoList) {
          Pipeline pipeline = containerLocations.get(
              omKeyLocationInfo.getContainerID());
          if (pipeline != null &&
              !pipeline.equals(omKeyLocationInfo.getPipeline())) {
            omKeyLocationInfo.setPipeline(pipeline);
          }
        }
      }
    }
  }
}
