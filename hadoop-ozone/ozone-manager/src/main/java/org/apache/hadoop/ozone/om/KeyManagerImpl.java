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

package org.apache.hadoop.ozone.om;

import static java.lang.String.format;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED_DEFAULT;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.READ;
import static org.apache.hadoop.hdds.scm.net.NetConstants.NODE_COST_DEFAULT;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getRemoteUser;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DEFRAG_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DEFRAG_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.ETAG;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_COMPACTION_SERVICE_COLUMNFAMILIES;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_COMPACTION_SERVICE_COLUMNFAMILIES_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_COMPACTION_SERVICE_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_COMPACTION_SERVICE_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_COMPACTION_SERVICE_RUN_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_COMPACTION_SERVICE_RUN_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_COMPACTION_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_COMPACTION_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MPU_CLEANUP_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MPU_CLEANUP_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MPU_CLEANUP_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MPU_CLEANUP_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_DEEP_CLEANING_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_DEEP_CLEANING_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_THREAD_NUMBER_DIR_DELETION;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_THREAD_NUMBER_DIR_DELETION_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_THREAD_NUMBER_KEY_DELETION;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_THREAD_NUMBER_KEY_DELETION_DEFAULT;
import static org.apache.hadoop.ozone.om.OzoneManagerUtils.getBucketLayout;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_KMS_PROVIDER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.SCM_GET_PIPELINE_EXCEPTION;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.KEY;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;
import static org.apache.hadoop.util.Time.monotonicNow;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
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
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.net.InnerNode;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.net.NodeImpl;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.TableMapping;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeletedBlock;
import org.apache.hadoop.ozone.om.PendingKeysDeletion.PurgedKey;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
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
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OMMultipartUploadUtils;
import org.apache.hadoop.ozone.om.service.CompactionService;
import org.apache.hadoop.ozone.om.service.DirectoryDeletingService;
import org.apache.hadoop.ozone.om.service.KeyDeletingService;
import org.apache.hadoop.ozone.om.service.MultipartUploadCleanupService;
import org.apache.hadoop.ozone.om.service.OpenKeyCleanupService;
import org.apache.hadoop.ozone.om.service.SnapshotDeletingService;
import org.apache.hadoop.ozone.om.snapshot.defrag.SnapshotDefragService;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExpiredMultipartUploadsBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.function.CheckedFunction;
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
  private final OzoneBlockTokenSecretManager secretManager;
  private final boolean grpcBlockTokenEnabled;

  private KeyDeletingService keyDeletingService;

  private SstFilteringService snapshotSstFilteringService;
  private SnapshotDefragService snapshotDefragService;
  private SnapshotDeletingService snapshotDeletingService;

  private final KeyProviderCryptoExtension kmsProvider;
  private DirectoryDeletingService dirDeletingService;
  private final OMPerformanceMetrics metrics;

  private BackgroundService openKeyCleanupService;
  private BackgroundService multipartUploadCleanupService;
  private DNSToSwitchMapping dnsToSwitchMapping;
  private CompactionService compactionService;

  public KeyManagerImpl(OzoneManager om, ScmClient scmClient,
      OzoneConfiguration conf, OMPerformanceMetrics metrics) {
    this(om, scmClient, om.getMetadataManager(), conf,
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

    this.ozoneManager = om;
    this.scmClient = scmClient;
    this.metadataManager = metadataManager;
    this.secretManager = secretManager;
    this.kmsProvider = kmsProvider;
    this.metrics = metrics;
  }

  @Override
  public void start(OzoneConfiguration configuration) {
    boolean isCompactionEnabled = configuration.getBoolean(OZONE_OM_COMPACTION_SERVICE_ENABLED,
        OZONE_OM_COMPACTION_SERVICE_ENABLED_DEFAULT);
    startCompactionService(configuration, isCompactionEnabled);

    boolean isSnapshotDeepCleaningEnabled = configuration.getBoolean(OZONE_SNAPSHOT_DEEP_CLEANING_ENABLED,
        OZONE_SNAPSHOT_DEEP_CLEANING_ENABLED_DEFAULT);
    if (keyDeletingService == null) {
      long blockDeleteInterval = configuration.getTimeDuration(
          OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
          OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);
      long serviceTimeout = configuration.getTimeDuration(
          OZONE_BLOCK_DELETING_SERVICE_TIMEOUT,
          OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT,
          TimeUnit.MILLISECONDS);
      int keyDeletingServiceCorePoolSize =
          configuration.getInt(OZONE_THREAD_NUMBER_KEY_DELETION,
              OZONE_THREAD_NUMBER_KEY_DELETION_DEFAULT);
      if (keyDeletingServiceCorePoolSize <= 0) {
        keyDeletingServiceCorePoolSize = 1;
      }
      keyDeletingService = new KeyDeletingService(ozoneManager,
          scmClient.getBlockClient(), blockDeleteInterval,
          serviceTimeout, configuration, keyDeletingServiceCorePoolSize, isSnapshotDeepCleaningEnabled);
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
      int dirDeletingServiceCorePoolSize =
          configuration.getInt(OZONE_THREAD_NUMBER_DIR_DELETION,
              OZONE_THREAD_NUMBER_DIR_DELETION_DEFAULT);
      if (dirDeletingServiceCorePoolSize <= 0) {
        dirDeletingServiceCorePoolSize = 1;
      }
      dirDeletingService =
          new DirectoryDeletingService(dirDeleteInterval, TimeUnit.MILLISECONDS,
              serviceTimeout, ozoneManager, configuration,
              dirDeletingServiceCorePoolSize, isSnapshotDeepCleaningEnabled);
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

    if (snapshotSstFilteringService == null &&
        ozoneManager.isFilesystemSnapshotEnabled()) {
      startSnapshotSstFilteringService(configuration);
    }

    if (snapshotDefragService == null &&
        ozoneManager.isFilesystemSnapshotEnabled()) {
      startSnapshotDefragService(configuration);
    }

    if (snapshotDeletingService == null &&
        ozoneManager.isFilesystemSnapshotEnabled()) {

      long snapshotServiceInterval = configuration.getTimeDuration(
          OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL,
          OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);
      long snapshotServiceTimeout = configuration.getTimeDuration(
          OZONE_SNAPSHOT_DELETING_SERVICE_TIMEOUT,
          OZONE_SNAPSHOT_DELETING_SERVICE_TIMEOUT_DEFAULT,
          TimeUnit.MILLISECONDS);
      try {
        snapshotDeletingService = new SnapshotDeletingService(
            snapshotServiceInterval, snapshotServiceTimeout,
            ozoneManager);
        snapshotDeletingService.start();
      } catch (IOException e) {
        LOG.error("Error starting Snapshot Deleting Service", e);
      }
    }

    if (multipartUploadCleanupService == null) {
      long serviceInterval = configuration.getTimeDuration(
          OZONE_OM_MPU_CLEANUP_SERVICE_INTERVAL,
          OZONE_OM_MPU_CLEANUP_SERVICE_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);
      long serviceTimeout = configuration.getTimeDuration(
          OZONE_OM_MPU_CLEANUP_SERVICE_TIMEOUT,
          OZONE_OM_MPU_CLEANUP_SERVICE_TIMEOUT_DEFAULT,
          TimeUnit.MILLISECONDS);
      multipartUploadCleanupService = new MultipartUploadCleanupService(
          serviceInterval, TimeUnit.MILLISECONDS, serviceTimeout,
          ozoneManager, configuration);
      multipartUploadCleanupService.start();
    }

    Class<? extends DNSToSwitchMapping> dnsToSwitchMappingClass =
        configuration.getClass(
            ScmConfigKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            TableMapping.class, DNSToSwitchMapping.class);
    DNSToSwitchMapping newInstance = ReflectionUtils.newInstance(
        dnsToSwitchMappingClass, configuration);
    dnsToSwitchMapping =
        ((newInstance instanceof CachedDNSToSwitchMapping) ? newInstance
            : new CachedDNSToSwitchMapping(newInstance));
  }

  /**
   * Start the snapshot SST filtering service if interval is not set to disabled value.
   * @param conf
   */
  public void startSnapshotSstFilteringService(OzoneConfiguration conf) {
    if (isSstFilteringSvcEnabled()) {
      if (isDefragSvcEnabled()) {
        LOG.info("SstFilteringService is disabled (despite configuration intending to enable it) " +
            "because SnapshotDefragService is enabled. Defrag effectively performs filtering already.");
        return;
      }

      LOG.info("SstFilteringService is enabled. Note SstFilteringService is " +
          "deprecated in favor of SnapshotDefragService and may be removed in a future release.");

      long serviceInterval = conf.getTimeDuration(
          OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL,
          OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);
      long serviceTimeout = conf.getTimeDuration(
          OZONE_SNAPSHOT_SST_FILTERING_SERVICE_TIMEOUT,
          OZONE_SNAPSHOT_SST_FILTERING_SERVICE_TIMEOUT_DEFAULT,
          TimeUnit.MILLISECONDS);

      snapshotSstFilteringService =
          new SstFilteringService(serviceInterval, TimeUnit.MILLISECONDS,
              serviceTimeout, ozoneManager, conf);
      snapshotSstFilteringService.start();
    } else {
      LOG.info("SstFilteringService is disabled.");
    }
  }

  /**
   * Stop the snapshot SST filtering service if it is running.
   */
  public void stopSnapshotSstFilteringService() {
    if (snapshotSstFilteringService != null) {
      snapshotSstFilteringService.shutdown();
      snapshotSstFilteringService = null;
    } else {
      LOG.info("SstFilteringService is already stopped or not started.");
    }
  }

  /**
   * Start the snapshot defrag service if interval is not set to disabled value.
   * @param conf
   */
  public void startSnapshotDefragService(OzoneConfiguration conf) {
    if (isDefragSvcEnabled()) {
      long serviceInterval = conf.getTimeDuration(
          OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL,
          OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);
      long serviceTimeout = conf.getTimeDuration(
          OZONE_SNAPSHOT_DEFRAG_SERVICE_TIMEOUT,
          OZONE_SNAPSHOT_DEFRAG_SERVICE_TIMEOUT_DEFAULT,
          TimeUnit.MILLISECONDS);

      try {
        snapshotDefragService =
            new SnapshotDefragService(serviceInterval, TimeUnit.MILLISECONDS,
                serviceTimeout, ozoneManager, conf);
        snapshotDefragService.start();
      } catch (IOException e) {
        LOG.error("Error starting Snapshot Defrag Service", e);
      }
    } else {
      LOG.info("SnapshotDefragService is disabled. Snapshot defragmentation will not run periodically.");
    }
  }

  /**
   * Stop the snapshot defrag service if it is running.
   */
  public void stopSnapshotDefragService() {
    if (snapshotDefragService != null) {
      snapshotDefragService.shutdown();
      snapshotDefragService = null;
    } else {
      LOG.info("SnapshotDefragService is already stopped or not started.");
    }
  }

  private void startCompactionService(OzoneConfiguration configuration,
                                      boolean isCompactionServiceEnabled) {
    if (compactionService == null && isCompactionServiceEnabled) {
      long compactionInterval = configuration.getTimeDuration(
          OZONE_OM_COMPACTION_SERVICE_RUN_INTERVAL,
          OZONE_OM_COMPACTION_SERVICE_RUN_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);
      long serviceTimeout = configuration.getTimeDuration(
          OZONE_OM_COMPACTION_SERVICE_TIMEOUT,
          OZONE_OM_COMPACTION_SERVICE_TIMEOUT_DEFAULT,
          TimeUnit.MILLISECONDS);
      String compactionColumnFamilies = configuration.get(
          OZONE_OM_COMPACTION_SERVICE_COLUMNFAMILIES,
          OZONE_OM_COMPACTION_SERVICE_COLUMNFAMILIES_DEFAULT);
      String[] tables = compactionColumnFamilies.split(",");
      compactionService = new CompactionService(ozoneManager, TimeUnit.MILLISECONDS,
          compactionInterval, serviceTimeout, Arrays.asList(tables));
      compactionService.start();
    }
  }

  KeyProviderCryptoExtension getKMSProvider() {
    return kmsProvider;
  }

  @Override
  public void stop() {
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
    if (snapshotSstFilteringService != null) {
      snapshotSstFilteringService.shutdown();
      snapshotSstFilteringService = null;
    }
    if (snapshotDefragService != null) {
      snapshotDefragService.shutdown();
      snapshotDefragService = null;
    }
    if (snapshotDeletingService != null) {
      snapshotDeletingService.shutdown();
      snapshotDeletingService = null;
    }
    if (multipartUploadCleanupService != null) {
      multipartUploadCleanupService.shutdown();
      multipartUploadCleanupService = null;
    }
    if (compactionService != null) {
      compactionService.shutdown();
      compactionService = null;
    }
  }

  /**
   * Get the SnapshotDefragService instance.
   *
   * @return SnapshotDefragService instance, or null if not initialized
   */
  @Override
  public SnapshotDefragService getSnapshotDefragService() {
    return snapshotDefragService;
  }

  private OmBucketInfo getBucketInfo(String volumeName, String bucketName)
      throws IOException {
    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
    return metadataManager.getBucketTable().get(bucketKey);
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
    return Objects.requireNonNull(edek, "edek == null");
  }

  @Override
  public OmKeyInfo lookupKey(OmKeyArgs args, ResolvedBucket bucket,
      String clientAddress) throws IOException {
    Objects.requireNonNull(args, "args == null");

    OmKeyInfo value = captureLatencyNs(metrics.getLookupReadKeyInfoLatencyNs(),
        () -> readKeyInfo(args, bucket.bucketLayout()));

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

  private OmKeyInfo readKeyInfo(OmKeyArgs args, BucketLayout bucketLayout)
      throws IOException {
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    OmKeyInfo value = null;

    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volumeName,
        bucketName);
    try {
      keyName = OMClientRequest
          .validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(), keyName,
              bucketLayout);

      if (bucketLayout.isFileSystemOptimized()) {
        value = getOmKeyInfoFSO(volumeName, bucketName, keyName);
      } else {
        value = getOmKeyInfo(volumeName, bucketName, keyName, bucketLayout);
        if (value != null) {
          // For Legacy & OBS buckets, any key is a file by default. This is to
          // keep getKeyInfo compatible with OFS clients.
          value.setFile(true);
        }
      }
    } catch (IOException ex) {
      if (ex instanceof OMException) {
        throw ex;
      }
      throw new OMException(
          format("Error reading key metadata: /%s/%s/%s",
              volumeName, bucketName, keyName),
          ex, INTERNAL_ERROR);
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
    int partNumberParam = args.getMultipartUploadPartNumber();
    if (partNumberParam > 0) {
      OmKeyLocationInfoGroup latestLocationVersion = value.getLatestVersionLocations();
      if (latestLocationVersion != null && latestLocationVersion.isMultipartKey()) {
        List<OmKeyLocationInfo> currentLocations = latestLocationVersion.getBlocksLatestVersionOnly()
                .stream()
                .filter(it -> it.getPartNumber() == partNumberParam)
                .collect(Collectors.toList());

        value.updateLocationInfoList(currentLocations, true, true);

        value.setDataSize(currentLocations.stream()
            .mapToLong(BlockLocationInfo::getLength)
            .sum());
      }
    }
    return value;
  }

  private OmKeyInfo getOmKeyInfo(String volumeName, String bucketName,
      String keyName, BucketLayout bucketLayout) throws IOException {
    String keyBytes =
        metadataManager.getOzoneKey(volumeName, bucketName, keyName);
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
    OzoneFileStatus fileStatus = OMFileRequest.getOMKeyInfoIfExists(
        metadataManager, volumeName, bucketName, keyName, scmBlockSize,
        ozoneManager.getDefaultReplicationConfig(), false);
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
    Objects.requireNonNull(value, "OMKeyInfo cannot be null");
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
  public ListKeysResult listKeys(String volumeName, String bucketName,
      String startKey, String keyPrefix,
      int maxKeys) throws IOException {
    Objects.requireNonNull(volumeName, "volumeName == null");
    Objects.requireNonNull(bucketName, "bucketName == null");
    OmBucketInfo omBucketInfo = getBucketInfo(volumeName, bucketName);
    if (omBucketInfo == null) {
      throw new OMException("Bucket " + bucketName + " not found.",
          ResultCodes.BUCKET_NOT_FOUND);
    }
    BucketLayout bucketLayout = omBucketInfo.getBucketLayout();
    // We don't take a lock in this path, since we walk the
    // underlying table using an iterator. That automatically creates a
    // snapshot of the data, so we don't need these locks at a higher level
    // when we iterate.
    if (bucketLayout.shouldNormalizePaths(ozoneManager.getEnableFileSystemPaths())) {
      startKey = OmUtils.normalizeKey(startKey, true);
      keyPrefix = OmUtils.normalizeKey(keyPrefix, true);
    }

    ListKeysResult listKeysResult =
        metadataManager.listKeys(volumeName, bucketName, startKey, keyPrefix,
            maxKeys);
    List<OmKeyInfo> keyList = listKeysResult.getKeys();

    // For listKeys, we return the latest Key Location by default
    for (OmKeyInfo omKeyInfo : keyList) {
      slimLocationVersion(omKeyInfo);
    }

    return listKeysResult;
  }

  @Override
  public PendingKeysDeletion getPendingDeletionKeys(
      final CheckedFunction<KeyValue<String, OmKeyInfo>, Boolean, IOException> filter, final int count)
      throws IOException {
    return getPendingDeletionKeys(null, null, null, filter, count);
  }

  @Override
  public PendingKeysDeletion getPendingDeletionKeys(
      String volume, String bucket, String startKey,
      CheckedFunction<KeyValue<String, OmKeyInfo>, Boolean, IOException> filter,
      int count) throws IOException {
    Map<String, PurgedKey> purgedKeys = Maps.newHashMap();
    Map<String, RepeatedOmKeyInfo> keysToModify = new HashMap<>();
    int notReclaimableKeyCount = 0;

    // Bucket prefix would be empty if volume is empty i.e. either null or "".
    Table<String, RepeatedOmKeyInfo> deletedTable = metadataManager.getDeletedTable();
    Optional<String> bucketPrefix = getBucketPrefix(volume, bucket, deletedTable);
    try (TableIterator<String, ? extends KeyValue<String, RepeatedOmKeyInfo>>
             delKeyIter = deletedTable.iterator(bucketPrefix.orElse(""))) {

      /* Seeking to the start key if it not null. The next key picked up would be ensured to start with the bucket
         prefix, {@link org.apache.hadoop.hdds.utils.db.Table#iterator(bucketPrefix)} would ensure this.
       */
      if (startKey != null) {
        delKeyIter.seek(startKey);
      }
      int currentCount = 0;
      while (delKeyIter.hasNext() && currentCount < count) {
        KeyValue<String, RepeatedOmKeyInfo> kv = delKeyIter.next();
        if (kv != null) {
          RepeatedOmKeyInfo notReclaimableKeyInfo = new RepeatedOmKeyInfo(kv.getValue().getBucketId());
          Map<String, PurgedKey> reclaimableKeys = Maps.newHashMap();
          // Multiple keys with the same path can be queued in one DB entry
          RepeatedOmKeyInfo infoList = kv.getValue();
          long bucketId = infoList.getBucketId();
          int reclaimableKeyCount = 0;
          for (OmKeyInfo info : infoList.getOmKeyInfoList()) {

            // Skip the key if the filter doesn't allow the file to be deleted.
            if (filter == null || filter.apply(Table.newKeyValue(kv.getKey(), info))) {
              List<DeletedBlock> deletedBlocks = info.getKeyLocationVersions().stream()
                  .flatMap(versionLocations -> versionLocations.getLocationList().stream()
                      .map(b -> new DeletedBlock(
                          new BlockID(b.getContainerID(),
                            b.getLocalID()),
                            b.getLength(),
                            QuotaUtil.getReplicatedSize(b.getLength(), info.getReplicationConfig())
                      ))).collect(Collectors.toList());
              String blockGroupName = kv.getKey() + "/" + reclaimableKeyCount++;

              BlockGroup keyBlocks = BlockGroup.newBuilder().setKeyName(blockGroupName)
                  .addAllDeletedBlocks(deletedBlocks)
                  .build();
              reclaimableKeys.put(blockGroupName,
                  new PurgedKey(info.getVolumeName(), info.getBucketName(), bucketId,
                  keyBlocks, kv.getKey(), OMKeyRequest.sumBlockLengths(info), info.isDeletedKeyCommitted()));
              currentCount++;
            } else {
              notReclaimableKeyInfo.addOmKeyInfo(info);
            }
          }

          List<OmKeyInfo> notReclaimableKeyInfoList = notReclaimableKeyInfo.getOmKeyInfoList();

          // If all the versions are not reclaimable, then modify key by just purging the key that can be purged.
          if (!notReclaimableKeyInfoList.isEmpty() &&
              notReclaimableKeyInfoList.size() != infoList.getOmKeyInfoList().size()) {
            keysToModify.put(kv.getKey(), notReclaimableKeyInfo);
          }
          purgedKeys.putAll(reclaimableKeys);
          notReclaimableKeyCount += notReclaimableKeyInfoList.size();
        }
      }
    }
    return new PendingKeysDeletion(purgedKeys, keysToModify, notReclaimableKeyCount);
  }

  private <V, R> List<KeyValue<String, R>> getTableEntries(String startKey,
          TableIterator<String, ? extends KeyValue<String, V>> tableIterator,
          Function<V, R> valueFunction,
          CheckedFunction<KeyValue<String, V>, Boolean, IOException> filter,
          int size) throws IOException {
    List<KeyValue<String, R>> entries = new ArrayList<>();
    /* Seek to the start key if it's not null. The next key in queue is ensured to start with the bucket
         prefix, {@link org.apache.hadoop.hdds.utils.db.Table#iterator(bucketPrefix)} would ensure this.
    */
    if (startKey != null) {
      tableIterator.seek(startKey);
    } else {
      tableIterator.seekToFirst();
    }
    int currentCount = 0;
    while (tableIterator.hasNext() && currentCount < size) {
      KeyValue<String, V> kv = tableIterator.next();
      if (kv != null && filter.apply(kv)) {
        entries.add(Table.newKeyValue(kv.getKey(), valueFunction.apply(kv.getValue())));
        currentCount++;
      }
    }
    return entries;
  }

  private Optional<String> getBucketPrefix(String volumeName, String bucketName, Table table) throws IOException {
    // Bucket prefix would be empty if both volume & bucket is empty i.e. either null or "".
    if (StringUtils.isEmpty(volumeName) && StringUtils.isEmpty(bucketName)) {
      return Optional.empty();
    } else if (StringUtils.isEmpty(bucketName) || StringUtils.isEmpty(volumeName)) {
      throw new IOException("One of volume : " + volumeName + ", bucket: " + bucketName + " is empty." +
          " Either both should be empty or none of the arguments should be empty");
    }
    return Optional.of(metadataManager.getTableBucketPrefix(table.getName(), volumeName, bucketName));
  }

  @Override
  public List<KeyValue<String, String>> getRenamesKeyEntries(
      String volume, String bucket, String startKey,
      CheckedFunction<KeyValue<String, String>, Boolean, IOException> filter, int size) throws IOException {
    Table<String, String> snapshotRenamedTable = metadataManager.getSnapshotRenamedTable();
    Optional<String> bucketPrefix = getBucketPrefix(volume, bucket, snapshotRenamedTable);
    try (TableIterator<String, ? extends KeyValue<String, String>>
             renamedKeyIter = snapshotRenamedTable.iterator(bucketPrefix.orElse(""))) {
      return getTableEntries(startKey, renamedKeyIter, Function.identity(), filter, size);
    }
  }

  @Override
  public CheckedFunction<KeyManager, OmDirectoryInfo, IOException> getPreviousSnapshotOzoneDirInfo(
      long volumeId, OmBucketInfo bucketInfo, OmDirectoryInfo keyInfo) throws IOException {
    String currentKeyPath = metadataManager.getOzonePathKey(volumeId, bucketInfo.getObjectID(),
        keyInfo.getParentObjectID(), keyInfo.getName());
    return getPreviousSnapshotOzonePathInfo(bucketInfo, keyInfo.getObjectID(), currentKeyPath,
        (km) -> km.getMetadataManager().getDirectoryTable());
  }

  @Override
  public CheckedFunction<KeyManager, OmDirectoryInfo, IOException> getPreviousSnapshotOzoneDirInfo(
      long volumeId, OmBucketInfo bucketInfo, OmKeyInfo keyInfo) throws IOException {
    String currentKeyPath = metadataManager.getOzonePathKey(volumeId, bucketInfo.getObjectID(),
        keyInfo.getParentObjectID(), keyInfo.getFileName());
    return getPreviousSnapshotOzonePathInfo(bucketInfo, keyInfo.getObjectID(), currentKeyPath,
        (previousSnapshotKM) -> previousSnapshotKM.getMetadataManager().getDirectoryTable());
  }

  @Override
  public CheckedFunction<KeyManager, OmKeyInfo, IOException> getPreviousSnapshotOzoneKeyInfo(
      long volumeId, OmBucketInfo bucketInfo, OmKeyInfo keyInfo) throws IOException {
    String currentKeyPath = bucketInfo.getBucketLayout().isFileSystemOptimized()
        ? metadataManager.getOzonePathKey(volumeId, bucketInfo.getObjectID(), keyInfo.getParentObjectID(),
        keyInfo.getFileName()) : metadataManager.getOzoneKey(bucketInfo.getVolumeName(), bucketInfo.getBucketName(),
        keyInfo.getKeyName());
    return getPreviousSnapshotOzonePathInfo(bucketInfo, keyInfo.getObjectID(), currentKeyPath,
        (previousSnapshotKM) -> previousSnapshotKM.getMetadataManager().getKeyTable(bucketInfo.getBucketLayout()));
  }

  private <T> CheckedFunction<KeyManager, T, IOException> getPreviousSnapshotOzonePathInfo(
      OmBucketInfo bucketInfo, long objectId, String currentKeyPath,
      Function<KeyManager, Table<String, T>> table) throws IOException {
    String renameKey = metadataManager.getRenameKey(bucketInfo.getVolumeName(), bucketInfo.getBucketName(), objectId);
    String renamedKey = metadataManager.getSnapshotRenamedTable().getIfExist(renameKey);
    return (previousSnapshotKM) -> table.apply(previousSnapshotKM).get(
        renamedKey != null ? renamedKey : currentKeyPath);
  }

  @Override
  public List<KeyValue<String, List<OmKeyInfo>>> getDeletedKeyEntries(
      String volume, String bucket, String startKey,
      CheckedFunction<KeyValue<String, RepeatedOmKeyInfo>, Boolean, IOException> filter,
      int size) throws IOException {
    Table<String, RepeatedOmKeyInfo> deletedTable = metadataManager.getDeletedTable();
    Optional<String> bucketPrefix = getBucketPrefix(volume, bucket, deletedTable);
    try (TableIterator<String, ? extends KeyValue<String, RepeatedOmKeyInfo>>
             delKeyIter = deletedTable.iterator(bucketPrefix.orElse(""))) {
      return getTableEntries(startKey, delKeyIter, RepeatedOmKeyInfo::cloneOmKeyInfoList, filter, size);
    }
  }

  @Override
  public ExpiredOpenKeys getExpiredOpenKeys(Duration expireThreshold,
      int count, BucketLayout bucketLayout, Duration leaseThreshold) throws IOException {
    return metadataManager.getExpiredOpenKeys(expireThreshold, count,
        bucketLayout, leaseThreshold);
  }

  @Override
  public List<ExpiredMultipartUploadsBucket> getExpiredMultipartUploads(
      Duration expireThreshold, int maxParts)
      throws IOException {
    return metadataManager.getExpiredMultipartUploads(expireThreshold,
        maxParts);
  }

  @Override
  public Map<String, String> getObjectTagging(OmKeyArgs args, ResolvedBucket bucket) throws IOException {
    Objects.requireNonNull(args, "args == null");

    OmKeyInfo value = captureLatencyNs(metrics.getLookupReadKeyInfoLatencyNs(),
        () -> readKeyInfo(args, bucket.bucketLayout()));

    return value.getTags();
  }

  @Override
  public OMMetadataManager getMetadataManager() {
    return metadataManager;
  }

  @Override
  public KeyDeletingService getDeletingService() {
    return keyDeletingService;
  }

  @Override
  public DirectoryDeletingService getDirDeletingService() {
    return dirDeletingService;
  }

  @Override
  public BackgroundService getOpenKeyCleanupService() {
    return openKeyCleanupService;
  }

  @Override
  public BackgroundService getMultipartUploadCleanupService() {
    return multipartUploadCleanupService;
  }

  @Override
  public SstFilteringService getSnapshotSstFilteringService() {
    return snapshotSstFilteringService;
  }

  @Override
  public SnapshotDeletingService getSnapshotDeletingService() {
    return snapshotDeletingService;
  }

  @Override
  public CompactionService getCompactionService() {
    return compactionService;
  }

  public boolean isSstFilteringSvcEnabled() {
    long serviceInterval = ozoneManager.getConfiguration()
        .getTimeDuration(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL,
            OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
    // any interval <= 0 causes IllegalArgumentException from scheduleWithFixedDelay
    return serviceInterval > 0;
  }

  public boolean isDefragSvcEnabled() {
    long serviceInterval = ozoneManager.getConfiguration()
        .getTimeDuration(OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL,
            OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
    // any interval <= 0 causes IllegalArgumentException from scheduleWithFixedDelay
    return serviceInterval > 0;
  }

  @Override
  public OmMultipartUploadList listMultipartUploads(String volumeName,
      String bucketName,
      String prefix, String keyMarker, String uploadIdMarker, int maxUploads, boolean withPagination)
      throws OMException {
    Objects.requireNonNull(volumeName, "volumeName == null");
    Objects.requireNonNull(bucketName, "bucketName == null");

    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volumeName,
        bucketName);
    try {
      List<OmMultipartUpload> multipartUploadKeys = metadataManager
          .getMultipartUploadKeys(volumeName, bucketName, prefix, keyMarker, uploadIdMarker, maxUploads,
              !withPagination);
      OmMultipartUploadList.Builder resultBuilder = OmMultipartUploadList.newBuilder();

      if (withPagination && multipartUploadKeys.size() == maxUploads + 1) {
        // Per spec, next markers should be the last element of the returned list, not the lookahead.
        multipartUploadKeys.remove(multipartUploadKeys.size() - 1);
        OmMultipartUpload lastReturned = multipartUploadKeys.get(multipartUploadKeys.size() - 1);
        resultBuilder.setNextKeyMarker(lastReturned.getKeyName())
            .setNextUploadIdMarker(lastReturned.getUploadId())
            .setIsTruncated(true);
      }

      return resultBuilder
          .setUploads(multipartUploadKeys)
          .build();

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
    Objects.requireNonNull(volumeName, "volumeName == null");
    Objects.requireNonNull(bucketName, "bucketName == null");
    Objects.requireNonNull(keyName, "keyName == null");
    Objects.requireNonNull(uploadID, "uploadID == null");
    boolean isTruncated = false;
    int nextPartNumberMarker = 0;
    BucketLayout bucketLayout = BucketLayout.DEFAULT;

    String buckKey = metadataManager.
          getBucketKey(volumeName, bucketName);
    OmBucketInfo buckInfo =
          metadataManager.getBucketTable().get(buckKey);
    bucketLayout = buckInfo.getBucketLayout();

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
        Iterator<PartKeyInfo> partKeyInfoMapIterator =
            multipartKeyInfo.getPartKeyInfoMap().iterator();

        ReplicationConfig replicationConfig = null;

        int count = 0;
        List<OmPartInfo> omPartInfoList = new ArrayList<>();

        while (count < maxParts && partKeyInfoMapIterator.hasNext()) {
          PartKeyInfo partKeyInfo = partKeyInfoMapIterator.next();
          nextPartNumberMarker = partKeyInfo.getPartNumber();
          // As we should return only parts with part number greater
          // than part number marker
          if (nextPartNumberMarker > partNumberMarker) {
            String partName = getPartName(partKeyInfo, volumeName, bucketName,
                keyName);
            // Before HDDS-9680, MPU part does not have eTag metadata, for
            // this case, we return null. The S3G will handle this case by
            // using the MPU part name as the eTag field instead.
            Optional<HddsProtos.KeyValue> eTag = partKeyInfo.getPartKeyInfo()
                .getMetadataList()
                .stream()
                .filter(keyValue -> keyValue.getKey().equals(ETAG))
                .findFirst();
            OmPartInfo omPartInfo = new OmPartInfo(partKeyInfo.getPartNumber(),
                partName,
                partKeyInfo.getPartKeyInfo().getModificationTime(),
                partKeyInfo.getPartKeyInfo().getDataSize(),
                eTag.map(HddsProtos.KeyValue::getValue).orElse(null));
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
                    OMMultipartUploadUtils.getMultipartOpenKey(volumeName, bucketName, keyName, uploadID,
                            metadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);
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
        Objects.requireNonNull(replicationConfig,
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

  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    validateOzoneObj(obj);
    ResolvedBucket resolvedBucket = ozoneManager.resolveBucketLink(
        Pair.of(obj.getVolumeName(), obj.getBucketName()));
    String volume = resolvedBucket.realVolume();
    String bucket = resolvedBucket.realBucket();
    String keyName = obj.getKeyName();
    OmKeyInfo keyInfo;
    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volume, bucket);
    try {
      OMFileRequest.validateBucket(metadataManager, volume, bucket);
      String objectKey = metadataManager.getOzoneKey(volume, bucket, keyName);
      if (isBucketFSOptimized(volume, bucket)) {
        keyInfo = getOmKeyInfoFSO(volume, bucket, keyName);
      } else {
        keyInfo = getOmKeyInfo(volume, bucket, keyName,
            resolvedBucket.bucketLayout());
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

    ResolvedBucket resolvedBucket;
    try {
      resolvedBucket = ozoneManager.resolveBucketLink(
          Pair.of(ozObject.getVolumeName(), ozObject.getBucketName()));
    } catch (IOException e) {
      throw new OMException("Failed to resolveBucketLink:", e, INTERNAL_ERROR);
    }
    String volume = resolvedBucket.realVolume();
    String bucket = resolvedBucket.realBucket();
    String keyName = ozObject.getKeyName();
    String objectKey = metadataManager.getOzoneKey(volume, bucket, keyName);
    OmKeyArgs args = new OmKeyArgs.Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName(keyName)
        .setHeadOp(true)
        .build();

    BucketLayout bucketLayout = BucketLayout.DEFAULT;
    String buckKey =
        metadataManager.getBucketKey(volume, bucket);
    OmBucketInfo buckInfo = null;
    try {
      buckInfo =
          metadataManager.getBucketTable().get(buckKey);
      bucketLayout = buckInfo.getBucketLayout();
    } catch (IOException e) {
      LOG.error("Failed to get bucket for the key: " + buckKey, e);
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
    Objects.requireNonNull(args, "Key args can not be null");
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
    Objects.requireNonNull(args, "Key args can not be null");
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();

    if (isBucketFSOptimized(volumeName, bucketName)) {
      return getOzoneFileStatusFSO(args, clientAddress, false);
    }
    return getOzoneFileStatus(args, clientAddress);
  }

  private OzoneFileStatus getOzoneFileStatus(OmKeyArgs args,
      String clientAddress) throws IOException {

    Objects.requireNonNull(args, "Key args can not be null");
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
      if (keyName.isEmpty()) {
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
    String dirKey = OzoneFSUtils.addTrailingSlashIfNeeded(keyName);
    String targetKey = OzoneFSUtils.addTrailingSlashIfNeeded(
        metadataManager.getOzoneKey(volume, bucket, keyName));

    Table<String, OmKeyInfo> keyTable = metadataManager.getKeyTable(layout);
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>>> cacheIterator =
        keyTable.cacheIterator();
    Set<String> deletedKeys = new HashSet<>();
    while (cacheIterator.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>> cacheEntry =
          cacheIterator.next();
      String cacheKey = cacheEntry.getKey().getCacheKey();
      CacheValue<OmKeyInfo> cacheValue = cacheEntry.getValue();
      boolean exists = cacheValue != null && cacheValue.getCacheValue() != null;
      if (exists
          && cacheKey.startsWith(targetKey)
          && !Objects.equals(cacheKey, targetKey)) {
        LOG.debug("Fake dir {} required for {}", targetKey, cacheKey);
        return createDirectoryKey(cacheValue.getCacheValue(), dirKey);
      }
      // deletedKeys may contain deleted entry while iterating cache iterator
      // To avoid race condition of flush of cache while iterating
      // table iterator.
      if (!exists) {
        deletedKeys.add(cacheKey);
      }
    }

    try (TableIterator<String, ? extends KeyValue<String, OmKeyInfo>>
        keyTblItr = keyTable.iterator(targetKey)) {
      while (keyTblItr.hasNext()) {
        KeyValue<String, OmKeyInfo> keyValue = keyTblItr.next();
        if (keyValue != null) {
          String key = keyValue.getKey();
          // HDDS-7871: RocksIterator#seek() may position at the key
          // past the target, we should check the full dbKeyName.
          // For example, seeking "/vol1/bucket1/dir2/" may return a key
          // in different volume/bucket, such as "/vol1/bucket2/dir2/key2".
          if (key.startsWith(targetKey)) {
            if (!Objects.equals(key, targetKey)
                && !deletedKeys.contains(key)) {
              LOG.debug("Fake dir {} required for {}", targetKey, key);
              return createDirectoryKey(keyValue.getValue(), dirKey);
            }
          } else {
            break;
          }
        }
      }
    }

    return null;
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
      if (keyName.isEmpty()) {
        OMFileRequest.validateBucket(metadataManager, volumeName, bucketName);
        return new OzoneFileStatus();
      }

      fileStatus = OMFileRequest.getOMKeyInfoIfExists(metadataManager,
          volumeName, bucketName, keyName, scmBlockSize,
          ozoneManager.getDefaultReplicationConfig());

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
    return keyInfo.toBuilder()
        .setKeyName(dir)
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(0)
        .setFileEncryptionInfo(encInfo)
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
    Objects.requireNonNull(args, "Key args can not be null");
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
    Objects.requireNonNull(key, "Key info can not be null");
    refreshPipeline(Collections.singletonList(key));
  }

  /**
   * Helper function for listStatus to find key in TableCache.
   */
  private void listStatusFindKeyInTableCache(
      Iterator<Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>>> cacheIter,
      String keyArgs, String startCacheKey, boolean recursive,
      TreeMap<String, OzoneFileStatus> cacheKeyMap) throws IOException {

    Map<String, OmKeyInfo> remainingKeys = new HashMap<>();
    // extract the /volume/buck/ prefix from the startCacheKey
    int volBuckEndIndex = StringUtils.ordinalIndexOf(
        startCacheKey, OZONE_URI_DELIMITER, 3);
    String volumeBuckPrefix = startCacheKey.substring(0, volBuckEndIndex + 1);

    while (cacheIter.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>> entry =
          cacheIter.next();
      String cacheKey = entry.getKey().getCacheKey();
      if (cacheKey.equals(keyArgs)) {
        continue;
      }
      OmKeyInfo cacheOmKeyInfo = entry.getValue().getCacheValue();
      // cacheOmKeyInfo is null if an entry is deleted in cache
      if (cacheOmKeyInfo != null && cacheKey.startsWith(
          keyArgs) && cacheKey.compareTo(startCacheKey) >= 0) {
        if (!recursive) {
          String remainingKey = StringUtils.stripEnd(cacheKey.substring(
              keyArgs.length()), OZONE_URI_DELIMITER);
          // For non-recursive, the remaining part of key can't have '/'
          if (remainingKey.contains(OZONE_URI_DELIMITER)) {
            remainingKeys.put(cacheKey, cacheOmKeyInfo);
            continue;
          }
        }
        OzoneFileStatus fileStatus = new OzoneFileStatus(
            cacheOmKeyInfo, scmBlockSize, !OzoneFSUtils.isFile(cacheKey));
        cacheKeyMap.putIfAbsent(cacheKey, fileStatus);
        // This else block has been added to capture deleted entries in cache.
        // Adding deleted entries in cacheKeyMap as there is a possible race
        // condition where table cache iterator is flushed already when
        // using in the caller of this method.
      } else if (cacheOmKeyInfo == null && !cacheKeyMap.containsKey(cacheKey)) {
        cacheKeyMap.put(cacheKey, null);
      }
    }

    // let's say fsPaths is disabled, then creating a key like a/b/c
    // will not create intermediate keys in the keyTable so only entry
    // in the keyTable would be {a/b/c}. This would be skipped from getting
    // added to cacheKeyMap above as remainingKey would be {b/c} and it
    // contains the slash, In this case we track such keys which are not added
    // to the map, find the immediate child and check if they are present in
    // the map. If not create a fake dir and add it. This is similar to the
    // logic in findKeyInDbWithIterator.
    if (!recursive) {
      for (Map.Entry<String, OmKeyInfo> entry : remainingKeys.entrySet()) {
        String remainingKey = entry.getKey();
        String immediateChild =
            OzoneFSUtils.getImmediateChild(remainingKey, keyArgs);
        if (!cacheKeyMap.containsKey(immediateChild)) {
          // immediateChild contains volume/bucket prefix remove it.
          String immediateChildKeyName =
              immediateChild.replaceAll(volumeBuckPrefix, "");
          OmKeyInfo fakeDirEntry =
              createDirectoryKey(entry.getValue(), immediateChildKeyName);
          cacheKeyMap.put(immediateChild,
              new OzoneFileStatus(fakeDirEntry, scmBlockSize, true));
        }
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

  @Override
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
    Objects.requireNonNull(args, "Key args can not be null");
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    List<OzoneFileStatus> fileStatusList = new ArrayList<>();
    if (numEntries <= 0) {
      return fileStatusList;
    }

    if (isBucketFSOptimized(volumeName, bucketName)) {
      Preconditions.checkArgument(!recursive);
      OzoneListStatusHelper statusHelper =
          new OzoneListStatusHelper(metadataManager, scmBlockSize,
              this::getOzoneFileStatusFSO,
              ozoneManager.getDefaultReplicationConfig());
      Collection<OzoneFileStatus> statuses =
          statusHelper.listStatusFSO(args, startKey, numEntries,
          clientAddress, allowPartialPrefixes);
      return buildFinalStatusList(statuses, args, clientAddress);
    }

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

    TableIterator<String, ? extends KeyValue<String, OmKeyInfo>> iterator;
    Table<String, OmKeyInfo> keyTable;
    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volumeName,
        bucketName);
    try {
      keyTable = metadataManager.getKeyTable(
          getBucketLayout(metadataManager, volumeName, bucketName));
      iterator = getIteratorForKeyInTableCache(recursive, startKey,
          volumeName, bucketName, cacheKeyMap, keyArgs, keyTable);
    } finally {
      metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volumeName,
          bucketName);
    }

    try {
      findKeyInDbWithIterator(recursive, startKey, numEntries, volumeName,
          bucketName, keyName, cacheKeyMap, keyArgs, keyTable, iterator);
    } finally {
      iterator.close();
    }

    int countEntries;

    countEntries = 0;
    // Convert results in cacheKeyMap to List
    for (OzoneFileStatus fileStatus : cacheKeyMap.values()) {
      // Here need to check if a key is deleted as cacheKeyMap will contain
      // deleted entries as well. Adding deleted entries in cacheKeyMap is done
      // as there is a possible race condition where table cache iterator is
      // flushed already and isKeyDeleted check may not work as expected
      // before putting entries in cacheKeyMap in findKeyInDbWithIterator call.
      if (fileStatus == null) {
        continue;
      }
      fileStatusList.add(fileStatus);
      countEntries++;
      if (countEntries >= numEntries) {
        break;
      }
    }
    // Clean up temp map and set
    cacheKeyMap.clear();

    List<OmKeyInfo> keyInfoList = new ArrayList<>(fileStatusList.size());
    fileStatusList.stream().map(OzoneFileStatus::getKeyInfo).forEach(keyInfoList::add);
    if (args.getLatestVersionLocation()) {
      slimLocationVersion(keyInfoList.toArray(new OmKeyInfo[0]));
    }

    refreshPipelineFromCache(keyInfoList);

    if (args.getSortDatanodes()) {
      sortDatanodes(clientAddress, keyInfoList);
    }
    return fileStatusList;
  }

  private TableIterator<String, ? extends KeyValue<String, OmKeyInfo>>
      getIteratorForKeyInTableCache(
      boolean recursive, String startKey, String volumeName, String bucketName,
      TreeMap<String, OzoneFileStatus> cacheKeyMap, String keyArgs,
      Table<String, OmKeyInfo> keyTable) throws IOException {
    TableIterator<String, ? extends KeyValue<String, OmKeyInfo>> iterator;
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>>>
        cacheIter = keyTable.cacheIterator();
    String startCacheKey = metadataManager.getOzoneKey(volumeName, bucketName, startKey);

    // First, find key in TableCache
    listStatusFindKeyInTableCache(cacheIter, keyArgs, startCacheKey,
        recursive, cacheKeyMap);
    iterator = keyTable.iterator();
    return iterator;
  }

  @SuppressWarnings("parameternumber")
  private void findKeyInDbWithIterator(boolean recursive, String startKey,
      long numEntries, String volumeName, String bucketName, String keyName,
      TreeMap<String, OzoneFileStatus> cacheKeyMap, String keyArgs,
      Table<String, OmKeyInfo> keyTable,
      TableIterator<String,
          ? extends KeyValue<String, OmKeyInfo>> iterator)
      throws IOException {
    // Then, find key in DB
    String seekKeyInDb =
        metadataManager.getOzoneKey(volumeName, bucketName, startKey);
    KeyValue<String, OmKeyInfo> entry = iterator.seek(seekKeyInDb);
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
            if (!cacheKeyMap.containsKey(entryInDb)) {
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
              if (!cacheKeyMap.containsKey(entryInDb)) {
                cacheKeyMap.put(entryInDb,
                    new OzoneFileStatus(omKeyInfo, scmBlockSize, !isFile));
                countEntries++;
              }
            } else {
              // if entry is a directory
              if (!cacheKeyMap.containsKey(entryInDb)) {
                if (!entryKeyName.equals(immediateChild)) {
                  OmKeyInfo fakeDirEntry = createDirectoryKey(
                      omKeyInfo, immediateChild);
                  String fakeDirKey = ozoneManager.getMetadataManager()
                      .getOzoneKey(fakeDirEntry.getVolumeName(),
                          fakeDirEntry.getBucketName(),
                          fakeDirEntry.getKeyName());
                  cacheKeyMap.put(fakeDirKey,
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
    refreshPipelineFromCache(keyInfoList);

    if (omKeyArgs.getSortDatanodes()) {
      sortDatanodes(clientAddress, keyInfoList);
    }

    return fileStatusFinalList;
  }

  private String getNextGreaterString(String volumeName, String bucketName,
      String keyPrefix) throws IOException {
    // Increment the last character of the string and return the new ozone key.
    Preconditions.checkArgument(!Strings.isNullOrEmpty(keyPrefix),
        "Key prefix is null or empty");
    final StringCodec codec = StringCodec.get();
    final byte[] keyPrefixInBytes = codec.toPersistedFormat(keyPrefix);
    keyPrefixInBytes[keyPrefixInBytes.length - 1]++;
    return metadataManager.getOzoneKey(volumeName, bucketName,
        codec.fromPersistedFormat(keyPrefixInBytes));
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

  private void sortDatanodes(String clientMachine, OmKeyInfo keyInfo) {
    sortDatanodes(clientMachine, Collections.singletonList(keyInfo));
  }

  private void sortDatanodes(String clientMachine, List<OmKeyInfo> keyInfos) {
    if (keyInfos != null && clientMachine != null) {
      final Map<Set<String>, List<? extends DatanodeDetails>> sortedPipelines = new HashMap<>();
      for (OmKeyInfo keyInfo : keyInfos) {
        OmKeyLocationInfoGroup key = keyInfo.getLatestVersionLocations();
        if (key == null) {
          LOG.warn("No location for key {}", keyInfo);
          continue;
        }
        for (OmKeyLocationInfo k : key.getLocationList()) {
          Pipeline pipeline = k.getPipeline();
          List<DatanodeDetails> nodes = pipeline.getNodes();
          if (nodes.isEmpty()) {
            LOG.warn("No datanodes in pipeline {}", pipeline.getId());
            continue;
          }

          final Set<String> uuidSet = nodes.stream().map(DatanodeDetails::getUuidString)
              .collect(Collectors.toSet());

          List<? extends DatanodeDetails> sortedNodes = sortedPipelines.get(uuidSet);
          if (sortedNodes == null) {
            sortedNodes = sortDatanodes(nodes, clientMachine);
            if (sortedNodes != null) {
              sortedPipelines.put(uuidSet, sortedNodes);
            }
          } else if (LOG.isDebugEnabled()) {
            LOG.debug("Found sorted datanodes for pipeline {} and client {} "
                + "in cache", pipeline.getId(), clientMachine);
          }
          if (!Objects.equals(pipeline.getNodesInOrder(), sortedNodes)) {
            k.setPipeline(pipeline.copyWithNodesInOrder(sortedNodes));
          }
        }
      }
    }
  }

  @VisibleForTesting
  public List<? extends DatanodeDetails> sortDatanodes(List<? extends DatanodeDetails> nodes,
                                             String clientMachine) {
    final Node client = getClientNode(clientMachine, nodes);
    return ozoneManager.getClusterMap()
        .sortByDistanceCost(client, nodes, nodes.size());
  }

  private Node getClientNode(String clientMachine,
                             List<? extends DatanodeDetails> nodes) {
    List<DatanodeDetails> matchingNodes = new ArrayList<>();
    boolean useHostname = ozoneManager.getConfiguration().getBoolean(
        HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME,
        HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
    for (DatanodeDetails node : nodes) {
      if ((useHostname ? node.getHostName() : node.getIpAddress()).equals(
          clientMachine)) {
        matchingNodes.add(node);
      }
    }
    return !matchingNodes.isEmpty() ? matchingNodes.get(0) :
        getOtherNode(clientMachine);
  }

  private Node getOtherNode(String clientMachine) {
    try {
      String clientLocation = resolveNodeLocation(clientMachine);
      if (clientLocation != null) {
        Node rack = ozoneManager.getClusterMap().getNode(clientLocation);
        if (rack instanceof InnerNode) {
          return new NodeImpl(clientMachine, clientLocation,
              (InnerNode) rack, rack.getLevel() + 1,
              NODE_COST_DEFAULT);
        }
      }
    } catch (Exception e) {
      LOG.info("Could not resolve client {}: {}",
          clientMachine, e.getMessage());
    }
    return null;
  }

  private String resolveNodeLocation(String hostname) {
    List<String> hosts = Collections.singletonList(hostname);
    List<String> resolvedHosts = dnsToSwitchMapping.resolve(hosts);
    if (resolvedHosts != null && !resolvedHosts.isEmpty()) {
      String location = resolvedHosts.get(0);
      LOG.debug("Node {} resolved to location {}", hostname, location);
      return location;
    } else {
      LOG.debug("Node resolution did not yield any result for {}", hostname);
      return null;
    }
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
  public TableIterator<String, ? extends KeyValue<String, OmKeyInfo>> getDeletedDirEntries(
      String volume, String bucket) throws IOException {
    Table<String, OmKeyInfo> deletedDirTable = metadataManager.getDeletedDirTable();
    Optional<String> bucketPrefix = getBucketPrefix(volume, bucket, deletedDirTable);
    return deletedDirTable.iterator(bucketPrefix.orElse(""));
  }

  @Override
  public DeleteKeysResult getPendingDeletionSubDirs(long volumeId, long bucketId, OmKeyInfo parentInfo,
      CheckedFunction<KeyValue<String, OmKeyInfo>, Boolean, IOException> filter, int remainingNum) throws IOException {
    return gatherSubPathsWithIterator(volumeId, bucketId, parentInfo, metadataManager.getDirectoryTable(),
        kv -> Table.newKeyValue(metadataManager.getOzoneDeletePathKey(kv.getValue().getObjectID(), kv.getKey()),
            OMFileRequest.getKeyInfoWithFullPath(parentInfo, kv.getValue())), filter, remainingNum);
  }

  private <T extends WithParentObjectId> DeleteKeysResult gatherSubPathsWithIterator(long volumeId, long bucketId,
      OmKeyInfo parentInfo, Table<String, T> table,
      CheckedFunction<KeyValue<String, T>, KeyValue<String, OmKeyInfo>, IOException> deleteKeyTransformer,
      CheckedFunction<KeyValue<String, OmKeyInfo>, Boolean, IOException> deleteKeyFilter, int remainingNum)
      throws IOException {
    List<OmKeyInfo> keyInfos = new ArrayList<>();
    String seekFileInDB = metadataManager.getOzonePathKey(volumeId, bucketId, parentInfo.getObjectID(), "");
    try (TableIterator<String, ? extends KeyValue<String, T>> iterator = table.iterator(seekFileInDB)) {
      while (iterator.hasNext() && remainingNum > 0) {
        KeyValue<String, T> entry = iterator.next();
        KeyValue<String, OmKeyInfo> keyInfo = deleteKeyTransformer.apply(entry);
        if (deleteKeyFilter.apply(keyInfo)) {
          keyInfos.add(keyInfo.getValue());
          remainingNum--;
        }
      }
      return new DeleteKeysResult(keyInfos, !iterator.hasNext());
    }
  }

  @Override
  public DeleteKeysResult getPendingDeletionSubFiles(long volumeId,
      long bucketId, OmKeyInfo parentInfo,
      CheckedFunction<Table.KeyValue<String, OmKeyInfo>, Boolean, IOException> filter, int remainingNum)
          throws IOException {
    CheckedFunction<KeyValue<String, OmKeyInfo>, KeyValue<String, OmKeyInfo>, IOException> tranformer = kv -> {
      OmKeyInfo keyInfo = OMFileRequest.getKeyInfoWithFullPath(parentInfo, kv.getValue());
      String deleteKey = metadataManager.getOzoneDeletePathKey(keyInfo.getObjectID(),
          metadataManager.getOzoneKey(keyInfo.getVolumeName(), keyInfo.getBucketName(), keyInfo.getKeyName()));
      return Table.newKeyValue(deleteKey, keyInfo);
    };
    return gatherSubPathsWithIterator(volumeId, bucketId, parentInfo, metadataManager.getFileTable(), tranformer,
        filter, remainingNum);
  }

  public boolean isBucketFSOptimized(String volName, String buckName)
      throws IOException {
    String buckKey =
        metadataManager.getBucketKey(volName, buckName);
    OmBucketInfo buckInfo =
        metadataManager.getBucketTable().get(buckKey);
    if (buckInfo != null) {
      return buckInfo.getBucketLayout().isFileSystemOptimized();
    }
    return false;
  }

  @Override
  public OmKeyInfo getKeyInfo(OmKeyArgs args, ResolvedBucket bucket,
      String clientAddress) throws IOException {
    Objects.requireNonNull(args, "args == null");

    OmKeyInfo value = captureLatencyNs(
        metrics.getGetKeyInfoReadKeyInfoLatencyNs(),
        () -> readKeyInfo(args, bucket.bucketLayout()));

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
        captureLatencyNs(metrics.getGetKeyInfoSortDatanodesLatencyNs(),
            () -> sortDatanodes(clientAddress, value));
      }
    }
    return value;
  }

  private void refreshPipelineFromCache(Iterable<OmKeyInfo> keyInfos)
      throws IOException {
    Set<Long> containerIds = new HashSet<>();
    for (OmKeyInfo keyInfo : keyInfos) {
      extractContainerIDs(keyInfo).forEach(containerIds::add);
    }

    // List API never force cache refresh. If a client detects a block
    // location is outdated, it'll call getKeyInfo with cacheRefresh=true
    // to request cache refresh on individual container.
    Map<Long, Pipeline> containerLocations =
        scmClient.getContainerLocations(containerIds, false);

    for (OmKeyInfo keyInfo : keyInfos) {
      setUpdatedContainerLocation(keyInfo, containerLocations);
    }
  }

  protected void refreshPipelineFromCache(OmKeyInfo keyInfo,
                                          boolean forceRefresh)
      throws IOException {
    Set<Long> containerIds = extractContainerIDs(keyInfo)
        .collect(Collectors.toSet());

    metrics.setForceContainerCacheRefresh(forceRefresh);
    Map<Long, Pipeline> containerLocations =
        scmClient.getContainerLocations(containerIds, forceRefresh);

    setUpdatedContainerLocation(keyInfo, containerLocations);
  }

  private void setUpdatedContainerLocation(OmKeyInfo keyInfo,
                         Map<Long, Pipeline> containerLocations) {
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

  @Nonnull
  private Stream<Long> extractContainerIDs(OmKeyInfo keyInfo) {
    return keyInfo.getKeyLocationVersions().stream()
        .flatMap(v -> v.getLocationList().stream())
        .map(BlockLocationInfo::getContainerID);
  }
}
