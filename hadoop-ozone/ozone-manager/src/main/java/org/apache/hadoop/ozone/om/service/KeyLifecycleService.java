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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.fs.FileSystem.TRASH_PREFIX;
import static org.apache.hadoop.fs.ozone.OzoneTrashPolicy.CURRENT;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_DELETE_BATCH_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_DELETE_BATCH_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_DELETE_CACHED_DIRECTORY_MAX_COUNT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_DELETE_CACHED_DIRECTORY_MAX_COUNT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_MOVE_TO_TRASH_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_MOVE_TO_TRASH_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_MPU_ABORT_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_MPU_ABORT_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_INTERVAL_MS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_KEYS_PROCESSED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_KEYS_PROCESSED_DEFAULT;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.OBJECT_STORE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult.EmptyTaskResult;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OzoneTrash;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmLCFilter;
import org.apache.hadoop.ozone.om.helpers.OmLCRule;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleScanState;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUpload;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OMMultipartUploadUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDirectoryRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyError;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetLifecycleServiceStatusResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RequestSource;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SaveLifecycleScanStateRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the background service to manage object lifecycle based on bucket lifecycle configuration.
 */
public class KeyLifecycleService extends BackgroundService {
  public static final Logger LOG =
      LoggerFactory.getLogger(KeyLifecycleService.class);

  private final OzoneManager ozoneManager;
  private int keyDeleteBatchSize;
  private int listMaxSize;
  private int mpuAbortLimitPerTask;
  private long cachedDirMaxCount;
  private final AtomicBoolean suspended;
  private final AtomicBoolean isServiceEnabled;
  private final AtomicBoolean moveToTrashEnabled;
  private KeyLifecycleServiceMetrics metrics;
  // A set of bucket name that have LifecycleActionTask scheduled
  private final ConcurrentHashMap<String, LifecycleActionTask> inFlight;
  private OMMetadataManager omMetadataManager;
  private int ratisByteLimit;
  private long stateSaveIntervalMs;
  private long maxKeysProcessedPerState;
  private ClientId clientId = ClientId.randomId();
  private AtomicLong callId = new AtomicLong(0);
  private OzoneTrash ozoneTrash;
  private static List<FaultInjector> injectors;
  private static boolean test = false;
  private static List<RuleListWithDirectoryList> consolidatedRuleList;

  public KeyLifecycleService(OzoneManager ozoneManager,
                             KeyManager manager, long serviceInterval,
                             long serviceTimeout, int poolSize,
                             ConfigurationSource conf) {
    super(KeyLifecycleService.class.getSimpleName(), serviceInterval, TimeUnit.MILLISECONDS,
        poolSize, serviceTimeout, ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
    this.keyDeleteBatchSize = conf.getInt(OZONE_KEY_LIFECYCLE_SERVICE_DELETE_BATCH_SIZE,
        OZONE_KEY_LIFECYCLE_SERVICE_DELETE_BATCH_SIZE_DEFAULT);
    Preconditions.checkArgument(keyDeleteBatchSize > 0,
        OZONE_KEY_LIFECYCLE_SERVICE_DELETE_BATCH_SIZE + " should be a positive value.");
    this.listMaxSize = keyDeleteBatchSize >= 10000 ? keyDeleteBatchSize : 10000;
    this.mpuAbortLimitPerTask = conf.getInt(OZONE_KEY_LIFECYCLE_SERVICE_MPU_ABORT_LIMIT_PER_TASK,
        OZONE_KEY_LIFECYCLE_SERVICE_MPU_ABORT_LIMIT_PER_TASK_DEFAULT);
    Preconditions.checkArgument(mpuAbortLimitPerTask > 0,
        OZONE_KEY_LIFECYCLE_SERVICE_MPU_ABORT_LIMIT_PER_TASK + " should be a positive value.");
    this.cachedDirMaxCount = conf.getLong(OZONE_KEY_LIFECYCLE_SERVICE_DELETE_CACHED_DIRECTORY_MAX_COUNT,
        OZONE_KEY_LIFECYCLE_SERVICE_DELETE_CACHED_DIRECTORY_MAX_COUNT_DEFAULT);
    this.suspended = new AtomicBoolean(false);
    this.metrics = KeyLifecycleServiceMetrics.create();
    this.isServiceEnabled = new AtomicBoolean(conf.getBoolean(OZONE_KEY_LIFECYCLE_SERVICE_ENABLED,
        OZONE_KEY_LIFECYCLE_SERVICE_ENABLED_DEFAULT));
    this.moveToTrashEnabled = new AtomicBoolean(conf.getBoolean(OZONE_KEY_LIFECYCLE_SERVICE_MOVE_TO_TRASH_ENABLED,
        OZONE_KEY_LIFECYCLE_SERVICE_MOVE_TO_TRASH_ENABLED_DEFAULT));
    this.stateSaveIntervalMs = conf.getLong(OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_INTERVAL_MS,
        OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_INTERVAL_MS_DEFAULT);
    if (!test && stateSaveIntervalMs <= 0) {
      LOG.warn("Illegal value {} for Property {}. Set {} to {}", stateSaveIntervalMs,
          OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_INTERVAL_MS, OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_INTERVAL_MS,
          OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_INTERVAL_MS_DEFAULT);
      stateSaveIntervalMs = OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_INTERVAL_MS_DEFAULT;
    }
    this.maxKeysProcessedPerState = conf.getLong(OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_KEYS_PROCESSED,
        OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_KEYS_PROCESSED_DEFAULT);
    if (!test && maxKeysProcessedPerState <= 0) {
      LOG.warn("Illegal value {} for Property {}. Set {} to {}", maxKeysProcessedPerState,
          OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_KEYS_PROCESSED, OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_KEYS_PROCESSED,
          OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_KEYS_PROCESSED_DEFAULT);
      maxKeysProcessedPerState = OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_KEYS_PROCESSED_DEFAULT;
    }
    LOG.info("stateSaveIntervalMs = {}, maxKeysProcessedPerState = {}", stateSaveIntervalMs, maxKeysProcessedPerState);
    this.inFlight = new ConcurrentHashMap();
    this.omMetadataManager = ozoneManager.getMetadataManager();
    int limit = (int) conf.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    // always go to 90% of max limit for request as other header will be added
    this.ratisByteLimit = (int) (limit * 0.9);
    this.ozoneTrash = ozoneManager.getOzoneTrash();
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    if (!shouldRun()) {
      return queue;
    }

    List<OmLifecycleConfiguration> lifecycleConfigurationList = null;
    try {
      lifecycleConfigurationList = omMetadataManager.listLifecycleConfigurations();
    } catch (OMException e) {
      LOG.error("Failed to list lifecycle configurations", e);
      return queue;
    }
    for (OmLifecycleConfiguration lifecycleConfiguration : lifecycleConfigurationList) {
      try {
        lifecycleConfiguration.valid();
      } catch (OMException e) {
        LOG.error("Skip invalid lifecycle configuration for {}/{}: LifecycleConfiguration:\n {}",
            lifecycleConfiguration.getVolume(), lifecycleConfiguration.getBucket(),
            lifecycleConfiguration.getProtobuf(), e);
        continue;
      }
      String bucketKey = omMetadataManager.getBucketKey(lifecycleConfiguration.getVolume(),
          lifecycleConfiguration.getBucket());
      if (lifecycleConfiguration.getRules().stream().anyMatch(r -> r.isEnabled())) {
        LifecycleActionTask task = new LifecycleActionTask(lifecycleConfiguration);
        if (this.inFlight.putIfAbsent(bucketKey, task) == null) {
          queue.add(task);
          LOG.info("LifecycleActionTask of {} is scheduled", bucketKey);
        } else {
          metrics.incrNumSkippedTask();
          LOG.info("LifecycleActionTask of {} is already running", bucketKey);
        }
      } else {
        LOG.info("LifecycleConfiguration of {} is not enabled", bucketKey);
      }
    }
    LOG.info("{} LifecycleActionTasks scheduled", queue.size());
    return queue;
  }

  private boolean shouldRun() {
    if (getOzoneManager() == null) {
      // OzoneManager can be null for testing
      return true;
    }
    return isServiceEnabled.get() && !suspended.get() && getOzoneManager().isLeaderReady();
  }

  public KeyLifecycleServiceMetrics getMetrics() {
    return metrics;
  }

  public OzoneManager getOzoneManager() {
    return ozoneManager;
  }

  /**
   * Suspend the service.
   */
  public void suspend() {
    suspended.set(true);
  }

  /**
   * Resume the service if suspended.
   */
  public void resume() {
    suspended.set(false);
  }

  public boolean isSuspended() {
    return suspended.get();
  }

  @Override
  public void shutdown() {
    super.shutdown();
    KeyLifecycleServiceMetrics.unregister();
  }

  /**
   * Build a GetLifecycleServiceStatusResponse instance.
   * @return GetLifecycleServiceStatusResponse instance
   */
  public GetLifecycleServiceStatusResponse status() {
    Set<String> runningBuckets = new HashSet<>(inFlight.keySet());
    return GetLifecycleServiceStatusResponse.newBuilder()
        .setIsEnabled(isServiceEnabled.get())
        .setIsSuspended(suspended.get())
        .addAllRunningBuckets(runningBuckets)
        .build();
  }

  /**
   * A lifecycle action task for one specific bucket, scanning OM DB and evaluating if any existing
   * object/key qualified for expiration according to bucket's lifecycle configuration, and sending
   * key delete command respectively.
   */
  public final class LifecycleActionTask implements BackgroundTask {
    private final OmLifecycleConfiguration policy;
    private long taskStartTime;
    private long numKeyIterated = 0;
    private long numDirIterated = 0;
    private long numDirDeleted = 0;
    private long numKeyDeleted = 0;
    private long sizeKeyDeleted = 0;
    private long numKeyRenamed = 0;
    private long sizeKeyRenamed = 0;
    private long numDirRenamed = 0;
    private long numMultipartUploadIterated = 0;
    private long numMultipartUploadAborted = 0;
    private String lastScannedKey;
    private String lastScannedDir;
    private String lastScannedDirKey;

    private long lastStateSaveTime = Time.monotonicNow();
    private long lastStateSaveKeyCount = 0;

    private boolean shouldSaveState() {
      if ((Time.monotonicNow() - lastStateSaveTime) > stateSaveIntervalMs ||
          (numKeyIterated - lastStateSaveKeyCount) >= maxKeysProcessedPerState) {
        return true;
      }
      return false;
    }

    public LifecycleActionTask(OmLifecycleConfiguration lcConfig) {
      this.policy = lcConfig;
    }

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() {
      EmptyTaskResult result = EmptyTaskResult.newResult();
      String bucketKey = omMetadataManager.getBucketKey(policy.getVolume(), policy.getBucket());
      // Check if this is the Leader OM. If not leader, no need to execute this task.
      if (shouldRun()) {
        LOG.info("Running LifecycleActionTask {}", bucketKey);
        taskStartTime = Time.monotonicNow();
        lastStateSaveTime = taskStartTime;
        OmBucketInfo bucket;
        try {
          if (getInjector(0) != null) {
            getInjector(0).pause();
          }
          bucket = omMetadataManager.getBucketTable().get(bucketKey);
          if (bucket == null) {
            LOG.warn("Bucket {} cannot be found, might be deleted during this task's execution", bucketKey);
            onFailure(bucketKey);
            return result;
          }
          if (bucket.getObjectID() != policy.getBucketObjectID()) {
            LOG.warn("Bucket object ID doesn't match. ID in bucket is {}, ID in LifecycleConfiguration is {}.",
                bucket.getObjectID(), policy.getBucketObjectID());
            onFailure(bucketKey);
            return result;
          }
        } catch (IOException e) {
          LOG.warn("Failed to get Bucket {}", bucketKey, e);
          onFailure(bucketKey);
          return result;
        }

        OmLifecycleScanState.Builder scanStateBuilder = null;
        try {
          OmLifecycleScanState scanState = omMetadataManager.getLifecycleScanStateTable().get(bucketKey);
          if (scanState == null || (scanState.getBucketObjID() != bucket.getObjectID() ||
              scanState.getLifecycleConfigurationUpdateID() != policy.getUpdateID() ||
              scanState.getScanEndTime() != null)) {
            scanStateBuilder = new OmLifecycleScanState.Builder();
            scanStateBuilder.setBucketKey(bucketKey);
            scanStateBuilder.setScanStartTime(System.currentTimeMillis());
            scanStateBuilder.setBucketObjID(bucket.getObjectID());
            scanStateBuilder.setLifecycleConfigurationUpdateID(policy.getUpdateID());
            LOG.info("Create/Recreate OmLifecycleScanState for {} bucket {} bucketID {} " +
                "lifecycleConfigurationUpdateID {}", bucket.getBucketLayout(), bucketKey, bucket.getObjectID(),
                policy.getUpdateID());
          } else {
            scanStateBuilder = scanState.toBuilder();
            LOG.info("Resume OmLifecycleScanState {}", scanState);
          }
        } catch (Exception e) {
          LOG.warn("Failed to get scan state for bucket {}", bucketKey, e);
        }

        try {
          List<OmLCRule> originRuleList = policy.getRules();
          // remove disabled rules
          List<OmLCRule> ruleList = originRuleList.stream().filter(
              r -> r.isEnabled()).collect(Collectors.toList());

          List<OmLCRule> expirationRules = ruleList.stream()
              .filter(r -> r.getExpiration() != null)
              .collect(Collectors.toList());
          List<OmLCRule> mpuRules = ruleList.stream()
              .filter(r -> r.getAbortIncompleteMultipartUpload() != null)
              .collect(Collectors.toList());

          if (!expirationRules.isEmpty()) {
            LimitedExpiredObjectList expiredKeyList = new LimitedExpiredObjectList(listMaxSize);
            LimitedExpiredObjectList expiredDirList = new LimitedExpiredObjectList(listMaxSize);
            Table<String, OmKeyInfo> keyTable = omMetadataManager.getKeyTable(bucket.getBucketLayout());
            /**
             * Filter treatment.
             * ""  - all objects
             * "/" - if it's OBS/Legacy, means keys starting with "/"; If it's FSO, not supported
             * "/key" - if it's OBS/Legacy, means keys starting with "/key", "/" is literally "/";
             *          If it's FSO, means keys or dirs starting with "key", "/" will be treated as separator mark.
             * "key" - if it's OBS/Legacy, means keys starting with "key";
             *         if it's FSO, means keys for dirs starting with "key" too.
             * "dir/" - if it's OBS/Legacy, means keys starting with "dir/";
             *        - if it's FSO, means keys/dirs under directory "dir", doesn't include directory "dir" itself.
             *        - For FSO bucket, as directory ModificationTime will not be updated when any of its child
             *          key/subdir changes, so remember to add the tailing slash "/" when configure prefix, otherwise
             *          the whole directory will be expired and deleted once its ModificationTime meats the condition.
             */
            if (bucket.getBucketLayout() == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
              OmVolumeArgs volume;
              try {
                volume = omMetadataManager.getVolumeTable().get(omMetadataManager.getVolumeKey(bucket.getVolumeName()));
                if (volume == null) {
                  LOG.warn("Volume {} cannot be found, might be deleted during this task's execution",
                      bucket.getVolumeName());
                  onFailure(bucketKey);
                  return result;
                }
              } catch (IOException e) {
                LOG.warn("Failed to get volume {}", bucket.getVolumeName(), e);
                onFailure(bucketKey);
                return result;
              }
              evaluateFSOBucket(volume, bucket, bucketKey, keyTable, expirationRules, expiredKeyList,
                  expiredDirList, scanStateBuilder);
            } else {
              // use bucket name as key iterator prefix
              evaluateBucket(bucket, keyTable, expirationRules, expiredKeyList, scanStateBuilder);
            }

            if (expiredKeyList.isEmpty() && expiredDirList.isEmpty()) {
              LOG.info("No expired keys/dirs found/remained for bucket {}", bucketKey);
              sendSaveScanStateRequest(scanStateBuilder, true);
            } else {
              LOG.info("{} expired keys and {} expired dirs found and remained for bucket {}",
                  expiredKeyList.size(), expiredDirList.size(), bucketKey);

              // If trash is enabled, move files to trash, instead of send delete requests.
              // OBS bucket doesn't support trash.
              if (bucket.getBucketLayout() == OBJECT_STORE) {
                sendDeleteKeysRequestAndClearList(bucket.getVolumeName(), bucket.getBucketName(), expiredKeyList,
                    false, scanStateBuilder, true);
              } else {
                // handle keys first, then directories
                handleAndClearFullList(bucket, expiredKeyList, false, scanStateBuilder, true);
                handleAndClearFullList(bucket, expiredDirList, true, scanStateBuilder, true);
              }
            }
          }

          if (!mpuRules.isEmpty()) {
            processMultipartUploads(bucket, mpuRules);
          }
        } catch (Throwable e) {
          LOG.error("Failed to evaluate lifecycle configuration for bucket {}", bucketKey, e);
          onFailure(bucketKey);
          return result;
        }

        onSuccess(bucketKey);
      }

      // By design, no one cares about the results of this call back.
      return result;
    }

    @SuppressWarnings("checkstyle:parameternumber")
    private void evaluateFSOBucket(OmVolumeArgs volume, OmBucketInfo bucket, String bucketKey,
        Table<String, OmKeyInfo> keyTable, List<OmLCRule> ruleList,
        LimitedExpiredObjectList expiredKeyList, LimitedExpiredObjectList expiredDirList,
        OmLifecycleScanState.Builder scanStateBuilder) {
      List<OmLCRule> prefixRuleList =
          ruleList.stream().filter(r -> r.isPrefixEnable()).collect(Collectors.toList());
      // r.isPrefixEnable() == false means empty filter
      List<OmLCRule> noPrefixRuleList =
          ruleList.stream().filter(r -> !r.isPrefixEnable()).collect(Collectors.toList());

      if (!noPrefixRuleList.isEmpty()) {
        // evaluate all rules against each key
        prefixRuleList.addAll(noPrefixRuleList);
        evaluateKeyAndDirTable(bucket, volume.getObjectID(), keyTable, "", null, "",
            prefixRuleList, expiredKeyList, expiredDirList, scanStateBuilder);
        return;
      }

      List<RuleListWithDirectoryList> unionPrefixRuleList =
          getRuleUnion(volume.getObjectID(), bucket, prefixRuleList, bucketKey);

      if (unionPrefixRuleList != null) {
        if (unionPrefixRuleList.isEmpty()) {
          // fallback to evaluate the whole bucket
          evaluateKeyAndDirTable(bucket, volume.getObjectID(), keyTable, "", null, "",
              prefixRuleList, expiredKeyList, expiredDirList, scanStateBuilder);
        } else {
          for (RuleListWithDirectoryList ruleWithDirList : unionPrefixRuleList) {
            List<OmLCRule> rules = ruleWithDirList.getRuleList();
            DirectoryList dir = ruleWithDirList.getDirList();
            evaluateKeyAndDirTable(bucket, volume.getObjectID(), keyTable, dir.getLastSubDirPath(),
                dir.getLastSubDir(), dir.getLastSubDirKey(),
                rules, expiredKeyList, expiredDirList, scanStateBuilder);
          }
        }
      }
    }

    /**
     * Finds the directory union list from a list of prefixes and sorts them
     * according to the FSO depth-first iteration order.
     */
    private List<RuleListWithDirectoryList> getRuleUnion(long volumeId, OmBucketInfo bucket,
        List<OmLCRule> rules, String bucketKey) {

      if (rules.isEmpty() || rules.stream().anyMatch(
          r -> r.getEffectivePrefix() == null || r.getEffectivePrefix().isEmpty())) {
        // The union of anything with the root is just the root itself.
        return new ArrayList();
      }

      List<RuleListWithDirectoryList> effectiveRuleList = new ArrayList<>();
      for (OmLCRule rule : rules) {
        String prefix = rule.getEffectivePrefix();
        // Resolve each prefix to actual FSO directories in the DB
        try {
          if (!prefix.endsWith(OzoneConsts.OM_KEY_PREFIX)) {
            // FSO bucket doesn't allow prefix without tailing '/'
            // Prefix ends with a slash, it explicitly refers to a directory (e.g. "log/")
            LOG.warn("Skip rule {} since FILE_SYSTEM_OPTIMIZED bucket prefix must end with '/'", rule);
            continue;
          }

          // Normalize by removing the trailing slash for uniform comparison
          String normalizedPrefix = prefix.substring(0, prefix.length() - 1);
          DirectoryList dirList = getDirList(volumeId, bucket, normalizedPrefix, bucketKey);
          // If the prefix is log/, and "log" dir really exists, then the matched dir is "log".
          // Otherwise, this rule doesn't match any dir/file in this FSO bucket, this rule can be skipped.
          if (!dirList.isEmpty() && dirList.isAllResolvedPrefix()) {
            RuleListWithDirectoryList ruleListWithDirectoryList = new RuleListWithDirectoryList(
                Collections.singletonList(rule), dirList, prefix);
            effectiveRuleList.add(ruleListWithDirectoryList);
          }
        } catch (IOException e) {
          // Directory doesn't exist or IO error, skip this rule
          LOG.warn("Skip to evaluate rule {} due to failed to resolve prefix {} for bucket {}",
              rule, prefix, bucketKey, e);
        }
      }

      if (effectiveRuleList.isEmpty()) {
        // there is no valid rule found, either prefix doesn't end with "/",
        // or any directory along the prefix cannot be found.
        LOG.warn("Prefix of all rules of bucket {} cannot be resolved to an existing directory. ", bucketKey);
        return null;
      }

      if (effectiveRuleList.size() == 1) {
        return effectiveRuleList;
      }

      // Find if one rule's prefix is the sub string of another rule's prefix.
      // e.g.
      // dir1/dir2/, dir1/dir2/dir3/, dir1/ -> dir1/
      // dir1/dir2/, dir1/dir3/, dir1/dir4/ -> dir1/dir2/, dir1/dir3/, dir1/dir4dir1/
      // dir1/dir2/dir3/, dir1/dir2/, dir2/ -> dir1/dir2/, dir2/
      // dir1/dir2/, dir1/dir3/, dir1/ -> dir1/
      List<RuleListWithDirectoryList> consolidatedRules = new ArrayList<>();
      Set<OmLCRule> skipEvaluatedRuleList = new HashSet<>();
      for (int i = 0; i < effectiveRuleList.size(); i++) {
        OmLCRule rule = effectiveRuleList.get(i).getRuleList().get(0);
        if (skipEvaluatedRuleList.contains(rule)) {
          continue;
        }

        RuleListWithDirectoryList consolidatedCandidate = new RuleListWithDirectoryList();
        String consolidatedPrefix = effectiveRuleList.get(i).getConsolidatedPrefix();
        String finalRuleIndexID = rule.getId();
        DirectoryList finalDirList = effectiveRuleList.get(i).getDirList();
        for (int j = i + 1; j < effectiveRuleList.size(); j++) {
          OmLCRule otherRule = effectiveRuleList.get(j).getRuleList().get(0);
          if (skipEvaluatedRuleList.contains(otherRule)) {
            continue;
          }

          DirectoryList otherDirList = effectiveRuleList.get(j).getDirList();
          String otherPrefix = otherRule.getEffectivePrefix();
          if (otherPrefix.startsWith(consolidatedPrefix)) {
            LOG.info("Rule {}'s prefix {} is sub string of rule {}'s prefix {}. " +
                    " Consolidate {} into {}.", otherRule.getId(), otherPrefix, finalRuleIndexID,
                consolidatedPrefix, otherRule.getId(), finalRuleIndexID);
            consolidatedCandidate.addRule(otherRule);
            skipEvaluatedRuleList.add(otherRule);
          } else if (consolidatedPrefix.startsWith(otherPrefix)) {
            LOG.info("Rule {}'s prefix {} is sub string of rule {}'s prefix {}. Consolidate {} int {}. ",
                consolidatedPrefix, consolidatedPrefix, otherRule.getId(), otherPrefix, consolidatedPrefix,
                otherRule.getId());
            consolidatedPrefix = otherPrefix;
            finalRuleIndexID = otherRule.getId();
            finalDirList = otherDirList;
            consolidatedCandidate.addRule(otherRule);
            skipEvaluatedRuleList.add(otherRule);
          }
        }

        consolidatedCandidate.addRule(rule);
        consolidatedCandidate.setDirList(finalDirList);
        consolidatedCandidate.setConsolidatedPrefix(consolidatedPrefix);
        consolidatedRules.add(consolidatedCandidate);
      }

      // Sort the list of paths lexicographically.
      // FSO Depth-First Search order evaluates directories in lexicographical order
      // (since it retrieves entries from RocksDB sorted by name within the same parent).
      // Standard string sort on logical paths separated by "/" perfectly matches this DFS order.
      List<RuleListWithDirectoryList> sortedConsolidatedRules =
          consolidatedRules.stream().sorted(new RuleListWithDirectoryListOrder()).collect(Collectors.toList());

      LOG.info("Final consolidated rules: " +
          sortedConsolidatedRules.stream().map(RuleListWithDirectoryList::toString).collect(Collectors.joining(", ")));
      if (test) {
        consolidatedRuleList = sortedConsolidatedRules;
      }
      return sortedConsolidatedRules;
    }

    private boolean canSkipDir(String currentDirPath, String lastScannedDirInState) {
      if (lastScannedDirInState == null || currentDirPath.isEmpty()) {
        return false;
      }
      String[] cur = currentDirPath.split(OM_KEY_PREFIX);
      String[] last = lastScannedDirInState.split(OM_KEY_PREFIX);
      int n = Math.min(cur.length, last.length);
      for (int i = 0; i < n; i++) {
        int cmp = cur[i].compareTo(last[i]);
        if (cmp != 0) {
          // current name > last name -> skip
          return cmp > 0;
        }
      }
      return false;
    }

    @SuppressWarnings({"checkstyle:parameternumber", "checkstyle:MethodLength"})
    private void evaluateKeyAndDirTable(OmBucketInfo bucket, long volumeObjId, Table<String, OmKeyInfo> keyTable,
        String directoryPath, @Nullable OmDirectoryInfo dir, String dirKey, List<OmLCRule> ruleList,
        LimitedExpiredObjectList keyList, LimitedExpiredObjectList dirList,
        OmLifecycleScanState.Builder scanStateBuilder) {
      String volumeName = bucket.getVolumeName();
      String bucketName = bucket.getBucketName();
      LimitedSizeStack stack = new LimitedSizeStack(cachedDirMaxCount);
      String lastScannedDirInState = scanStateBuilder == null ? null : scanStateBuilder.getLastScannedDir();
      String lastScannedDirKeyInState = scanStateBuilder == null ? null : scanStateBuilder.getLastScannedDirKey();
      String lastScannedKeyInState = scanStateBuilder == null ? null : scanStateBuilder.getLastScannedKey();
      try {
        if (dir != null) {
          stack.push(new PendingEvaluateDirectory(dir, dirKey, directoryPath, null));
        } else {
          // put a placeholder PendingEvaluateDirectory to stack for bucket
          stack.push(new PendingEvaluateDirectory(null, "", "", null));
        }
      } catch (CapacityFullException e) {
        LOG.warn("Abort evaluate {}/{} at {}", volumeName, bucketName, directoryPath != null ? directoryPath : "", e);
        return;
      }

      HashSet<Long> deletedDirSet = new HashSet<>();
      while (!stack.isEmpty()) {
        if (!shouldRun()) {
          LOG.info("LifecycleActionTask for bucket {} stopping. " +
              "Service enabled: {}, suspended: {}, leader ready: {}",
              bucketName, isServiceEnabled.get(), suspended.get(), 
              getOzoneManager() != null ? getOzoneManager().isLeaderReady() : "N/A");
          return;
        }

        PendingEvaluateDirectory item = stack.pop();
        OmDirectoryInfo currentDir = item.getDirectoryInfo();
        String currentDirPath = item.getDirPath();
        long currentDirObjID = currentDir == null ? bucket.getObjectID() : currentDir.getObjectID();
        String currentDirTableKey = item.getDirTableKey();

        /**
         *            /
         *    dir1  dir2  dir3  dir30
         *    / \          / \
         *  dir4  dir5   dir6 dir7
         *               / \
         *             dir8 dir9
         *  lastScannedDir = dir3/dir6/dir8, which means
         *     Scanned:
         *        dir30
         *        dir3/dir7
         *        dir3/dir6/dir9
         *     Half scanned:
         *        dir3/dir6/dir8
         *     Not scanned:
         *        dir3/dir6
         *        dir3
         *        dir1/dir5
         *        dir/dir4
         *        dir1
         *   directoryTable table key format : /volumeId/bucketId/parentId/dirName
         *   based on the depth first evaluation order, and stack push posh iteration pattern
         *   - dir1, on grand level of lastScannedDir, and name order < lastScannedDir grand, not scanned
         *   - dir2, on grand level of lastScannedDir, and name order < lastScannedDir grand, not scanned
         *   - dir3, grand of lastScannedDir, not scanned
         *   - dir30, on grand level of lastScannedDir, and name order > lastScannedDir grand, scanned, skip
         *   - dir3/dir6, parent of lastScannedDir, not scanned
         *   - dir3/dir7, parent level of lastScannedDir, and name order > lastScannedDir parent, scanned, skip
         *   - dir3/dir8, lastScannedDir, partially scanned,
         *   - dir3/dir9, same the same parentID as lastScannedDir, and name order > lastScannedDir, scanned, skip
         */
        if (canSkipDir(currentDirPath, lastScannedDirInState)) {
          LOG.info("Skip {} in LifecycleActionTask for bucket {}. ", currentDirPath, bucketName);
          continue;
        }

        lastScannedDir = currentDirPath;
        lastScannedDirKey = currentDirTableKey;
        if (shouldSaveState()) {
          flushAndSaveState(bucket, keyList, dirList, scanStateBuilder);
        }

        // use current directory's object ID to iterate the keys and directories under it
        String prefix =
            OM_KEY_PREFIX + volumeObjId + OM_KEY_PREFIX + bucket.getObjectID() + OM_KEY_PREFIX + currentDirObjID;
        LOG.debug("Prefix {} for {}/{}", prefix, bucket.getVolumeName(), bucket.getBucketName());

        // get direct sub directories
        DirectoryList subDirSummary;
        boolean newSubDirPushed = false;
        long deletedDirCount = 0;
        if (item.isFirstEvaluate()) {
          try {
            subDirSummary = getSubDirectory(currentDirObjID, prefix, omMetadataManager);
          } catch (IOException e) {
            // log failure, continue to process other directories in stack
            LOG.warn("Failed to get sub directories of {} under {}/{}", currentDirPath, volumeName, bucketName, e);
            continue;
          }
        } else {
          // this item is a parent directory, check how many sub directories are deleted.
          subDirSummary = item.getSubDirSummary();
          for (OmDirectoryInfo subDir : subDirSummary.getSubDirList()) {
            if (deletedDirSet.remove(subDir.getObjectID())) {
              deletedDirCount++;
            }
          }
        }

        if (item.isFirstEvaluate()) {
          // filter sub directory list
          if (!subDirSummary.getSubDirList().isEmpty()) {
            Iterator<OmDirectoryInfo> iterator = subDirSummary.getSubDirList().iterator();
            while (iterator.hasNext()) {
              OmDirectoryInfo subDir = iterator.next();
              String subDirPath = currentDirPath.isEmpty() ? subDir.getName() :
                  currentDirPath + OM_KEY_PREFIX + subDir.getName();
              if (subDirPath.startsWith(TRASH_PREFIX)) {
                iterator.remove();
                continue;
              }
              boolean matched = false;
              for (OmLCRule rule : ruleList) {
                if (rule.getEffectivePrefix() != null && subDirPath.startsWith(rule.getEffectivePrefix())) {
                  matched = true;
                  break;
                }
              }
              if (!matched) {
                iterator.remove();
              }
            }
          }

          if (!subDirSummary.getSubDirList().isEmpty()) {
            item.setDirectoryList(subDirSummary);
            item.setFirstEvaluate(false);
            try {
              stack.push(item);
            } catch (CapacityFullException e) {
              LOG.warn("Abort evaluate {}/{} at {}", volumeName, bucketName, currentDirPath, e);
              return;
            }

            // depth first evaluation, push subDirs into stack
            for (int i = 0; i < subDirSummary.getSubDirCount(); i++) {
              OmDirectoryInfo subDir = subDirSummary.getSubDirList().get(i);
              String subDirPath = currentDirPath.isEmpty() ? subDir.getName() :
                  currentDirPath + OM_KEY_PREFIX + subDir.getName();
              try {
                stack.push(new PendingEvaluateDirectory(subDir, subDirSummary.getSubDirKeyList().get(i),
                    subDirPath, null));
              } catch (CapacityFullException e) {
                LOG.warn("Abort evaluate {}/{} at {}", volumeName, bucketName, subDirPath, e);
                return;
              }
            }
            newSubDirPushed = true;
          }
        }

        if (newSubDirPushed) {
          continue;
        }

        // evaluate direct files, first check cache, then check table
        // there are three cases:
        // a. key is deleted in cache, while it's not deleted in table yet
        // b. key is new added in cache, not in table yet
        // c. key is updated in cache(rename), but not updated in table yet
        //    in this case, the fromKey is a deleted key in cache, and the toKey is a newly added key in cache,
        //    and fromKey is also in table
        long numKeysUnderDir = 0;
        long numKeysExpired = 0;
        HashSet<String> deletedKeySetInCache = new HashSet();
        HashSet<String> keySetInCache = new HashSet();
        Iterator<Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>>> cacheIter = keyTable.cacheIterator();
        while (cacheIter.hasNext()) {
          Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>> entry = cacheIter.next();
          OmKeyInfo key = entry.getValue().getCacheValue();
          if (key == null) {
            deletedKeySetInCache.add(entry.getKey().getCacheKey());
            continue;
          }
          if (key.getParentObjectID() == currentDirObjID) {
            numKeysUnderDir++;
            keySetInCache.add(entry.getKey().getCacheKey());
            String keyPath = currentDirPath.isEmpty() ? key.getKeyName() :
                currentDirPath + OM_KEY_PREFIX + key.getKeyName();
            for (OmLCRule rule : ruleList) {
              if (rule.match(key, keyPath)) {
                // mark key as expired, check next key
                if (keyList.isFull()) {
                  // if keyList is full, send delete/rename request for expired keys
                  handleAndClearFullList(bucket, keyList, false, scanStateBuilder, false);
                }
                keyList.add(keyPath, key.getReplicatedSize(), key.getUpdateID());
                numKeysExpired++;
                break;
              }
            }
            lastScannedKey = entry.getKey().getCacheKey();
          }
        }

        try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> keyTblItr =
                 keyTable.iterator(prefix)) {
          boolean seekPerformed = false;
          if (lastScannedDirKeyInState != null && lastScannedDirKeyInState.compareTo(currentDirTableKey) == 0 &&
              lastScannedKeyInState != null && lastScannedKeyInState.startsWith(prefix)) {
            LOG.info("Seek to key {} under directory {}", scanStateBuilder.getLastScannedKey(), lastScannedDirInState);
            keyTblItr.seek(scanStateBuilder.getLastScannedKey());
            seekPerformed = true;
          }

          while (keyTblItr.hasNext()) {
            if (shouldSaveState()) {
              LOG.info("Saving scan state for bucket {} at key {}", bucketName, lastScannedKey);
              flushAndSaveState(bucket, keyList, dirList, scanStateBuilder);
            }
            Table.KeyValue<String, OmKeyInfo> keyValue = keyTblItr.next();
            OmKeyInfo key = keyValue.getValue();
            String keyPath = currentDirPath.isEmpty() ? key.getKeyName() :
                currentDirPath + OM_KEY_PREFIX + key.getKeyName();
            if (seekPerformed && keyValue.getKey().equals(scanStateBuilder.getLastScannedKey())) {
              continue;
            }
            if (deletedKeySetInCache.remove(keyValue.getKey()) || keySetInCache.remove(keyValue.getKey())) {
              continue;
            }
            numKeyIterated++;
            numKeysUnderDir++;
            for (OmLCRule rule : ruleList) {
              if (key.getParentObjectID() == currentDirObjID && rule.match(key, keyPath)) {
                // mark key as expired, check next key
                if (keyList.isFull()) {
                  // if keyList is full, send delete request for pending deletion keys
                  handleAndClearFullList(bucket, keyList, false, scanStateBuilder, false);
                }
                keyList.add(keyPath, key.getReplicatedSize(), key.getUpdateID());
                numKeysExpired++;
                break;
              }
            }
            lastScannedKey = keyValue.getKey();
          }
        } catch (IOException e) {
          // log failure and continue the process other directories in stack
          LOG.warn("Failed to iterate keyTable for bucket {}/{}", volumeName, bucketName, e);
          continue;
        }

        // if this directory is empty or all files/subDirs are expired, evaluate itself
        if ((numKeysUnderDir == 0 && subDirSummary.getSubDirCount() == 0) ||
            (numKeysUnderDir == numKeysExpired && deletedDirCount == subDirSummary.getSubDirCount())) {
          List<String> pathList = new ArrayList<>();
          boolean skipDir = false;
          for (int i = 0; i < ruleList.size(); i++) { // NOPMD
            OmLCRule rule = ruleList.get(i);
            String path = rule.getEffectivePrefix() != null && rule.getEffectivePrefix().endsWith(OM_KEY_PREFIX) ?
                currentDirPath + OM_KEY_PREFIX : currentDirPath;
            if (path != null && path.equals(rule.getEffectivePrefix())) {
              LOG.info("Prefix directory {} doesn't get expired", path);
              skipDir = true;
              break;
            }
            pathList.add(path);
          }

          if (skipDir) {
            continue;
          }
          for (int i = 0; i < ruleList.size(); i++) {
            String path = pathList.get(i);
            OmLCRule rule = ruleList.get(i);
            if (currentDir != null && rule.match(currentDir, path)) {
              if (dirList.isFull()) {
                // if expiredDirList is full, send delete request for both pending deletion keys and directories
                handleAndClearFullList(bucket, keyList, false, scanStateBuilder, false);
                handleAndClearFullList(bucket, dirList, true, scanStateBuilder, false);
                if (getInjector(2) != null && getInjector(2).getException() != null) {
                  Throwable ex = getInjector(2).getException();
                  getInjector(2).setException(null);
                  throw new RuntimeException(ex);
                }
              }
              dirList.add(currentDirPath, 0, currentDir.getUpdateID());
              deletedDirSet.add(currentDir.getObjectID());
              break;
            }
          }
        }
      }
    }

    private DirectoryList getSubDirectory(long dirObjID, String prefix, OMMetadataManager metaMgr)
        throws IOException {
      DirectoryList subDirList = new DirectoryList();

      // Check all dirTable cache for any sub paths.
      Table dirTable = metaMgr.getDirectoryTable();
      Iterator<Map.Entry<CacheKey<String>, CacheValue<OmDirectoryInfo>>>
          cacheIter = dirTable.cacheIterator();
      HashSet<String> deletedDirSet = new HashSet();
      while (cacheIter.hasNext()) {
        Map.Entry<CacheKey<String>, CacheValue<OmDirectoryInfo>> entry =
            cacheIter.next();
        numDirIterated++;
        OmDirectoryInfo cacheOmDirInfo = entry.getValue().getCacheValue();
        if (cacheOmDirInfo == null) {
          deletedDirSet.add(entry.getKey().getCacheKey());
          continue;
        }
        if (cacheOmDirInfo.getParentObjectID() == dirObjID) {
          subDirList.addSubDir(entry.getKey().getCacheKey(), cacheOmDirInfo, cacheOmDirInfo.getName());
        }
      }

      // Check dirTable entries for any sub paths.
      try (TableIterator<String, ? extends Table.KeyValue<String, OmDirectoryInfo>>
               iterator = dirTable.iterator(prefix)) {
        while (iterator.hasNext()) {
          numDirIterated++;
          Table.KeyValue<String, OmDirectoryInfo> entry = iterator.next();
          OmDirectoryInfo dir = entry.getValue();
          if (deletedDirSet.contains(entry.getKey())) {
            continue;
          }
          if (dir.getParentObjectID() == dirObjID) {
            subDirList.addSubDir(entry.getKey(), dir, dir.getName());
          }
        }
      }
      return subDirList;
    }

    private void flushAndSaveState(OmBucketInfo bucket, LimitedExpiredObjectList expiredKeyList, 
        LimitedExpiredObjectList expiredDirList, OmLifecycleScanState.Builder scanStateBuilder) {
      boolean saved = false;
      if (expiredKeyList != null && !expiredKeyList.isEmpty()) {
        if (bucket.getBucketLayout() == OBJECT_STORE) {
          sendDeleteKeysRequestAndClearList(bucket.getVolumeName(), bucket.getBucketName(), expiredKeyList,
              false, scanStateBuilder, false);
        } else {
          handleAndClearFullList(bucket, expiredKeyList, false, scanStateBuilder, false);
        }
        saved = true;
      }
      if (expiredDirList != null && !expiredDirList.isEmpty()) {
        if (bucket.getBucketLayout() != OBJECT_STORE) {
          handleAndClearFullList(bucket, expiredDirList, true, scanStateBuilder, false);
          saved = true;
        }
      }
      if (!saved) {
        sendSaveScanStateRequest(scanStateBuilder, false);
      }
      lastStateSaveTime = Time.monotonicNow();
      lastStateSaveKeyCount = numKeyIterated;
    }

    private void evaluateBucket(OmBucketInfo bucketInfo,
        Table<String, OmKeyInfo> keyTable, List<OmLCRule> ruleList, LimitedExpiredObjectList expiredKeyList,
        OmLifecycleScanState.Builder scanStateBuilder) {
      String volumeName = bucketInfo.getVolumeName();
      String bucketName = bucketInfo.getBucketName();
      String bucketPrefix = omMetadataManager.getBucketKey(volumeName, bucketName);

      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> keyTblItr =
               keyTable.iterator(bucketPrefix)) {
        boolean seekPerformed = false;
        if (scanStateBuilder != null && scanStateBuilder.getLastScannedKey() != null) {
          keyTblItr.seek(scanStateBuilder.getLastScannedKey());
          seekPerformed = true;
        }

        while (keyTblItr.hasNext()) {
          if (!shouldRun()) {
            LOG.info("KeyLifecycleService is suspended or disabled. " +
                "Stopping LifecycleActionTask for bucket {}.", bucketName);
            return;
          }
          if (shouldSaveState()) {
            flushAndSaveState(bucketInfo, expiredKeyList, null, scanStateBuilder);
          }
          Table.KeyValue<String, OmKeyInfo> keyValue = keyTblItr.next();
          if (seekPerformed && keyValue.getKey().equals(scanStateBuilder.getLastScannedKey())) {
            continue;
          }
          processKey(bucketInfo, keyValue.getValue(), ruleList, expiredKeyList, scanStateBuilder);
          numKeyIterated++;
          lastScannedKey = keyValue.getKey();
        }
      } catch (IOException e) {
        // log failure and continue the process to delete/move files already identified in this run
        LOG.warn("Failed to iterate through bucket {}/{}", volumeName, bucketName, e);
      }
    }

    private void processKey(OmBucketInfo bucketInfo, OmKeyInfo key, List<OmLCRule> ruleList,
        LimitedExpiredObjectList expiredKeyList, OmLifecycleScanState.Builder scanStateBuilder) {
      if (bucketInfo.getBucketLayout() == BucketLayout.LEGACY &&
          key.getKeyName().startsWith(TRASH_PREFIX + OzoneConsts.OM_KEY_PREFIX)) {
        return;
      }
      for (OmLCRule rule : ruleList) {
        if (rule.match(key)) {
          // mark key as expired, check next key
          if (expiredKeyList.isFull()) {
            // if expiredKeyList is full, send delete/rename request for expired keys
            handleAndClearFullList(bucketInfo, expiredKeyList, false, scanStateBuilder, false);
            if (getInjector(2) != null && getInjector(2).getException() != null) {
              Throwable ex = getInjector(2).getException();
              getInjector(2).setException(null);
              throw new RuntimeException(ex);
            }
          }
          expiredKeyList.add(key.getKeyName(), key.getReplicatedSize(), key.getUpdateID());
          break;
        }
      }
    }

    /**
     * Process AbortIncompleteMultipartUpload actions for incomplete multipart uploads.
     * Iterates through the multipartInfoTable and aborts uploads that match the rule criteria
     * and have exceeded the configured days after initiation.
     *
     * @param bucketInfo the bucket information
     * @param ruleList list of lifecycle rules with AbortIncompleteMultipartUpload action
     */
    private void processMultipartUploads(OmBucketInfo bucketInfo, List<OmLCRule> ruleList) {
      String volumeName = bucketInfo.getVolumeName();
      String bucketName = bucketInfo.getBucketName();
      String bucketPrefix = omMetadataManager.getBucketKeyPrefix(volumeName, bucketName);

      LOG.debug("Processing AbortIncompleteMultipartUpload actions for bucket {}/{}", volumeName, bucketName);

      PartCountLimitedList expiredUploads = new PartCountLimitedList(mpuAbortLimitPerTask);
      try (TableIterator<String, ? extends Table.KeyValue<String, OmMultipartKeyInfo>> mpuIterator =
               omMetadataManager.getMultipartInfoTable().iterator(bucketPrefix)) {
        while (mpuIterator.hasNext()) {
          if (!shouldRun()) {
            LOG.info("KeyLifecycleService is suspended or disabled. " +
                "Stopping multipart upload processing for bucket {}.", bucketName);
            return;
          }
          Table.KeyValue<String, OmMultipartKeyInfo> entry = mpuIterator.next();
          OmMultipartKeyInfo mpuKeyInfo = entry.getValue();
          numMultipartUploadIterated++;

          OmMultipartUpload upload;
          try {
            upload = OmMultipartUpload.from(entry.getKey());
          } catch (IllegalArgumentException e) {
            LOG.warn("Failed to parse multipart upload key {} in bucket {}/{}, skipping",
                entry.getKey(), volumeName, bucketName, e);
            continue;
          }

          upload.setCreationTime(Instant.ofEpochMilli(mpuKeyInfo.getCreationTime()));
          String keyName = upload.getKeyName();

          String multipartOpenKey;
          try {
            multipartOpenKey = OMMultipartUploadUtils.getMultipartOpenKey(
                volumeName, bucketName, keyName, upload.getUploadId(),
                omMetadataManager, bucketInfo.getBucketLayout());
          } catch (OMException e) {
            LOG.warn("Failed to get multipart open key for {}/{}/{}, skipping",
                volumeName, bucketName, keyName, e);
            continue;
          }

          OmKeyInfo openKeyInfo = omMetadataManager.getOpenKeyTable(bucketInfo.getBucketLayout())
              .get(multipartOpenKey);
          if (openKeyInfo == null) {
            LOG.warn("Open key not found for multipart upload {}/{}/{}, skipping",
                volumeName, bucketName, keyName);
            continue;
          }

          for (OmLCRule rule : ruleList) {
            if (shouldAbortUpload(openKeyInfo, upload, keyName, rule)) {
              if (expiredUploads.isFull()) {
                LOG.info("Multipart upload batch reached part count limit {}, aborting current batch " +
                    "({} uploads, {} parts) for bucket {}/{}",
                    mpuAbortLimitPerTask, expiredUploads.size(), expiredUploads.getPartCount(),
                    volumeName, bucketName);
                abortExpiredMultipartUploadsAndClear(bucketInfo, expiredUploads);
              }

              // Get part count for this MPU (at least 1 even if no parts uploaded yet)
              int partCount = Math.max(1, mpuKeyInfo.getPartKeyInfoMap().size());
              expiredUploads.add(upload, partCount);
              LOG.debug("Multipart upload {}/{}/{} with uploadId {} ({} parts) will be aborted",
                  volumeName, bucketName, keyName, upload.getUploadId(), partCount);
              break;
            }
          }
        }
      } catch (IOException e) {
        LOG.warn("Failed to iterate multipartInfoTable for bucket {}/{}", volumeName, bucketName, e);
        return;
      }

      if (!expiredUploads.isEmpty()) {
        LOG.info("{} expired multipart uploads ({} parts) remaining for bucket {}/{}",
            expiredUploads.size(), expiredUploads.getPartCount(), volumeName, bucketName);
        abortExpiredMultipartUploadsAndClear(bucketInfo, expiredUploads);
      }
    }

    /**
     * Check if a multipart upload should be aborted based on the lifecycle rule.
     *
     * @param openKeyInfo the open key information with tags
     * @param upload the multipart upload information
     * @param keyName the key name of the upload
     * @param rule the lifecycle rule to evaluate against
     * @return true if the upload should be aborted, false otherwise
     */
    private boolean shouldAbortUpload(OmKeyInfo openKeyInfo, OmMultipartUpload upload,
                                      String keyName, OmLCRule rule) {

      if (!rule.getAbortIncompleteMultipartUpload().shouldAbort(
          upload.getCreationTime().toEpochMilli())) {
        return false;
      }

      String effectivePrefix = rule.getEffectivePrefix();
      if (effectivePrefix != null && !keyName.startsWith(effectivePrefix)) {
        return false;
      }

      OmLCFilter filter = rule.getFilter();
      if (filter != null && !filter.match(openKeyInfo, keyName)) {
        return false;
      }

      return true;
    }

    /**
     * Abort expired multipart uploads by sending an abort request.
     *
     * @param bucketInfo the bucket information
     * @param expiredUploads list of expired multipart uploads to abort
     */
    private void abortExpiredMultipartUploads(OmBucketInfo bucketInfo, List<OmMultipartUpload> expiredUploads) {
      String volumeName = bucketInfo.getVolumeName();
      String bucketName = bucketInfo.getBucketName();

      List<OzoneManagerProtocolProtos.ExpiredMultipartUploadInfo> expiredMPUInfoList = expiredUploads.stream()
          .map(upload -> OzoneManagerProtocolProtos.ExpiredMultipartUploadInfo.newBuilder()
              .setName(upload.getDbKey())
              .build())
          .collect(Collectors.toList());

      OzoneManagerProtocolProtos.ExpiredMultipartUploadsBucket expiredMPUBucket =
          OzoneManagerProtocolProtos.ExpiredMultipartUploadsBucket.newBuilder()
              .setVolumeName(volumeName)
              .setBucketName(bucketName)
              .addAllMultipartUploads(expiredMPUInfoList)
              .build();

      OzoneManagerProtocolProtos.MultipartUploadsExpiredAbortRequest abortRequest =
          OzoneManagerProtocolProtos.MultipartUploadsExpiredAbortRequest.newBuilder()
              .addExpiredMultipartUploadsPerBucket(expiredMPUBucket)
              .build();

      OMRequest omRequest = OMRequest.newBuilder()
          .setCmdType(OzoneManagerProtocolProtos.Type.AbortExpiredMultiPartUploads)
          .setMultipartUploadsExpiredAbortRequest(abortRequest)
          .setVersion(ClientVersion.CURRENT_VERSION)
          .setClientId(clientId.toString())
          .build();

      try {
        long startTime = System.nanoTime();
        OzoneManagerProtocolProtos.OMResponse response = OzoneManagerRatisUtils.submitRequest(
            getOzoneManager(), omRequest, clientId, callId.getAndIncrement());
        long endTime = System.nanoTime();

        if (response != null) {
          if (response.getSuccess()) {
            numMultipartUploadAborted += expiredUploads.size();

            LOG.info("Successfully aborted {} multipart uploads for bucket {}/{} in {} ns",
                expiredUploads.size(), volumeName, bucketName, endTime - startTime);
          } else {
            LOG.error("Failed to abort multipart uploads for bucket {}/{}: {}",
                volumeName, bucketName, response.getMessage());
          }
        } else {
          LOG.error("Received null response when aborting multipart uploads for bucket {}/{}",
              volumeName, bucketName);
        }
      } catch (ServiceException e) {
        LOG.error("Failed to submit abort multipart uploads request for bucket {}/{}",
            volumeName, bucketName, e);
      }
    }

    private void abortExpiredMultipartUploadsAndClear(OmBucketInfo bucketInfo,
                                                      PartCountLimitedList expiredUploads) {
      if (expiredUploads.isEmpty()) {
        return;
      }

      abortExpiredMultipartUploads(bucketInfo, expiredUploads.getUploads());
      expiredUploads.clear();
    }

    /**
     * If the prefix is /dir1/dir2, but dir1 doesn't exist, then it will return an exception.
     * If the prefix is /dir1/dir2, but dir2 doesn't exist, then it will return a list with dir1 only.
     * If the prefix is /dir1/dir2, although dir1 exists, but get(dir1) failed with IOException,
     * then it will return an exception too.
     */
    private DirectoryList getDirList(long volumeID, OmBucketInfo bucket, String prefix, String bucketKey)
        throws IOException {
      // find KeyInfo of each directory for the prefix
      java.nio.file.Path keyPath = Paths.get(prefix);
      Iterator<java.nio.file.Path> elements = keyPath.iterator();
      long lastKnownParentId = bucket.getObjectID();
      DirectoryList directoryList = new DirectoryList();
      StringBuffer currentDirPath = new StringBuffer();
      while (elements.hasNext()) {
        String dirName = elements.next().toString();
        String dbDirName = omMetadataManager.getOzonePathKey(
            volumeID, bucket.getObjectID(), lastKnownParentId, dirName);
        try {
          OmDirectoryInfo omDirInfo = omMetadataManager.getDirectoryTable().get(dbDirName);
          // It's OK there is no directory for the last part of the prefix, which is probably not a directory
          if (omDirInfo == null) {
            if (elements.hasNext()) {
              throw new OMException("Directory " + dbDirName + " does not exist for bucket " + bucketKey,
                  OMException.ResultCodes.DIRECTORY_NOT_FOUND);
            }
            directoryList.setNotAllResolvedPrefix();
          } else {
            if (currentDirPath.length() == 0) {
              currentDirPath.append(dirName);
            } else {
              currentDirPath.append('/').append(dirName);
            }
            directoryList.addSubDir(dbDirName, omDirInfo, currentDirPath.toString());
            lastKnownParentId = omDirInfo.getObjectID();
          }
        } catch (IOException e) {
          LOG.warn("Failed to get directory {} for bucket {}", dbDirName, bucketKey, e);
          throw new IOException("Failed to get directory " + dbDirName + " for bucket " + bucketKey);
        }
      }
      return directoryList;
    }

    private void onFailure(String bucketName) {
      inFlight.remove(bucketName);
      metrics.incrNumFailureTask();
      metrics.incNumKeyIterated(numKeyIterated);
      metrics.incNumDirIterated(numDirIterated);
      metrics.incNumMultipartUploadIterated(numMultipartUploadIterated);
      long timeSpent = Time.monotonicNow() - taskStartTime;
      LOG.info("Spent {} ms on bucket {} to iterate {} keys and {} dirs and {} multipart uploads, " +
              "deleted {} keys with {} bytes, and {} dirs, renamed {} keys with {} bytes, and {} dirs to trash, " +
              "aborted {} multipart uploads", timeSpent, bucketName, numKeyIterated,
          numDirIterated, numMultipartUploadIterated, numKeyDeleted, sizeKeyDeleted, numDirDeleted,
          numKeyRenamed, sizeKeyRenamed, numDirRenamed, numMultipartUploadAborted);
    }

    private void onSuccess(String bucketName) {
      inFlight.remove(bucketName);
      metrics.incrNumSuccessTask();
      long timeSpent = Time.monotonicNow() - taskStartTime;
      metrics.incTaskLatencyMs(timeSpent);
      metrics.incNumKeyIterated(numKeyIterated);
      metrics.incNumDirIterated(numDirIterated);
      metrics.incNumMultipartUploadIterated(numMultipartUploadIterated);
      metrics.incNumMultipartUploadAborted(numMultipartUploadAborted);
      LOG.info("Spent {} ms on bucket {} to iterate {} keys and {} dirs and {} multipart uploads, " +
              "deleted {} keys with {} bytes, and {} dirs, renamed {} keys with {} bytes, and {} dirs to trash, " +
              "aborted {} multipart uploads", timeSpent, bucketName, numKeyIterated,
          numDirIterated, numMultipartUploadIterated, numKeyDeleted, sizeKeyDeleted, numDirDeleted,
          numKeyRenamed, sizeKeyRenamed, numDirRenamed, numMultipartUploadAborted);
    }

    private void handleAndClearFullList(OmBucketInfo bucket, LimitedExpiredObjectList keysList,
        boolean dir, OmLifecycleScanState.Builder scanStateBuilder, boolean scanFinished) {
      if (moveToTrashEnabled.get() && bucket.getBucketLayout() != OBJECT_STORE && ozoneTrash != null) {
        moveToTrash(bucket, keysList, dir);
        sendSaveScanStateRequest(scanStateBuilder, scanFinished);
      } else {
        sendDeleteKeysRequestAndClearList(bucket.getVolumeName(), bucket.getBucketName(), keysList, dir,
            scanStateBuilder, scanFinished);
      }
    }

    private void sendSaveScanStateRequest(OmLifecycleScanState.Builder scanStateBuilder, boolean scanFinished) {
      if (scanStateBuilder != null) {
        if (lastScannedDir != null) {
          scanStateBuilder.setLastScannedDir(lastScannedDir);
        }
        if (lastScannedDirKey != null) {
          scanStateBuilder.setLastScannedDirKey(lastScannedDirKey);
        }
        if (lastScannedKey != null) {
          scanStateBuilder.setLastScannedKey(lastScannedKey);
        }
        if (scanFinished) {
          scanStateBuilder.setScanEndTime(System.currentTimeMillis());
        }
        OmLifecycleScanState state = scanStateBuilder.build();

        SaveLifecycleScanStateRequest saveRequest = SaveLifecycleScanStateRequest.newBuilder()
            .setState(state.getProtobuf())
            .build();

        OMRequest omRequest = OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.SaveLifecycleScanState)
            .setVersion(ClientVersion.CURRENT_VERSION)
            .setClientId(clientId.toString())
            .setSaveLifecycleScanStateRequest(saveRequest)
            .build();

        LOG.debug("Save scan state {}", state);
        try {
          OzoneManagerRatisUtils.submitRequest(getOzoneManager(), omRequest, clientId, callId.getAndIncrement());
        } catch (ServiceException e) {
          LOG.error("Failed to submit SaveLifecycleScanState request", e);
        }
      }
    }

    private void sendDeleteKeysRequestAndClearList(String volume, String bucket, LimitedExpiredObjectList keysList,
        boolean dir, OmLifecycleScanState.Builder scanStateBuilder, boolean scanFinished) {
      try {
        if (getInjector(1) != null) {
          try {
            getInjector(1).pause();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        int batchSize = keyDeleteBatchSize;
        int startIndex = 0;
        for (int i = 0; i < keysList.size();) {
          DeleteKeyArgs.Builder builder =
              DeleteKeyArgs.newBuilder().setBucketName(bucket).setVolumeName(volume);
          int endIndex = startIndex + (batchSize < (keysList.size() - startIndex) ?
              batchSize : keysList.size() - startIndex);
          int keyCount = endIndex - startIndex;
          builder.addAllKeys(keysList.nameSubList(startIndex, endIndex));
          builder.addAllUpdateIDs(keysList.updateIDSubList(startIndex, endIndex));

          DeleteKeyArgs deleteKeyArgs = builder.build();
          DeleteKeysRequest.Builder requestBuilder = DeleteKeysRequest.newBuilder()
                  .setDeleteKeys(deleteKeyArgs)
                  .setSourceType(RequestSource.LIFECYCLE);

          if (scanStateBuilder != null) {
            if (lastScannedKey != null) {
              scanStateBuilder.setLastScannedKey(lastScannedKey);
            }
            if (lastScannedDir != null) {
              scanStateBuilder.setLastScannedDir(lastScannedDir);
            }
            if (lastScannedDirKey != null) {
              scanStateBuilder.setLastScannedDirKey(lastScannedDirKey);
            }
            if (scanFinished) {
              scanStateBuilder.setScanEndTime(System.currentTimeMillis());
            }
            OmLifecycleScanState state = scanStateBuilder.build();
            requestBuilder.setScanState(state.getProtobuf());
            LOG.debug("Save scan state: {}", state);
          }

          DeleteKeysRequest deleteKeysRequest = requestBuilder.build();
          LOG.debug("request size {} for {} keys", deleteKeysRequest.getSerializedSize(), keyCount);

          if (deleteKeysRequest.getSerializedSize() < ratisByteLimit) {
            // send request out
            OMRequest omRequest = OMRequest.newBuilder()
                .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKeys)
                .setVersion(ClientVersion.CURRENT_VERSION)
                .setClientId(clientId.toString())
                .setDeleteKeysRequest(deleteKeysRequest)
                .build();
            long startTime = System.nanoTime();
            final OzoneManagerProtocolProtos.OMResponse response = OzoneManagerRatisUtils.submitRequest(
                getOzoneManager(), omRequest, clientId, callId.getAndIncrement());
            long endTime = System.nanoTime();
            LOG.debug("DeleteKeys request with {} keys cost {} ns", keyCount, endTime - startTime);
            long deletedCount = keyCount;
            long deletedSize = keysList.replicatedSizeSubList(startIndex, endIndex)
                .stream().mapToLong(Long::longValue).sum();
            if (response != null) {
              if (!response.getSuccess()) {
                // log the failure and continue the iterating
                LOG.error("DeleteKeys request " + response.getStatus() + " failed for volume: {}, bucket: {}",
                    volume, bucket);
                if (response.getDeleteKeysResponse().hasUnDeletedKeys()) {
                  DeleteKeyArgs unDeletedKeys = response.getDeleteKeysResponse().getUnDeletedKeys();
                  for (String key : unDeletedKeys.getKeysList()) {
                    Long size = keysList.getReplicatedSize(key);
                    if (size == null) {
                      LOG.error("Undeleted key {}/{}/{} doesn't in keyLists", volume, bucket, key);
                      continue;
                    }
                    deletedCount -= 1;
                    deletedSize -= size;
                  }
                }
                for (DeleteKeyError e : response.getDeleteKeysResponse().getErrorsList()) {
                  Long size = keysList.getReplicatedSize(e.getKey());
                  if (size == null) {
                    LOG.error("Deleted error key {}/{}/{} doesn't in keyLists", volume, bucket, e.getKey());
                    continue;
                  }
                  deletedCount -= 1;
                  deletedSize -= size;
                }
              } else {
                LOG.debug("DeleteKeys request of total {} keys, {} not deleted", keyCount,
                    response.getDeleteKeysResponse().getErrorsCount());
              }
            }
            if (dir) {
              numDirDeleted += deletedCount;
              metrics.incrNumDirDeleted(deletedCount);
            } else {
              numKeyDeleted += deletedCount;
              sizeKeyDeleted += deletedSize;
              metrics.incrNumKeyDeleted(deletedCount);
              metrics.incrSizeKeyDeleted(deletedSize);
            }
            i += keyCount;
            startIndex += keyCount;
          } else {
            batchSize /= 2;
          }
        }
      } catch (ServiceException e) {
        LOG.error("Failed to send DeleteKeysRequest", e);
      } finally {
        keysList.clear();
      }
    }

    private void moveToTrash(OmBucketInfo bucket, LimitedExpiredObjectList keysList, boolean isDir) {
      if (keysList.isEmpty()) {
        return;
      }
      String volumeName = bucket.getVolumeName();
      String bucketName = bucket.getBucketName();
      String trashRoot = TRASH_PREFIX + OM_KEY_PREFIX + bucket.getOwner();
      Path trashCurrent = new Path(trashRoot, CURRENT);
      try {
        checkAndCreateTrashDirIfNeeded(bucket, trashCurrent);
      } catch (IOException e) {
        keysList.clear();
        return;
      }

      for (int i = 0; i < keysList.size(); i++) {
        String keyName = keysList.getName(i);
        Path keyPath = new Path(OzoneConsts.OZONE_URI_DELIMITER + keyName);
        Path baseKeyTrashPath = Path.mergePaths(trashCurrent, keyPath.getParent());
        try {
          checkAndCreateTrashDirIfNeeded(bucket, baseKeyTrashPath);
        } catch (IOException e) {
          LOG.error("Failed to check and create Trash dir {} for bucket {}/{}", baseKeyTrashPath,
              volumeName, bucketName, e);
          continue;
        }
        String targetKeyName = trashCurrent + OM_KEY_PREFIX + keyName;
        KeyArgs keyArgs = KeyArgs.newBuilder().setKeyName(keyName)
            .setVolumeName(volumeName).setBucketName(bucketName).build();

        /**
         * Trash examples:
         * /s3v/test/readme ->  /s3v/test/.Trash/hadoop/Current/readme
         * /s3v/test/dir1/readme -> /s3v/test/.Trash/hadoop/Current/dir1/readme
         */
        RenameKeyRequest renameKeyRequest = RenameKeyRequest.newBuilder()
            .setKeyArgs(keyArgs)
            .setToKeyName(targetKeyName)
            .setUpdateID(keysList.getUpdateID(i))
            .build();

        // send request out
        OMRequest omRequest = OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.RenameKey)
            .setVersion(ClientVersion.CURRENT_VERSION)
            .setClientId(clientId.toString())
            .setRenameKeyRequest(renameKeyRequest)
            .build();
        try {
          // perform preExecute as ratis submit do no perform preExecute
          OMClientRequest omClientRequest = OzoneManagerRatisUtils.createClientRequest(omRequest, ozoneManager);
          UserGroupInformation ugi = UserGroupInformation.createRemoteUser(bucket.getOwner());
          OzoneManagerProtocolProtos.OMResponse omResponse =
              ugi.doAs(new PrivilegedExceptionAction<OzoneManagerProtocolProtos.OMResponse>() {
                @Override
                public OzoneManagerProtocolProtos.OMResponse run() throws Exception {
                  OMRequest request = omClientRequest.preExecute(ozoneManager);
                  return OzoneManagerRatisUtils.submitRequest(getOzoneManager(),
                      request, clientId, callId.getAndIncrement());
                }
              });
          if (omResponse != null) {
            if (!omResponse.getSuccess()) {
              // log the failure and continue the iterating
              LOG.error("RenameKey request failed with source key: {}, dest key: {}", keyName, targetKeyName);
              continue;
            }
          }
          LOG.info("RenameKey request succeed with source key: {}, dest key: {}", keyName, targetKeyName);

          if (isDir) {
            numDirRenamed += 1;
            metrics.incrNumDirRenamed(1);
          } else {
            numKeyRenamed += 1;
            sizeKeyRenamed += keysList.getReplicatedSize(i);
            metrics.incrNumKeyRenamed(1);
            metrics.incrSizeKeyRenamed(keysList.getReplicatedSize(i));
          }
        } catch (IOException | InterruptedException e) {
          LOG.error("Failed to send RenameKeysRequest", e);
        }
      }
      keysList.clear();
    }

    private void checkAndCreateTrashDirIfNeeded(OmBucketInfo bucket, Path dirPath) throws IOException {
      OmKeyArgs key = new OmKeyArgs.Builder().setVolumeName(bucket.getVolumeName())
          .setBucketName(bucket.getBucketName()).setKeyName(dirPath.toString())
          .setOwnerName(bucket.getOwner()).build();
      try {
        ozoneManager.getFileStatus(key);
      } catch (IOException e) {
        if (e instanceof OMException &&
            (((OMException) e).getResult() == OMException.ResultCodes.FILE_NOT_FOUND ||
                ((OMException) e).getResult() == OMException.ResultCodes.DIRECTORY_NOT_FOUND)) {
          // create the trash/Current directory for user
          KeyArgs keyArgs = KeyArgs.newBuilder().setVolumeName(bucket.getVolumeName())
              .setBucketName(bucket.getBucketName()).setKeyName(dirPath.toString())
              .setOwnerName(bucket.getOwner()).setRecursive(true).build();
          OMRequest omRequest = OMRequest.newBuilder().setCreateDirectoryRequest(
                  CreateDirectoryRequest.newBuilder().setKeyArgs(keyArgs))
              .setCmdType(OzoneManagerProtocolProtos.Type.CreateDirectory)
              .setVersion(ClientVersion.CURRENT_VERSION)
              .setClientId(clientId.toString())
              .build();
          try {
            // perform preExecute as ratis submit do no perform preExecute
            final OMClientRequest omClientRequest = OzoneManagerRatisUtils.createClientRequest(omRequest, ozoneManager);
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(bucket.getOwner());
            OzoneManagerProtocolProtos.OMResponse omResponse =
                ugi.doAs(new PrivilegedExceptionAction<OzoneManagerProtocolProtos.OMResponse>() {
                  @Override
                  public OzoneManagerProtocolProtos.OMResponse run() throws Exception {
                    OMRequest request = omClientRequest.preExecute(ozoneManager);
                    return OzoneManagerRatisUtils.submitRequest(getOzoneManager(),
                        request, clientId, callId.getAndIncrement());
                  }
                });

            if (omResponse != null) {
              if (!omResponse.getSuccess()) {
                LOG.error("CreateDirectory request failed with {}, path: {}",
                    omResponse.getMessage(), dirPath);
                throw new IOException("Failed to create trash directory " + dirPath);
              }
            }
            LOG.info("Created directory {}/{}/{}", bucket.getVolumeName(), bucket.getBucketName(), dirPath);
          } catch (InterruptedException | IOException e1) {
            LOG.error("Failed to send CreateDirectoryRequest for {}", dirPath, e1);
            throw new IOException("Failed to send CreateDirectoryRequest request for " + dirPath);
          }
        } else {
          LOG.error("Failed to get trash current directory {} status", dirPath, e);
          throw e;
        }
      }
    }
  }

  @VisibleForTesting
  public static FaultInjector getInjector(int index) {
    return injectors  != null ? injectors.get(index) : null;
  }

  @VisibleForTesting
  public static void setInjectors(List<FaultInjector> instance) {
    injectors = instance;
  }

  @VisibleForTesting
  public static Logger getLog() {
    return LOG;
  }

  @VisibleForTesting
  public void setListMaxSize(int size) {
    this.listMaxSize = size;
  }

  @VisibleForTesting
  public void setMpuAbortLimitPerTask(int limit) {
    this.mpuAbortLimitPerTask = limit;
  }

  @VisibleForTesting
  public void setOzoneTrash(OzoneTrash ozoneTrash) {
    this.ozoneTrash = ozoneTrash;
  }

  @VisibleForTesting
  public void setMoveToTrashEnabled(boolean enabled) {
    this.moveToTrashEnabled.set(enabled);
  }

  /**
   * An in-memory list with limited size to hold expired object infos, including object name and current update ID.
   */
  public static class LimitedExpiredObjectList {
    private final LimitedSizeList<String> objectNames;
    private final List<Long> objectReplicatedSize;
    private final List<Long> objectUpdateIDs;

    public LimitedExpiredObjectList(int maxListSize) {
      this.objectNames = new LimitedSizeList<>(maxListSize);
      this.objectReplicatedSize = new ArrayList<>();
      this.objectUpdateIDs = new ArrayList<>();
    }

    public void add(String name, long size, long updateID) {
      objectNames.add(name);
      objectReplicatedSize.add(size);
      objectUpdateIDs.add(updateID);
    }

    public void addAll(LimitedExpiredObjectList other) {
      objectNames.addAll(other.objectNames);
      objectReplicatedSize.addAll(other.objectReplicatedSize);
      objectUpdateIDs.addAll(other.objectUpdateIDs);
    }

    public int size() {
      return objectNames.size();
    }

    public List<String> nameSubList(int fromIndex, int toIndex) {
      return objectNames.subList(fromIndex, toIndex);
    }

    public List<Long> updateIDSubList(int fromIndex, int toIndex) {
      return objectUpdateIDs.subList(fromIndex, toIndex);
    }

    public List<Long> replicatedSizeSubList(int fromIndex, int toIndex) {
      return objectReplicatedSize.subList(fromIndex, toIndex);
    }

    public Long getReplicatedSize(String keyName) {
      for (int index = 0; index < objectNames.size(); index++) {
        if (objectNames.get(index).equals(keyName)) {
          return objectReplicatedSize.get(index);
        }
      }
      return null;
    }

    public void clear() {
      objectNames.clear();
      objectUpdateIDs.clear();
      objectReplicatedSize.clear();
    }

    public boolean isEmpty() {
      return objectNames.isEmpty();
    }

    public boolean isFull() {
      return objectNames.isFull();
    }

    public String getName(int index) {
      return objectNames.get(index);
    }

    public long getUpdateID(int index) {
      return objectUpdateIDs.get(index);
    }

    public long getReplicatedSize(int index) {
      return objectReplicatedSize.get(index);
    }
  }

  /**
   * An in-memory list with a maximum size. This class is not thread safe.
   */
  public static class LimitedSizeList<T> {
    private final List<T> internalList;
    private final int maxSize;

    public LimitedSizeList(int maxSize) {
      this.maxSize = maxSize;
      this.internalList = new ArrayList<>();
    }

    /**
     * Add an element to the list. It blindly adds the element without check whether the list is full or not.
     * Caller must check the size of the list through isFull() before calling this method.
     */
    public void add(T element) {
      internalList.add(element);
    }

    public void addAll(LimitedSizeList<T> other) {
      internalList.addAll(other.internalList);
    }

    public T get(int index) {
      return internalList.get(index);
    }

    public int size() {
      return internalList.size();
    }

    public List<T> subList(int fromIndex, int toIndex) {
      return internalList.subList(fromIndex, toIndex);
    }

    public boolean isEmpty() {
      return internalList.isEmpty();
    }

    public boolean isFull() {
      boolean full = internalList.size() >= maxSize;
      if (full) {
        LOG.debug("LimitedSizeList has reached maximum size {}", maxSize);
      }
      return full;
    }

    public void clear() {
      internalList.clear();
    }
  }

  /**
   * A list that tracks the total part count of multipart uploads.
   * The list is considered "full" when the total part count reaches the limit.
   * This is used because some MPUs might have 1 part while others might have 10,000 parts.
   */
  public static class PartCountLimitedList {
    private final List<OmMultipartUpload> uploads;
    private final int maxPartCount;
    private int currentPartCount;

    public PartCountLimitedList(int maxPartCount) {
      this.maxPartCount = maxPartCount;
      this.uploads = new ArrayList<>();
      this.currentPartCount = 0;
    }

    /**
     * Add a multipart upload with its part count.
     * Caller should check isFull() before calling this method.
     */
    public void add(OmMultipartUpload upload, int partCount) {
      uploads.add(upload);
      currentPartCount += partCount;
    }

    public List<OmMultipartUpload> getUploads() {
      return uploads;
    }

    public int size() {
      return uploads.size();
    }

    public int getPartCount() {
      return currentPartCount;
    }

    public boolean isEmpty() {
      return uploads.isEmpty();
    }

    public boolean isFull() {
      boolean full = currentPartCount >= maxPartCount;
      if (full) {
        LOG.debug("PartCountLimitedList has reached maximum part count {}", maxPartCount);
      }
      return full;
    }

    public void clear() {
      uploads.clear();
      currentPartCount = 0;
    }
  }

  /**
   * An in-memory class to hold the information required in directory recursive evaluation.
   */
  public static class PendingEvaluateDirectory {
    private final OmDirectoryInfo directoryInfo;
    private String dirTableKey;
    private String dirPath;
    private DirectoryList directoryList;
    private boolean firstEvaluate;

    public PendingEvaluateDirectory(OmDirectoryInfo dir, String dirTableKey, String dirPath, DirectoryList summary) {
      this.directoryInfo = dir;
      this.dirTableKey = dirTableKey;
      this.dirPath = dirPath;
      this.directoryList = summary;
      this.firstEvaluate = true;
    }

    public String getDirTableKey() {
      return dirTableKey;
    }

    public void setDirTableKey(String tableKey) {
      dirTableKey = tableKey;
    }

    public String getDirPath() {
      return dirPath;
    }

    public OmDirectoryInfo getDirectoryInfo() {
      return directoryInfo;
    }

    public DirectoryList getSubDirSummary() {
      return directoryList;
    }

    public void setDirectoryList(DirectoryList summary) {
      directoryList = summary;
    }

    public boolean isFirstEvaluate() {
      return firstEvaluate;
    }

    public void setFirstEvaluate(boolean firstEvaluate) {
      this.firstEvaluate = firstEvaluate;
    }
  }

  /**
   * An in-memory class to hold a directory list.
   */
  public static class DirectoryList {
    private final List<OmDirectoryInfo> subDirList;
    private final List<String> subDirKeyList;
    private final List<String> subDirKeyPathList;
    private int subDirCount;
    private boolean allResolvedPrefix = true;

    public DirectoryList() {
      this.subDirList = new ArrayList<>();
      this.subDirKeyList = new ArrayList<>();
      this.subDirKeyPathList = new ArrayList<>();
      this.subDirCount = 0;
    }

    public int getSubDirCount() {
      return subDirCount;
    }

    public List<OmDirectoryInfo> getSubDirList() {
      return subDirList;
    }

    public List<String> getSubDirKeyList() {
      return subDirKeyList;
    }

    public void addSubDir(String key, OmDirectoryInfo dir, String dirPath) {
      subDirKeyList.add(key);
      subDirList.add(dir);
      subDirKeyPathList.add(dirPath);
      subDirCount++;
    }

    public OmDirectoryInfo getLastSubDir() {
      return subDirCount > 0 ? subDirList.get(subDirCount - 1) : null;
    }

    public String getLastSubDirKey() {
      return subDirCount > 0 ? subDirKeyList.get(subDirCount - 1) : null;
    }

    public String getLastSubDirPath() {
      return subDirCount > 0 ? subDirKeyPathList.get(subDirCount - 1) : null;
    }

    public boolean isAllResolvedPrefix() {
      return allResolvedPrefix;
    }

    public void setNotAllResolvedPrefix() {
      this.allResolvedPrefix = false;
    }

    public boolean isEmpty() {
      return subDirCount == 0;
    }

    @Override
    public String toString() {
      return "DirectoryList { " +
          "subDirList = " + subDirList +
          ", subDirKeyList = " + subDirKeyList +
          ", subDirKeyPathList = " + subDirKeyPathList +
          ", subDirCount = " + subDirCount +
          ", allResolvedPrefix = " + allResolvedPrefix +
          '}';
    }
  }

  /**
   * An in-memory class to hold a rule list, together with the resolved prefix's directory list.
   */
  public static class RuleListWithDirectoryList {
    private DirectoryList dirList;
    private final List<OmLCRule> ruleList;
    private String consolidatedPrefix;

    public RuleListWithDirectoryList() {
      this.ruleList = new ArrayList<>();
      this.dirList = new DirectoryList();
    }

    public RuleListWithDirectoryList(List<OmLCRule> ruleList, DirectoryList dirList, String prefix) {
      this.ruleList = ruleList;
      this.dirList = dirList;
      this.consolidatedPrefix = prefix;
    }

    public List<OmLCRule> getRuleList() {
      return ruleList;
    }

    public DirectoryList getDirList() {
      return dirList;
    }

    public void addRule(OmLCRule rule) {
      this.ruleList.add(rule);
    }

    public void setDirList(DirectoryList dirList) {
      this.dirList = dirList;
    }

    public String getConsolidatedPrefix() {
      return consolidatedPrefix;
    }

    public void setConsolidatedPrefix(String consolidatedPrefix) {
      this.consolidatedPrefix = consolidatedPrefix;
    }

    public boolean isEmpty() {
      return dirList.isEmpty() && ruleList.isEmpty() && consolidatedPrefix == null;
    }

    @Override
    public String toString() {
      return "RuleListWithDirectoryList { " +
          "dirList = " + dirList +
          ", ruleList = " + ruleList +
          ", consolidatedPrefix = '" + consolidatedPrefix + '\'' +
          '}';
    }
  }

  /**
   * Orders of RuleListWithDirectoryList.
   */
  public static class RuleListWithDirectoryListOrder implements Comparator<RuleListWithDirectoryList> {

    public static final Comparator<RuleListWithDirectoryList> INSTANCE =
        new RuleListWithDirectoryListOrder();

    @Override
    public int compare(RuleListWithDirectoryList o1, RuleListWithDirectoryList o2) {
      List<OmDirectoryInfo> dirList1 = o1.getDirList().getSubDirList();
      List<OmDirectoryInfo> dirList2 = o2.getDirList().getSubDirList();

      // find their shared parent directory
      OmDirectoryInfo parent = null;
      for (int i = dirList1.size() - 1; i >= 0; i--) {
        long objID = dirList1.get(i).getObjectID();
        for (int j = dirList2.size() - 1; j >= 0; j--) {
          if (dirList2.get(j).getObjectID() == objID) {
            parent = dirList2.get(j);
            break;
          }
        }
        if (parent != null) {
          break;
        }
      }
      if (parent == null) {
        // e.g,  dir1 and dir2, dir2 should ahead of dir2, given the depth-first and dir's RocksDB key order
        return dirList2.get(0).getName().compareTo(dirList1.get(0).getName());
      } else {
        long parentID = parent.getObjectID();
        OmDirectoryInfo dir1 = dirList1.stream().filter(dir -> dir.getParentObjectID() == parentID)
            .collect(Collectors.toList()).get(0);
        OmDirectoryInfo dir2 = dirList2.stream().filter(dir -> dir.getParentObjectID() == parentID)
            .collect(Collectors.toList()).get(0);
        return dir2.getName().compareTo(dir1.getName());
      }
    }
  }

  /**
   * An in-memory stack with a maximum size. This class is not thread safe.
   */
  public static class LimitedSizeStack {
    private final Deque<PendingEvaluateDirectory> stack;
    private final long maxSize;

    public LimitedSizeStack(long maxSize) {
      this.maxSize = maxSize;
      this.stack = new ArrayDeque<>();
    }

    public boolean isEmpty() {
      return stack.isEmpty();
    }

    public void push(PendingEvaluateDirectory e) throws CapacityFullException {
      if (stack.size() >= maxSize) {
        throw new CapacityFullException("LimitedSizeStack has reached maximum size " + maxSize);
      }
      stack.push(e);
    }

    public PendingEvaluateDirectory pop() {
      return stack.pop();
    }
  }

  /**
   * An exception which indicates the collection is full.
   */
  public static class CapacityFullException extends Exception {
    public CapacityFullException(String message) {
      super(message);
    }
  }

  public static void setTest(boolean test) {
    KeyLifecycleService.test = test;
  }

  public static void reSetConsolidatedRuleList() {
    consolidatedRuleList = null;
  }

  public static List<RuleListWithDirectoryList> getConsolidatedRuleList() {
    return consolidatedRuleList;
  }
}
