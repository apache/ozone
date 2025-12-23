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
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.OBJECT_STORE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.ozone.om.helpers.OmLCRule;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDirectoryRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyError;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetLifecycleServiceStatusResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyRequest;
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
  private long cachedDirMaxCount;
  private final AtomicBoolean suspended;
  private final AtomicBoolean isServiceEnabled;
  private KeyLifecycleServiceMetrics metrics;
  // A set of bucket name that have LifecycleActionTask scheduled
  private final ConcurrentHashMap<String, LifecycleActionTask> inFlight;
  private OMMetadataManager omMetadataManager;
  private int ratisByteLimit;
  private ClientId clientId = ClientId.randomId();
  private AtomicLong callId = new AtomicLong(0);
  private OzoneTrash ozoneTrash;
  private static List<FaultInjector> injectors;

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
    this.cachedDirMaxCount = conf.getLong(OZONE_KEY_LIFECYCLE_SERVICE_DELETE_CACHED_DIRECTORY_MAX_COUNT,
        OZONE_KEY_LIFECYCLE_SERVICE_DELETE_CACHED_DIRECTORY_MAX_COUNT_DEFAULT);
    this.suspended = new AtomicBoolean(false);
    this.metrics = KeyLifecycleServiceMetrics.create();
    this.isServiceEnabled = new AtomicBoolean(conf.getBoolean(OZONE_KEY_LIFECYCLE_SERVICE_ENABLED,
        OZONE_KEY_LIFECYCLE_SERVICE_ENABLED_DEFAULT));
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

        List<OmLCRule> originRuleList = policy.getRules();
        // remove disabled rules
        List<OmLCRule> ruleList = originRuleList.stream().filter(r -> r.isEnabled()).collect(Collectors.toList());

        // scan file or key table for evaluate rules against files or keys
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
         *        - For FSO bucket, as directory ModificationTime will not be updated when any of its child key/subdir
         *          changes, so remember to add the tailing slash "/" when configure prefix, otherwise the whole
         *          directory will be expired and deleted once its ModificationTime meats the condition.
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
          evaluateFSOBucket(volume, bucket, bucketKey, keyTable, ruleList, expiredKeyList, expiredDirList);
        } else {
          // use bucket name as key iterator prefix
          evaluateBucket(bucket, keyTable, ruleList, expiredKeyList);
        }

        if (expiredKeyList.isEmpty() && expiredDirList.isEmpty()) {
          LOG.info("No expired keys/dirs found/remained for bucket {}", bucketKey);
          onSuccess(bucketKey);
          return result;
        }

        LOG.info("{} expired keys and {} expired dirs found and remained for bucket {}",
            expiredKeyList.size(), expiredDirList.size(), bucketKey);

        // If trash is enabled, move files to trash, instead of send delete requests.
        // OBS bucket doesn't support trash.
        if (bucket.getBucketLayout() == OBJECT_STORE) {
          sendDeleteKeysRequestAndClearList(bucket.getVolumeName(), bucket.getBucketName(), expiredKeyList, false);
        } else {
          // handle keys first, then directories
          handleAndClearFullList(bucket, expiredKeyList, false);
          handleAndClearFullList(bucket, expiredDirList, true);
        }
        onSuccess(bucketKey);
      }

      // By design, no one cares about the results of this call back.
      return result;
    }

    @SuppressWarnings("checkstyle:parameternumber")
    private void evaluateFSOBucket(OmVolumeArgs volume, OmBucketInfo bucket, String bucketKey,
        Table<String, OmKeyInfo> keyTable, List<OmLCRule> ruleList,
        LimitedExpiredObjectList expiredKeyList, LimitedExpiredObjectList expiredDirList) {
      List<OmLCRule> prefixStartsWithTrashRuleList =
          ruleList.stream().filter(r -> r.isPrefixEnable() && r.getEffectivePrefix().startsWith(
              TRASH_PREFIX + OzoneConsts.OM_KEY_PREFIX)).collect(Collectors.toList());
      List<OmLCRule> prefixRuleList =
          ruleList.stream().filter(r -> r.isPrefixEnable()).collect(Collectors.toList());
      // r.isPrefixEnable() == false means empty filter
      List<OmLCRule> noPrefixRuleList =
          ruleList.stream().filter(r -> !r.isPrefixEnable()).collect(Collectors.toList());

      prefixRuleList.removeAll(prefixStartsWithTrashRuleList);
      prefixStartsWithTrashRuleList.stream().forEach(
          r -> LOG.info("Skip rule {} as its prefix starts with {}", r, TRASH_PREFIX + OzoneConsts.OM_KEY_PREFIX));

      for (OmLCRule rule : prefixRuleList) {
        // find KeyInfo of each directory for prefix
        List<OmDirectoryInfo> dirList;
        try {
          dirList = getDirList(volume, bucket, rule.getEffectivePrefix(), bucketKey);
        } catch (IOException e) {
          LOG.warn("Skip rule {} as its prefix doesn't have all directory exist", rule);
          // skip this rule if some directory doesn't exist for this rule's prefix
          continue;
        }
        StringBuffer lastDirPath = new StringBuffer();
        OmDirectoryInfo lastDir = null;
        if (!dirList.isEmpty()) {
          lastDir = dirList.get(dirList.size() - 1);
          for (int i = 0; i < dirList.size(); i++) {
            lastDirPath.append(dirList.get(i).getName());
            if (i != dirList.size() - 1) {
              lastDirPath.append(OM_KEY_PREFIX);
            }
          }
          if (lastDirPath.toString().startsWith(TRASH_PREFIX)) {
            LOG.info("Skip evaluate trash directory {}", lastDirPath);
          } else {
            evaluateKeyAndDirTable(bucket, volume.getObjectID(), keyTable, lastDirPath.toString(), lastDir,
                Arrays.asList(rule), expiredKeyList, expiredDirList);
          }

          if (!rule.getEffectivePrefix().endsWith(OM_KEY_PREFIX)) {
            // if the prefix doesn't end with "/", then also search and evaluate the directory itself
            // for example, "dir1/dir2" matches both directory "dir1/dir2" and "dir1/dir22"
            // or "dir1" matches both directory "dir1" and "dir11"
            long objID;
            String objPrefix;
            String objPath;
            if (dirList.size() > 1) {
              OmDirectoryInfo secondLastDir = dirList.get(dirList.size() - 2);
              objID = secondLastDir.getObjectID();
              objPrefix = OM_KEY_PREFIX + volume.getObjectID() + OM_KEY_PREFIX + bucket.getObjectID() +
                  OM_KEY_PREFIX + secondLastDir.getObjectID();
              StringBuffer secondLastDirPath = new StringBuffer();
              for (int i = 0; i < dirList.size() - 1; i++) {
                secondLastDirPath.append(dirList.get(i).getName());
                if (i != dirList.size() - 2) {
                  secondLastDirPath.append(OM_KEY_PREFIX);
                }
              }
              objPath = secondLastDirPath.toString();
            } else {
              objID = bucket.getObjectID();
              objPrefix = OM_KEY_PREFIX + volume.getObjectID() + OM_KEY_PREFIX + bucket.getObjectID() +
                  OM_KEY_PREFIX + bucket.getObjectID();
              objPath = "";
            }
            try {
              SubDirectorySummary subDirSummary = getSubDirectory(objID, objPrefix, omMetadataManager);
              for (OmDirectoryInfo subDir : subDirSummary.getSubDirList()) {
                String subDirPath = objPath.isEmpty() ? subDir.getName() : objPath + OM_KEY_PREFIX + subDir.getName();
                if (!subDir.getName().equals(TRASH_PREFIX) && subDirPath.startsWith(rule.getEffectivePrefix()) &&
                    (lastDir == null || subDir.getObjectID() != lastDir.getObjectID())) {
                  evaluateKeyAndDirTable(bucket, volume.getObjectID(), keyTable, subDirPath, subDir,
                      Arrays.asList(rule), expiredKeyList, expiredDirList);
                }
              }
            } catch (IOException e) {
              // log failure and continue the process
              LOG.warn("Failed to get sub directories of {} under {}/{}", objPrefix,
                  bucket.getVolumeName(), bucket.getBucketName(), e);
              return;
            }
          }
        } else {
          evaluateKeyAndDirTable(bucket, volume.getObjectID(), keyTable, "", null,
              Arrays.asList(rule), expiredKeyList, expiredDirList);
        }
      }

      if (!noPrefixRuleList.isEmpty()) {
        evaluateKeyAndDirTable(bucket, volume.getObjectID(), keyTable, "", null,
            noPrefixRuleList, expiredKeyList, expiredDirList);
      }
    }

    @SuppressWarnings({"checkstyle:parameternumber", "checkstyle:MethodLength"})
    private void evaluateKeyAndDirTable(OmBucketInfo bucket, long volumeObjId, Table<String, OmKeyInfo> keyTable,
        String directoryPath, @Nullable OmDirectoryInfo dir, List<OmLCRule> ruleList, LimitedExpiredObjectList keyList,
        LimitedExpiredObjectList dirList) {
      String volumeName = bucket.getVolumeName();
      String bucketName = bucket.getBucketName();
      LimitedSizeStack stack = new LimitedSizeStack(cachedDirMaxCount);
      try {
        if (dir != null) {
          stack.push(new PendingEvaluateDirectory(dir, directoryPath, null));
        } else {
          // put a placeholder PendingEvaluateDirectory to stack for bucket
          stack.push(new PendingEvaluateDirectory(null, "", null));
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

        // use current directory's object ID to iterate the keys and directories under it
        String prefix =
            OM_KEY_PREFIX + volumeObjId + OM_KEY_PREFIX + bucket.getObjectID() + OM_KEY_PREFIX + currentDirObjID;
        LOG.debug("Prefix {} for {}/{}", prefix, bucket.getVolumeName(), bucket.getBucketName());

        // get direct sub directories
        SubDirectorySummary subDirSummary = item.getSubDirSummary();
        boolean newSubDirPushed = false;
        long deletedDirCount = 0;
        if (subDirSummary == null) {
          try {
            subDirSummary = getSubDirectory(currentDirObjID, prefix, omMetadataManager);
          } catch (IOException e) {
            // log failure, continue to process other directories in stack
            LOG.warn("Failed to get sub directories of {} under {}/{}", currentDirPath, volumeName, bucketName, e);
            continue;
          }

          // filter sub directory list
          if (!subDirSummary.getSubDirList().isEmpty()) {
            Iterator<OmDirectoryInfo> iterator = subDirSummary.getSubDirList().iterator();
            while (iterator.hasNext()) {
              OmDirectoryInfo subDir = iterator.next();
              String subDirPath = currentDirPath.isEmpty() ? subDir.getName() :
                  currentDirPath + OM_KEY_PREFIX + subDir.getName();
              if (subDirPath.startsWith(TRASH_PREFIX)) {
                iterator.remove();
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
            item.setSubDirSummary(subDirSummary);
            try {
              stack.push(item);
            } catch (CapacityFullException e) {
              LOG.warn("Abort evaluate {}/{} at {}", volumeName, bucketName, currentDirPath, e);
              return;
            }

            // depth first evaluation, push subDirs into stack
            for (OmDirectoryInfo subDir : subDirSummary.getSubDirList()) {
              String subDirPath = currentDirPath.isEmpty() ? subDir.getName() :
                  currentDirPath + OM_KEY_PREFIX + subDir.getName();
              try {
                stack.push(new PendingEvaluateDirectory(subDir, subDirPath, null));
              } catch (CapacityFullException e) {
                LOG.warn("Abort evaluate {}/{} at {}", volumeName, bucketName, subDirPath, e);
                return;
              }
            }
            newSubDirPushed = true;
          }
        } else {
          // this item is a parent directory, check how many sub directories are deleted.
          for (OmDirectoryInfo subDir : subDirSummary.getSubDirList()) {
            if (deletedDirSet.remove(subDir.getObjectID())) {
              deletedDirCount++;
            }
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
                  handleAndClearFullList(bucket, keyList, false);
                }
                keyList.add(keyPath, key.getReplicatedSize(), key.getUpdateID());
                numKeysExpired++;
                break;
              }
            }
          }
        }

        try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> keyTblItr =
                 keyTable.iterator(prefix)) {
          while (keyTblItr.hasNext()) {
            Table.KeyValue<String, OmKeyInfo> keyValue = keyTblItr.next();
            OmKeyInfo key = keyValue.getValue();
            String keyPath = currentDirPath.isEmpty() ? key.getKeyName() :
                currentDirPath + OM_KEY_PREFIX + key.getKeyName();
            numKeyIterated++;
            if (deletedKeySetInCache.remove(keyValue.getKey()) || keySetInCache.remove(keyValue.getKey())) {
              continue;
            }
            numKeysUnderDir++;
            for (OmLCRule rule : ruleList) {
              if (key.getParentObjectID() == currentDirObjID && rule.match(key, keyPath)) {
                // mark key as expired, check next key
                if (keyList.isFull()) {
                  // if keyList is full, send delete request for pending deletion keys
                  handleAndClearFullList(bucket, keyList, false);
                }
                keyList.add(keyPath, key.getReplicatedSize(), key.getUpdateID());
                numKeysExpired++;
              }
            }
          }
        } catch (IOException e) {
          // log failure and continue the process other directories in stack
          LOG.warn("Failed to iterate keyTable for bucket {}/{}", volumeName, bucketName, e);
          continue;
        }

        // if this directory is empty or all files/subDirs are expired, evaluate itself
        if ((numKeysUnderDir == 0 && subDirSummary.getSubDirCount() == 0) ||
            (numKeysUnderDir == numKeysExpired && deletedDirCount == subDirSummary.getSubDirCount())) {
          for (OmLCRule rule : ruleList) {
            String path = (rule.getEffectivePrefix() != null && rule.getEffectivePrefix().endsWith(OM_KEY_PREFIX)) ?
                currentDirPath + OM_KEY_PREFIX : currentDirPath;
            if (currentDir != null && rule.match(currentDir, path)) {
              if (dirList.isFull()) {
                // if expiredDirList is full, send delete request for pending deletion directories
                handleAndClearFullList(bucket, dirList, true);
              }
              dirList.add(currentDirPath, 0, currentDir.getUpdateID());
              deletedDirSet.add(currentDir.getObjectID());
            }
          }
        }
      }
    }

    private SubDirectorySummary getSubDirectory(long dirObjID, String prefix, OMMetadataManager metaMgr)
        throws IOException {
      SubDirectorySummary subDirList = new SubDirectorySummary();

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
          subDirList.addSubDir(cacheOmDirInfo);
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
            subDirList.addSubDir(dir);
          }
        }
      }
      return subDirList;
    }

    private void evaluateBucket(OmBucketInfo bucketInfo,
        Table<String, OmKeyInfo> keyTable, List<OmLCRule> ruleList, LimitedExpiredObjectList expiredKeyList) {
      String volumeName = bucketInfo.getVolumeName();
      String bucketName = bucketInfo.getBucketName();

      if (bucketInfo.getBucketLayout() == BucketLayout.LEGACY) {
        List<OmLCRule> prefixStartsWithTrashRuleList =
            ruleList.stream().filter(r -> r.isPrefixEnable() && r.getEffectivePrefix().startsWith(
                TRASH_PREFIX + OzoneConsts.OM_KEY_PREFIX)).collect(Collectors.toList());
        ruleList.removeAll(prefixStartsWithTrashRuleList);
        prefixStartsWithTrashRuleList.stream().forEach(
            r -> LOG.info("Skip rule {} as its prefix starts with {}", r, TRASH_PREFIX + OzoneConsts.OM_KEY_PREFIX));
      }

      // use bucket name as key iterator prefix
      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> keyTblItr =
               keyTable.iterator(omMetadataManager.getBucketKey(volumeName, bucketName))) {
        while (keyTblItr.hasNext()) {
          if (!shouldRun()) {
            LOG.info("KeyLifecycleService is suspended or disabled. " +
                "Stopping LifecycleActionTask for bucket {}.", bucketName);
            return;
          }
          Table.KeyValue<String, OmKeyInfo> keyValue = keyTblItr.next();
          OmKeyInfo key = keyValue.getValue();
          numKeyIterated++;
          if (bucketInfo.getBucketLayout() == BucketLayout.LEGACY &&
              key.getKeyName().startsWith(TRASH_PREFIX + OzoneConsts.OM_KEY_PREFIX)) {
            LOG.info("Skip evaluate trash directory {} and all its child files and sub directories", TRASH_PREFIX);
            continue;
          }
          for (OmLCRule rule : ruleList) {
            if (rule.match(key)) {
              // mark key as expired, check next key
              if (expiredKeyList.isFull()) {
                // if expiredKeyList is full, send delete/rename request for expired keys
                handleAndClearFullList(bucketInfo, expiredKeyList, false);
              }
              expiredKeyList.add(key.getKeyName(), key.getReplicatedSize(), key.getUpdateID());
              break;
            }
          }
        }
      } catch (IOException e) {
        // log failure and continue the process to delete/move files already identified in this run
        LOG.warn("Failed to iterate through bucket {}/{}", volumeName, bucketName, e);
      }
    }

    /**
     * If prefix is /dir1/dir2, but dir1 doesn't exist, then it will return exception.
     * If prefix is /dir1/dir2, but dir2 doesn't exist, then it will return a list with dir1 only.
     * If prefix is /dir1/dir2, although dir1 exists, but get(dir1) failed with IOException,
     * then it will return exception too.
     */
    private List<OmDirectoryInfo> getDirList(OmVolumeArgs volume, OmBucketInfo bucket, String prefix, String bucketKey)
        throws IOException {
      // find KeyInfo of each directory for prefix
      java.nio.file.Path keyPath = Paths.get(prefix);
      Iterator<java.nio.file.Path> elements = keyPath.iterator();
      long lastKnownParentId = bucket.getObjectID();
      List<OmDirectoryInfo> dirList = new ArrayList<>();
      while (elements.hasNext()) {
        String dirName = elements.next().toString();
        String dbDirName = omMetadataManager.getOzonePathKey(
            volume.getObjectID(), bucket.getObjectID(),
            lastKnownParentId, dirName);
        try {
          OmDirectoryInfo omDirInfo = omMetadataManager.getDirectoryTable().get(dbDirName);
          // It's OK there is no directory for the last part of prefix, which is probably not a directory
          if (omDirInfo == null) {
            if (elements.hasNext()) {
              throw new OMException("Directory " + dbDirName + " does not exist for bucket " + bucketKey,
                  OMException.ResultCodes.DIRECTORY_NOT_FOUND);
            }
          } else {
            dirList.add(omDirInfo);
            lastKnownParentId = omDirInfo.getObjectID();
          }
        } catch (IOException e) {
          LOG.warn("Failed to get directory {} from bucket {}", dbDirName, bucketKey, e);
          throw new IOException("Failed to get directory " + dbDirName + " from bucket " + bucketKey);
        }
      }
      return dirList;
    }

    private void onFailure(String bucketName) {
      inFlight.remove(bucketName);
      metrics.incrNumFailureTask();
      metrics.incNumKeyIterated(numKeyIterated);
      metrics.incNumDirIterated(numDirIterated);
    }

    private void onSuccess(String bucketName) {
      inFlight.remove(bucketName);
      metrics.incrNumSuccessTask();
      long timeSpent = Time.monotonicNow() - taskStartTime;
      metrics.incTaskLatencyMs(timeSpent);
      metrics.incNumKeyIterated(numKeyIterated);
      metrics.incNumDirIterated(numDirIterated);
      LOG.info("Spend {} ms on bucket {} to iterate {} keys and {} dirs, deleted {} keys with {} bytes, " +
          "and {} dirs, renamed {} keys with {} bytes, and {} dirs to trash", timeSpent, bucketName, numKeyIterated,
          numDirIterated, numKeyDeleted, sizeKeyDeleted, numDirDeleted, numKeyRenamed, sizeKeyRenamed, numDirRenamed);
    }

    private void handleAndClearFullList(OmBucketInfo bucket, LimitedExpiredObjectList keysList, boolean dir) {
      if (bucket.getBucketLayout() != OBJECT_STORE && ozoneTrash != null) {
        moveToTrash(bucket, keysList, dir);
      } else {
        sendDeleteKeysRequestAndClearList(bucket.getVolumeName(), bucket.getBucketName(), keysList, dir);
      }
    }

    private void sendDeleteKeysRequestAndClearList(String volume, String bucket,
        LimitedExpiredObjectList keysList, boolean dir) {
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
          DeleteKeysRequest deleteKeysRequest = DeleteKeysRequest.newBuilder().setDeleteKeys(deleteKeyArgs).build();
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
  public void setOzoneTrash(OzoneTrash ozoneTrash) {
    this.ozoneTrash = ozoneTrash;
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
   * An in-memory class to hold the information required in directory recursive evaluation.
   */
  public static class PendingEvaluateDirectory {
    private final OmDirectoryInfo directoryInfo;
    private final String dirPath;
    private SubDirectorySummary subDirSummary;

    public PendingEvaluateDirectory(OmDirectoryInfo dir, String path, SubDirectorySummary summary) {
      this.directoryInfo = dir;
      this.dirPath = path;
      this.subDirSummary = summary;
    }

    public String getDirPath() {
      return dirPath;
    }

    public OmDirectoryInfo getDirectoryInfo() {
      return directoryInfo;
    }

    public SubDirectorySummary getSubDirSummary() {
      return subDirSummary;
    }

    public void setSubDirSummary(SubDirectorySummary summary) {
      subDirSummary = summary;
    }
  }

  /**
   * An in-memory class to hold sub directory summary.
   */
  public static class SubDirectorySummary {
    private final List<OmDirectoryInfo> subDirList;
    private long subDirCount;

    public SubDirectorySummary() {
      this.subDirList = new ArrayList<>();
      this.subDirCount = 0;
    }

    public long getSubDirCount() {
      return subDirCount;
    }

    public List<OmDirectoryInfo> getSubDirList() {
      return subDirList;
    }

    public void addSubDir(OmDirectoryInfo dir) {
      subDirList.add(dir);
      subDirCount++;
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
}
