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

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_DELETE_BATCH_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_DELETE_BATCH_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_ENABLED_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OzoneTrash;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmLCRule;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
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
  //TODO: honor this parameter in next patch
  private int keyLimitPerIterator;
  private int keyLimitPerRequest;
  private final AtomicBoolean suspended;
  private KeyLifecycleServiceMetrics metrics;
  private boolean isServiceEnabled;
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
    this.keyLimitPerIterator = conf.getInt(OZONE_KEY_LIFECYCLE_SERVICE_DELETE_BATCH_SIZE,
        OZONE_KEY_LIFECYCLE_SERVICE_DELETE_BATCH_SIZE_DEFAULT);
    Preconditions.checkArgument(keyLimitPerIterator >= 0,
        OZONE_KEY_LIFECYCLE_SERVICE_DELETE_BATCH_SIZE + " cannot be negative.");
    this.keyLimitPerRequest = 100000;
    this.suspended = new AtomicBoolean(false);
    this.metrics = KeyLifecycleServiceMetrics.create();
    this.isServiceEnabled = conf.getBoolean(OZONE_KEY_LIFECYCLE_SERVICE_ENABLED,
        OZONE_KEY_LIFECYCLE_SERVICE_ENABLED_DEFAULT);
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

    List<OmLifecycleConfiguration> lifecycleConfigurationList =
        omMetadataManager.listLifecycleConfigurations();
    for (OmLifecycleConfiguration lifecycleConfiguration : lifecycleConfigurationList) {
      if (lifecycleConfiguration.getRules().stream().anyMatch(r -> r.isEnabled())) {
        LifecycleActionTask task = new LifecycleActionTask(lifecycleConfiguration);
        if (this.inFlight.putIfAbsent(lifecycleConfiguration.getFormattedKey(), task) == null) {
          queue.add(task);
          LOG.info("LifecycleActionTask of {} is scheduled", lifecycleConfiguration.getFormattedKey());
        } else {
          metrics.incrNumSkippedTask();
          LOG.info("LifecycleActionTask of {} is already running", lifecycleConfiguration.getFormattedKey());
        }
      } else {
        LOG.info("LifecycleConfiguration of {} is not enabled", lifecycleConfiguration.getFormattedKey());
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
    return isServiceEnabled && !suspended.get() && getOzoneManager().isLeaderReady();
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
  @VisibleForTesting
  public void suspend() {
    suspended.set(true);
  }

  /**
   * Resume the service if suspended.
   */
  @VisibleForTesting
  public void resume() {
    suspended.set(false);
  }

  @Override
  public void shutdown() {
    super.shutdown();
    KeyLifecycleServiceMetrics.unregister();
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
      String bucketName = policy.getFormattedKey();
      // Check if this is the Leader OM. If not leader, no need to execute this task.
      if (shouldRun()) {
        LOG.info("Running LifecycleActionTask {}", bucketName);
        taskStartTime = System.currentTimeMillis();
        OmBucketInfo bucket;
        try {
          if (getInjector(0) != null) {
            getInjector(0).pause();
          }
          bucket = omMetadataManager.getBucketTable().get(bucketName);
          if (bucket == null) {
            LOG.warn("Bucket {} cannot be found, might be deleted during this task's execution", bucketName);
            onFailure(bucketName);
            return result;
          }
        } catch (IOException e) {
          LOG.warn("Failed to get Bucket {}", bucketName, e);
          onFailure(bucketName);
          return result;
        }

        List<OmLCRule> originRuleList = policy.getRules();
        // remove disabled rules
        List<OmLCRule> ruleList = originRuleList.stream().filter(r -> r.isEnabled()).collect(Collectors.toList());

        boolean tagEnabled = ruleList.stream().anyMatch(r -> r.isTagEnable());
        boolean prefixEnabled = ruleList.stream().anyMatch(r -> r.isPrefixEnable());
        // TODO, set a rule with tag on FSO bucket should fail at creation time.
        if (bucket.getBucketLayout() == BucketLayout.FILE_SYSTEM_OPTIMIZED && tagEnabled) {
          LOG.info("Fail the task as rule with tag is not supported on FSO bucket {}", bucketName);
          onFailure(bucketName);
          return result;
        }

        // scan file or key table for evaluate rules against files or keys
        List<String> expiredKeyNameList = new ArrayList<>();
        List<String> expiredDirNameList = new ArrayList<>();
        List<Long> expiredKeyUpdateIDList = new ArrayList<>();
        List<Long> expiredDirUpdateIDList = new ArrayList<>();
        // TODO: limit expired key size in each iterator
        Table<String, OmKeyInfo> keyTable = omMetadataManager.getKeyTable(bucket.getBucketLayout());
        /**
         * Filter treatment.
         * ""  - all objects
         * "/" - if it's OBS/Legacy, means keys starting with "/"; If it's FSO, means root directory
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
              onFailure(bucketName);
              return result;
            }
          } catch (IOException e) {
            LOG.warn("Failed to get volume {}", bucket.getVolumeName(), e);
            onFailure(bucketName);
            return result;
          }
          evaluateFSOBucket(volume, bucket, bucketName, keyTable, ruleList,
              expiredKeyNameList, expiredKeyUpdateIDList, expiredDirNameList, expiredDirUpdateIDList);
        } else if (tagEnabled && !prefixEnabled) {
          // use bucket name as key iterator prefix
          evaluateBucket(bucketName, keyTable, ruleList, expiredKeyNameList, expiredKeyUpdateIDList);
        } else {
          // no tag - a prefix filtered bucket key iterator for each rule
          for (OmLCRule rule : ruleList) {
            evaluateBucket(bucketName, keyTable, rule, expiredKeyNameList, expiredKeyUpdateIDList);
          }
        }

        if (expiredKeyNameList.isEmpty() && expiredDirNameList.isEmpty()) {
          LOG.info("No expired keys/dirs found for bucket {}", bucketName);
          onSuccess(bucketName);
          return result;
        }

        LOG.info("{} expired keys and {} expired dirs found for bucket {}",
            expiredKeyNameList.size(), expiredDirNameList.size(), bucketName);

        // If trash is enabled, move files to trash, instead of send delete requests.
        // OBS bucket doesn't support trash.
        if (bucket.getBucketLayout() == BucketLayout.OBJECT_STORE) {
          sendDeleteKeysRequest(bucket.getVolumeName(), bucket.getBucketName(),
              expiredKeyNameList, expiredKeyUpdateIDList, false);
        } else if (ozoneTrash != null) {
          // move keys to trash
          // TODO: add unit test in next patch
          moveKeysToTrash(expiredKeyNameList);
        } else {
          sendDeleteKeysRequest(bucket.getVolumeName(), bucket.getBucketName(), expiredKeyNameList,
              expiredKeyUpdateIDList, false);
          if (!expiredDirNameList.isEmpty()) {
            sendDeleteKeysRequest(bucket.getVolumeName(), bucket.getBucketName(), expiredDirNameList,
                expiredDirUpdateIDList, true);
          }
        }
        onSuccess(bucketName);
      }

      // By design, no one cares about the results of this call back.
      return result;
    }

    private void evaluateBucket(String bucketName, Table<String, OmKeyInfo> keyTable,
        OmLCRule rule, List<String> expiredKeyList, List<Long> expiredKeyUpdateIDList) {
      // create a prefix filtered bucket key iterator
      String prefix = bucketName;
      if (rule.isPrefixEnable()) {
        prefix = bucketName + OzoneConsts.OM_KEY_PREFIX + rule.getPrefix();
      }
      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
               keyTblItr = keyTable.iterator(prefix)) {
        while (keyTblItr.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> keyValue = keyTblItr.next();
          OmKeyInfo key = keyValue.getValue();
          numKeyIterated++;
          if (rule.verify(key)) {
            // add key for pending deletion, check next key
            expiredKeyList.add(key.getKeyName());
            expiredKeyUpdateIDList.add(key.getUpdateID());
            sizeKeyDeleted += key.getReplicatedSize();
          }
        }
      } catch (IOException e) {
        // log failure and continue the process to delete/move files already identified in this run
        LOG.warn("Failed to iterate through bucket {}", bucketName, e);
      }
    }

    @SuppressWarnings("checkstyle:parameternumber")
    private void evaluateFSOBucket(OmVolumeArgs volume, OmBucketInfo bucket, String bucketName,
                                   Table<String, OmKeyInfo> keyTable, List<OmLCRule> ruleList,
                                   List<String> expiredKeyList, List<Long> expiredKeyUpdateIDList,
                                   List<String> expiredDirList, List<Long> expiredDirUpdateIDList) {
      List<OmLCRule> directoryStylePrefixRuleList =
          ruleList.stream().filter(r -> r.isDirectoryStylePrefix()).collect(Collectors.toList());
      List<OmLCRule> nonDirectoryStylePrefixRuleList =
          ruleList.stream().filter(r -> r.isPrefixEnable() && !r.isDirectoryStylePrefix()).collect(Collectors.toList());
      // r.isPrefixEnable() == false means empty filter
      List<OmLCRule> noPrefixRuleList =
          ruleList.stream().filter(r -> !r.isPrefixEnable()).collect(Collectors.toList());

      Table<String, OmDirectoryInfo> directoryInfoTable = omMetadataManager.getDirectoryTable();
      for (OmLCRule rule : directoryStylePrefixRuleList) {
        // find KeyInfo of each directory for prefix
        List<OmDirectoryInfo> dirList;
        try {
          dirList = getDirList(volume, bucket, rule.getPrefix(), bucketName);
        } catch (IOException e) {
          LOG.warn("Skip rule {} as its prefix doesn't have all directory exist", rule);
          // skip this rule if some directory doesn't exist for this rule's prefix
          continue;
        }
        // use last directory's object ID to iterate the keys
        String prefix = OzoneConsts.OM_KEY_PREFIX + volume.getObjectID() +
            OzoneConsts.OM_KEY_PREFIX + bucket.getObjectID() + OzoneConsts.OM_KEY_PREFIX;
        StringBuffer directoryPath = new StringBuffer();
        if (!dirList.isEmpty()) {
          prefix += dirList.get(dirList.size() - 1).getObjectID();
          for (OmDirectoryInfo dir : dirList) {
            directoryPath.append(dir.getName()).append(OzoneConsts.OM_KEY_PREFIX);
          }
          if (directoryPath.toString().equals(rule.getCanonicalPrefix() + OzoneConsts.OM_KEY_PREFIX)) {
            expiredDirList.add(directoryPath.toString());
            expiredDirUpdateIDList.add(dirList.get(dirList.size() - 1).getUpdateID());
          }
        }

        LOG.info("Prefix {} for {}", prefix, bucketName);
        evaluateKeyTable(keyTable, prefix, directoryPath.toString(), rule, expiredKeyList,
            expiredKeyUpdateIDList, bucketName);
        evaluateDirTable(directoryInfoTable, prefix, directoryPath.toString(), rule,
            expiredDirList, expiredDirUpdateIDList, bucketName);
      }

      for (OmLCRule rule : nonDirectoryStylePrefixRuleList) {
        // find the directory for the prefix, it may not exist
        OmDirectoryInfo dirInfo = getDirectory(volume, bucket, rule.getPrefix(), bucketName);
        String prefix = OzoneConsts.OM_KEY_PREFIX + volume.getObjectID() +
            OzoneConsts.OM_KEY_PREFIX + bucket.getObjectID() + OzoneConsts.OM_KEY_PREFIX;
        if (dirInfo != null) {
          prefix += dirInfo.getObjectID();
          if (dirInfo.getName().equals(rule.getCanonicalPrefix())) {
            expiredDirList.add(dirInfo.getName());
            expiredDirUpdateIDList.add(dirInfo.getUpdateID());
          }
        }
        LOG.info("Prefix {} for {}", prefix, bucketName);
        evaluateKeyTable(keyTable, prefix, "", rule, expiredKeyList, expiredKeyUpdateIDList, bucketName);
        evaluateDirTable(directoryInfoTable, prefix, "", rule, expiredDirList, expiredDirUpdateIDList, bucketName);
      }

      if (!noPrefixRuleList.isEmpty()) {
        String prefix = OzoneConsts.OM_KEY_PREFIX + volume.getObjectID() +
            OzoneConsts.OM_KEY_PREFIX + bucket.getObjectID() + OzoneConsts.OM_KEY_PREFIX;
        LOG.info("prefix {} for {}", prefix, bucketName);
        // use bucket name as key iterator prefix
        try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> keyTblItr =
                 keyTable.iterator(prefix)) {
          while (keyTblItr.hasNext()) {
            Table.KeyValue<String, OmKeyInfo> keyValue = keyTblItr.next();
            OmKeyInfo key = keyValue.getValue();
            numKeyIterated++;
            for (OmLCRule rule : noPrefixRuleList) {
              if (rule.verify(key)) {
                // mark key as expired, check next key
                expiredKeyList.add(key.getKeyName());
                expiredKeyUpdateIDList.add(key.getUpdateID());
                sizeKeyDeleted += key.getReplicatedSize();
                break;
              }
            }
          }
        } catch (IOException e) {
          // log failure and continue the process to delete/move files already identified in this run
          LOG.warn("Failed to iterate keyTable for bucket {}", bucketName, e);
        }

        try (TableIterator<String, ? extends Table.KeyValue<String, OmDirectoryInfo>> dirTblItr =
                 directoryInfoTable.iterator(prefix)) {
          while (dirTblItr.hasNext()) {
            Table.KeyValue<String, OmDirectoryInfo> entry = dirTblItr.next();
            OmDirectoryInfo dir = entry.getValue();
            numDirIterated++;
            for (OmLCRule rule : noPrefixRuleList) {
              if (rule.verify(dir, dir.getPath())) {
                // mark key as expired, check next key
                expiredDirList.add(dir.getPath());
                expiredDirUpdateIDList.add(dir.getUpdateID());
                break;
              }
            }
          }
        } catch (IOException e) {
          // log failure and continue the process to delete/move files already identified in this run
          LOG.warn("Failed to iterate keyTable for bucket {}", bucketName, e);
        }
      }
    }

    private void evaluateKeyTable(Table<String, OmKeyInfo> keyTable, String prefix, String directoryPath,
        OmLCRule rule, List<String> keyList, List<Long> keyUpdateIDList, String bucketName) {
      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> keyTblItr =
               keyTable.iterator(prefix)) {
        while (keyTblItr.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> keyValue = keyTblItr.next();
          OmKeyInfo key = keyValue.getValue();
          String keyPath = directoryPath + key.getKeyName();
          numKeyIterated++;
          if (rule.verify(key, keyPath)) {
            // mark key as expired, check next key
            keyList.add(keyPath);
            keyUpdateIDList.add(key.getUpdateID());
            sizeKeyDeleted += key.getReplicatedSize();
          }
        }
      } catch (IOException e) {
        // log failure and continue the process to delete/move files already identified in this run
        LOG.warn("Failed to iterate keyTable for bucket {}", bucketName, e);
      }
    }

    private void evaluateDirTable(Table<String, OmDirectoryInfo> directoryInfoTable, String prefix,
        String directoryPath, OmLCRule rule, List<String> dirList, List<Long> dirUpdateIDList, String bucketName) {
      try (TableIterator<String, ? extends Table.KeyValue<String, OmDirectoryInfo>> dirTblItr =
               directoryInfoTable.iterator(prefix)) {
        while (dirTblItr.hasNext()) {
          Table.KeyValue<String, OmDirectoryInfo> entry = dirTblItr.next();
          OmDirectoryInfo dir = entry.getValue();
          String dirPath = directoryPath + dir.getName();
          numDirIterated++;
          if (rule.verify(dir, dirPath)) {
            // mark dir as expired, check next key
            dirList.add(dirPath);
            dirUpdateIDList.add(dir.getUpdateID());
          }
        }
      } catch (IOException e) {
        // log failure and continue the process to delete/move files already identified in this run
        LOG.warn("Failed to iterate directoryInfoTable for bucket {}", bucketName, e);
      }
    }

    private void evaluateBucket(String bucketName,
        Table<String, OmKeyInfo> keyTable, List<OmLCRule> ruleList,
        List<String> expiredKeyList, List<Long> expiredKeyUpdateIDList) {
      // use bucket name as key iterator prefix
      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> keyTblItr =
               keyTable.iterator(bucketName)) {
        while (keyTblItr.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> keyValue = keyTblItr.next();
          OmKeyInfo key = keyValue.getValue();
          numKeyIterated++;
          for (OmLCRule rule : ruleList) {
            if (rule.verify(key)) {
              // mark key as expired, check next key
              expiredKeyList.add(key.getKeyName());
              expiredKeyUpdateIDList.add(key.getUpdateID());
              sizeKeyDeleted += key.getReplicatedSize();
              break;
            }
          }
        }
      } catch (IOException e) {
        // log failure and continue the process to delete/move files already identified in this run
        LOG.warn("Failed to iterate through bucket {}", bucketName, e);
      }
    }

    private OmDirectoryInfo getDirectory(OmVolumeArgs volume, OmBucketInfo bucket, String prefix, String bucketName) {
      String dbDirName = omMetadataManager.getOzonePathKey(
          volume.getObjectID(), bucket.getObjectID(), bucket.getObjectID(), prefix);
      try {
        return omMetadataManager.getDirectoryTable().get(dbDirName);
      } catch (IOException e) {
        LOG.info("Failed to get directory object of {} for bucket {}", dbDirName, bucketName);
        return null;
      }
    }

    private List<OmDirectoryInfo> getDirList(OmVolumeArgs volume, OmBucketInfo bucket, String prefix, String bucketName)
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
              throw new IOException("Failed to get directory object of " + dbDirName + " for bucket " + bucketName);
            }
          } else {
            dirList.add(omDirInfo);
            lastKnownParentId = omDirInfo.getObjectID();
          }
        } catch (IOException e) {
          LOG.warn("Failed to get directory object of {} for bucket {}", dbDirName, bucketName);
          throw new IOException("Failed to get directory object for " + dbDirName + " for bucket " + bucketName);
        }
      }
      return dirList;
    }

    private void onFailure(String bucketName) {
      inFlight.remove(bucketName);
      metrics.incrNumFailureTask();
    }

    private void onSuccess(String bucketName) {
      inFlight.remove(bucketName);
      metrics.incrNumSuccessTask();
      long timeSpent = System.currentTimeMillis() - taskStartTime;
      metrics.incTaskLatencyMs(timeSpent);
      metrics.incNumKeyIterated(numKeyIterated);
      metrics.incNumDirIterated(numDirIterated);
      metrics.incrSizeKeyDeleted(sizeKeyDeleted);
      LOG.info("Spend {} ms on bucket {} to iterate {} keys and {} dirs, deleted {} keys with {} bytes, and {} dirs",
          timeSpent, bucketName, numKeyIterated, numDirIterated, numKeyDeleted, sizeKeyDeleted, numDirDeleted);
    }

    private void sendDeleteKeysRequest(String volume, String bucket, List<String> keysList,
        List<Long> expiredKeyUpdateIDList, boolean dir) {
      try {
        if (getInjector(1) != null) {
          try {
            getInjector(1).pause();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        int batchSize = keyLimitPerRequest;
        int startIndex = 0;
        for (int i = 0; i < keysList.size();) {
          DeleteKeyArgs.Builder builder =
              DeleteKeyArgs.newBuilder().setBucketName(bucket).setVolumeName(volume);
          int endIndex = startIndex + (batchSize < (keysList.size() - startIndex) ?
              batchSize : keysList.size() - startIndex);
          int keyCount = endIndex - startIndex;
          builder.addAllKeys(keysList.subList(startIndex, endIndex));
          builder.addAllUpdateIDs(expiredKeyUpdateIDList.subList(startIndex, endIndex));

          DeleteKeyArgs deleteKeyArgs = builder.build();
          DeleteKeysRequest deleteKeysRequest = DeleteKeysRequest.newBuilder().setDeleteKeys(deleteKeyArgs).build();
          LOG.info("request size {} for {} keys", deleteKeysRequest.getSerializedSize(), keyCount);

          if (deleteKeysRequest.getSerializedSize() < ratisByteLimit) {
            // send request out
            OMRequest omRequest = OMRequest.newBuilder()
                .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKeys)
                .setVersion(ClientVersion.CURRENT_VERSION)
                .setClientId(clientId.toString())
                .setDeleteKeysRequest(deleteKeysRequest)
                .build();
            OzoneManagerRatisUtils.submitRequest(getOzoneManager(), omRequest, clientId, callId.getAndIncrement());
            i += batchSize;
            startIndex += batchSize;
            if (dir) {
              numDirDeleted += keyCount;
              metrics.incrNumDirDeleted(keyCount);
            } else {
              numKeyDeleted += keyCount;
              metrics.incrNumKeyDeleted(keyCount);
            }
          } else {
            batchSize /= 2;
          }
        }
      } catch (ServiceException e) {
        LOG.error("Failed to send DeleteKeysRequest", e);
      }
    }

    private void moveKeysToTrash(List<String> keysList) {
      for (String key : keysList) {
        try {
          ozoneTrash.moveToTrash(new Path(key));
        } catch (IOException e) {
          // log failure and continue
          LOG.warn("Failed to move key {} to trash", key, e);
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
}
