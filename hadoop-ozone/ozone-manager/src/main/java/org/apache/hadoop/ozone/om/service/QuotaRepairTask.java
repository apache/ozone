/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.ozone.om.service;

import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.OLD_QUOTA_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.QUOTA_RESET;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

/**
 * Quota repair task.
 */
public class QuotaRepairTask {
  private static final Logger LOG = LoggerFactory.getLogger(
      QuotaRepairTask.class);
  private static final int BATCH_SIZE = 5000;
  private static final int TASK_THREAD_CNT = 3;
  public static final long EPOCH_DEFAULT = -1L;
  private final OzoneManager om;
  private ExecutorService executor;
  public QuotaRepairTask(OzoneManager ozoneManager) {
    this.om = ozoneManager;
  }

  public void repair() throws Exception {
    LOG.info("Starting quota repair task");
    try {
      // thread pool with 3 Table type * (1 task each + 3 thread each)
      executor = Executors.newFixedThreadPool(12);

      // repair active db
      OMMetadataManager activeMetaManager = createActiveDBCheckpoint(om.getMetadataManager(), om.getConfiguration());
      repairActiveDb(activeMetaManager);
      // list all snapshot
      // repair snapshot dbs

      updateOldVolumeQuotaSupport(om.getMetadataManager());
    } finally {
      LOG.info("Completed quota repair task");
      executor.shutdown();
    }
  }

  private void repairActiveDb(OMMetadataManager metadataManager) throws Exception {
    Map<String, OmBucketInfo> nameBucketInfoMap = new HashMap<>();
    Map<String, OmBucketInfo> idBucketInfoMap = new HashMap<>();
    Map<String, OmBucketInfo> oriBucketInfoMap = new HashMap<>();
    prepareAllBucketInfo(nameBucketInfoMap, idBucketInfoMap, oriBucketInfoMap, metadataManager);

    repairCount(nameBucketInfoMap, idBucketInfoMap, metadataManager);

    // update bucket count
    updateOldBucketQuotaSupport(metadataManager, nameBucketInfoMap);
    IOzoneManagerLock lock = metadataManager.getLock();
    try {
      nameBucketInfoMap.values().stream().forEach(e -> lock.acquireReadLock(
          BUCKET_LOCK, e.getVolumeName(), e.getBucketName()));
      try (BatchOperation batchOperation = metadataManager.getStore().initBatchOperation()) {
        for (Map.Entry<String, OmBucketInfo> entry : nameBucketInfoMap.entrySet()) {
          if (!isChange(oriBucketInfoMap.get(entry.getKey()), entry.getValue())) {
            continue;
          }
          String bucketKey = metadataManager.getBucketKey(entry.getValue().getVolumeName(),
              entry.getValue().getBucketName());
          metadataManager.getBucketTable().putWithBatch(batchOperation, bucketKey, entry.getValue());
          metadataManager.getBucketTable().addCacheEntry(new CacheKey<>(bucketKey),
              CacheValue.get(EPOCH_DEFAULT, entry.getValue()));
        }
        metadataManager.getStore().commitBatchOperation(batchOperation);
      }
    } finally {
      nameBucketInfoMap.values().stream().forEach(e -> lock.releaseReadLock(
          BUCKET_LOCK, e.getVolumeName(), e.getBucketName()));
    }
  }
  
  private OMMetadataManager createActiveDBCheckpoint(
      OMMetadataManager omMetaManager, OzoneConfiguration conf) throws IOException {
    // cleanup
    File dbLocation = omMetaManager.getStore().getDbLocation();
    if (dbLocation == null) {
      throw new NullPointerException("db location is null");
    }
    String tempData = dbLocation.getParent();
    if (tempData == null) {
      throw new NullPointerException("parent db dir is null");
    }
    File repairTmpPath = Paths.get(tempData, "temp-repair-quota").toFile();
    FileUtils.deleteDirectory(repairTmpPath);

    // create snapshot
    DBCheckpoint checkpoint = omMetaManager.getStore().getCheckpoint("temp-repair-quota", true);
    return OmMetadataManagerImpl.createCheckpointMetadataManager(conf, checkpoint);
  }

  private void prepareAllBucketInfo(
      Map<String, OmBucketInfo> nameBucketInfoMap, Map<String, OmBucketInfo> idBucketInfoMap,
      Map<String, OmBucketInfo> oriBucketInfoMap, OMMetadataManager metadataManager) throws IOException {
    try (TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>>
             iterator = metadataManager.getBucketTable().iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmBucketInfo> entry = iterator.next();
        OmBucketInfo bucketInfo = entry.getValue();
        String bucketNameKey = buildNamePath(bucketInfo.getVolumeName(),
            bucketInfo.getBucketName());
        oriBucketInfoMap.put(bucketNameKey, bucketInfo.copyObject());
        bucketInfo.incrUsedNamespace(-bucketInfo.getUsedNamespace());
        bucketInfo.incrUsedBytes(-bucketInfo.getUsedBytes());
        nameBucketInfoMap.put(bucketNameKey, bucketInfo);
        idBucketInfoMap.put(buildIdPath(metadataManager.getVolumeId(bucketInfo.getVolumeName()),
                bucketInfo.getObjectID()), bucketInfo);
      }
    }
  }

  private boolean isChange(OmBucketInfo lBucketInfo, OmBucketInfo rBucketInfo) {
    if (lBucketInfo.getUsedNamespace() != rBucketInfo.getUsedNamespace()
        || lBucketInfo.getUsedBytes() != rBucketInfo.getUsedBytes()
        || lBucketInfo.getQuotaInNamespace() != rBucketInfo.getQuotaInNamespace()
        || lBucketInfo.getQuotaInBytes() != rBucketInfo.getQuotaInBytes()) {
      return true;
    }
    return false;
  }

  private static void updateOldVolumeQuotaSupport(OMMetadataManager metadataManager) throws IOException {
    LOG.info("Starting volume quota support update");
    IOzoneManagerLock lock = metadataManager.getLock();
    try (TableIterator<String, ? extends Table.KeyValue<String, OmVolumeArgs>>
             iterator = metadataManager.getVolumeTable().iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmVolumeArgs> entry = iterator.next();
        OmVolumeArgs omVolumeArgs = entry.getValue();
        if (!(omVolumeArgs.getQuotaInBytes() == OLD_QUOTA_DEFAULT
            || omVolumeArgs.getQuotaInNamespace() == OLD_QUOTA_DEFAULT)) {
          continue;
        }
        try {
          lock.acquireReadLock(VOLUME_LOCK, omVolumeArgs.getVolume());
          boolean isQuotaReset = false;
          if (omVolumeArgs.getQuotaInBytes() == OLD_QUOTA_DEFAULT) {
            omVolumeArgs.setQuotaInBytes(QUOTA_RESET);
            isQuotaReset = true;
          }
          if (omVolumeArgs.getQuotaInNamespace() == OLD_QUOTA_DEFAULT) {
            omVolumeArgs.setQuotaInNamespace(QUOTA_RESET);
            isQuotaReset = true;
          }
          if (isQuotaReset) {
            metadataManager.getVolumeTable().addCacheEntry(
                new CacheKey<>(entry.getKey()), CacheValue.get(EPOCH_DEFAULT, omVolumeArgs));
            metadataManager.getVolumeTable().put(entry.getKey(), omVolumeArgs);
          }
        } finally {
          lock.releaseReadLock(VOLUME_LOCK, omVolumeArgs.getVolume());
        }
      }
    }
    LOG.info("Completed volume quota support update");
  }
  
  private static String buildNamePath(String volumeName, String bucketName) {
    final StringBuilder builder = new StringBuilder();
    builder.append(OM_KEY_PREFIX)
        .append(volumeName)
        .append(OM_KEY_PREFIX)
        .append(bucketName)
        .append(OM_KEY_PREFIX);
    return builder.toString();
  }

  private static String buildIdPath(long volumeId, long bucketId) {
    final StringBuilder builder = new StringBuilder();
    builder.append(OM_KEY_PREFIX)
        .append(volumeId)
        .append(OM_KEY_PREFIX)
        .append(bucketId)
        .append(OM_KEY_PREFIX);
    return builder.toString();
  }
  
  private void repairCount(
      Map<String, OmBucketInfo> nameBucketInfoMap, Map<String, OmBucketInfo> idBucketInfoMap,
      OMMetadataManager metadataManager) throws Exception {
    LOG.info("Starting quota repair counting for all keys, files and directories");
    Map<String, CountPair> keyCountMap = new ConcurrentHashMap<>();
    Map<String, CountPair> fileCountMap = new ConcurrentHashMap<>();
    Map<String, CountPair> directoryCountMap = new ConcurrentHashMap<>();
    try {
      nameBucketInfoMap.keySet().stream().forEach(e -> keyCountMap.put(e,
          new CountPair()));
      idBucketInfoMap.keySet().stream().forEach(e -> fileCountMap.put(e,
          new CountPair()));
      idBucketInfoMap.keySet().stream().forEach(e -> directoryCountMap.put(e,
          new CountPair()));
      
      List<Future<?>> tasks = new ArrayList<>();
      tasks.add(executor.submit(() -> recalculateUsages(
          metadataManager.getKeyTable(BucketLayout.OBJECT_STORE),
          keyCountMap, "Key usages", true)));
      tasks.add(executor.submit(() -> recalculateUsages(
          metadataManager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED),
          fileCountMap, "File usages", true)));
      tasks.add(executor.submit(() -> recalculateUsages(
          metadataManager.getDirectoryTable(),
          directoryCountMap, "Directory usages", false)));
      for (Future<?> f : tasks) {
        f.get();
      }
    } catch (UncheckedIOException ex) {
      LOG.error("quota repair failure", ex.getCause());
      throw ex.getCause();
    } catch (UncheckedExecutionException ex) {
      LOG.error("quota repair failure", ex.getCause());
      throw new Exception(ex.getCause());
    }
    
    // update count to bucket info
    updateCountToBucketInfo(nameBucketInfoMap, keyCountMap);
    updateCountToBucketInfo(idBucketInfoMap, fileCountMap);
    updateCountToBucketInfo(idBucketInfoMap, directoryCountMap);
    LOG.info("Completed quota repair counting for all keys, files and directories");
  }

  private static void updateOldBucketQuotaSupport(
      OMMetadataManager metadataManager, Map<String, OmBucketInfo> nameBucketInfoMap) {
    for (Map.Entry<String, OmBucketInfo> entry : nameBucketInfoMap.entrySet()) {
      if (entry.getValue().getQuotaInBytes() == OLD_QUOTA_DEFAULT
          || entry.getValue().getQuotaInNamespace() == OLD_QUOTA_DEFAULT) {
        OmBucketInfo.Builder builder = entry.getValue().toBuilder();
        if (entry.getValue().getQuotaInBytes() == OLD_QUOTA_DEFAULT) {
          builder.setQuotaInBytes(QUOTA_RESET);
        }
        if (entry.getValue().getQuotaInNamespace() == OLD_QUOTA_DEFAULT) {
          builder.setQuotaInNamespace(QUOTA_RESET);
        }
        OmBucketInfo bucketInfo = builder.build();
        entry.setValue(bucketInfo);
      }
    }
  }

  private <VALUE> void recalculateUsages(
      Table<String, VALUE> table, Map<String, CountPair> prefixUsageMap,
      String strType, boolean haveValue) throws UncheckedIOException,
      UncheckedExecutionException {
    LOG.info("Starting recalculate {}", strType);

    List<Table.KeyValue<String, VALUE>> kvList = new ArrayList<>(BATCH_SIZE);
    BlockingQueue<List<Table.KeyValue<String, VALUE>>> q
        = new ArrayBlockingQueue<>(TASK_THREAD_CNT);
    List<Future<?>> tasks = new ArrayList<>();
    AtomicBoolean isRunning = new AtomicBoolean(true);
    for (int i = 0; i < TASK_THREAD_CNT; ++i) {
      tasks.add(executor.submit(() -> captureCount(
          prefixUsageMap, q, isRunning, haveValue)));
    }
    int count = 0;
    long startTime = System.currentTimeMillis();
    try (TableIterator<String, ? extends Table.KeyValue<String, VALUE>>
             keyIter = table.iterator()) {
      while (keyIter.hasNext()) {
        count++;
        kvList.add(keyIter.next());
        if (kvList.size() == BATCH_SIZE) {
          q.put(kvList);
          kvList = new ArrayList<>(BATCH_SIZE);
        }
      }
      q.put(kvList);
      isRunning.set(false);
      for (Future<?> f : tasks) {
        f.get();
      }
      LOG.info("Recalculate {} completed, count {} time {}ms", strType,
          count, (System.currentTimeMillis() - startTime));
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException ex) {
      throw new UncheckedExecutionException(ex);
    }
  }
  
  private static <VALUE> void captureCount(
      Map<String, CountPair> prefixUsageMap,
      BlockingQueue<List<Table.KeyValue<String, VALUE>>> q,
      AtomicBoolean isRunning, boolean haveValue) throws UncheckedIOException {
    try {
      while (isRunning.get() || !q.isEmpty()) {
        List<Table.KeyValue<String, VALUE>> kvList
            = q.poll(100, TimeUnit.MILLISECONDS);
        if (null != kvList) {
          for (Table.KeyValue<String, VALUE> kv : kvList) {
            extractCount(kv, prefixUsageMap, haveValue);
          }
        }
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }
  
  private static <VALUE> void extractCount(
      Table.KeyValue<String, VALUE> kv,
      Map<String, CountPair> prefixUsageMap,
      boolean haveValue) {
    try {
      String prefix = getVolumeBucketPrefix(kv.getKey());
      CountPair usage = prefixUsageMap.get(prefix);
      if (null == usage) {
        return;
      }
      usage.incrNamespace(1L);
      // avoid decode of value
      if (haveValue) {
        VALUE value = kv.getValue();
        if (value instanceof OmKeyInfo) {
          usage.incrSpace(((OmKeyInfo) value).getReplicatedSize());
        }
      }
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }
  
  private static synchronized void updateCountToBucketInfo(
      Map<String, OmBucketInfo> bucketInfoMap,
      Map<String, CountPair> prefixUsageMap) {
    for (Map.Entry<String, CountPair> entry : prefixUsageMap.entrySet()) {
      // update bucket info if available
      OmBucketInfo omBucketInfo = bucketInfoMap.get(entry.getKey());
      if (omBucketInfo != null) {
        omBucketInfo.incrUsedBytes(entry.getValue().getSpace());
        omBucketInfo.incrUsedNamespace(entry.getValue().getNamespace());
      }
    }
  }
  
  private static String getVolumeBucketPrefix(String key) {
    // get bucket prefix with /<volume>/<bucket>/ 
    // -- as represents name in OBS and id in FSO
    String prefix = key;
    int idx = key.indexOf(OM_KEY_PREFIX, 1);
    if (idx != -1) {
      idx = key.indexOf(OM_KEY_PREFIX, idx + 1);
      if (idx != -1) {
        prefix = key.substring(0, idx + 1);
      }
    }
    return prefix;
  }
  
  private static class CountPair {
    private AtomicLong space = new AtomicLong();
    private AtomicLong namespace = new AtomicLong();

    public void incrSpace(long val) {
      space.getAndAdd(val);
    }
    
    public void incrNamespace(long val) {
      namespace.getAndAdd(val);
    }
    
    public long getSpace() {
      return space.get();
    }

    public long getNamespace() {
      return namespace.get();
    }
  }
}
