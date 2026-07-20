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

import static org.apache.hadoop.hdds.utils.db.IteratorType.KEY_AND_VALUE;
import static org.apache.hadoop.hdds.utils.db.IteratorType.KEY_ONLY;
import static org.apache.hadoop.ozone.OzoneConsts.OLD_QUOTA_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ServiceException;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
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
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainInfo;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Quota repair task.
 */
public class QuotaRepairTask {
  private static final Logger LOG = LoggerFactory.getLogger(
      QuotaRepairTask.class);
  private static final int BATCH_SIZE = 5000;
  private static final int TASK_THREAD_CNT = 3;
  /**
   * Parallel full-table scans: OBS keys, FSO files, dirs, active deleted keys/dirs,
   * snapshot DB deleted keys/dirs.
   */
  private static final int QUOTA_REPAIR_SCAN_TASKS = 6;
  private static final AtomicBoolean IN_PROGRESS = new AtomicBoolean(false);
  private static final RepairStatus REPAIR_STATUS = new RepairStatus();
  private static final AtomicLong RUN_CNT = new AtomicLong(0);
  private final OzoneManager om;
  private ExecutorService executor;

  public QuotaRepairTask(OzoneManager ozoneManager) {
    this.om = ozoneManager;
  }

  public CompletableFuture<Boolean> repair() throws IOException {
    return repair(Collections.emptyList());
  }

  public CompletableFuture<Boolean> repair(List<String> buckets) throws IOException {
    // lock in progress operation and reject any other
    if (!IN_PROGRESS.compareAndSet(false, true)) {
      LOG.info("quota repair task already running");
      throw new OMException("Quota repair is already running", OMException.ResultCodes.QUOTA_ERROR);
    }
    REPAIR_STATUS.reset(RUN_CNT.get() + 1);
    return CompletableFuture.supplyAsync(() -> repairTask(buckets));
  }

  public static String getStatus() {
    return REPAIR_STATUS.toString();
  }

  private boolean repairTask(List<String> buckets) {
    LOG.info("Starting quota repair task {}", REPAIR_STATUS);
    // thread pool: scan task types * (1 coordinator + worker threads per task)
    executor = Executors.newFixedThreadPool(QUOTA_REPAIR_SCAN_TASKS * (1 + TASK_THREAD_CNT));
    try (OMMetadataManager activeMetaManager =
        createActiveDBCheckpoint(om.getMetadataManager(), om.getConfiguration())) {
      OzoneManagerProtocolProtos.QuotaRepairRequest.Builder builder
          = OzoneManagerProtocolProtos.QuotaRepairRequest.newBuilder();
      // repair active db
      repairActiveDb(activeMetaManager, builder, buckets);

      // submit request to update
      ClientId clientId = ClientId.randomId();
      OzoneManagerProtocolProtos.OMRequest omRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
          .setCmdType(OzoneManagerProtocolProtos.Type.QuotaRepair)
          .setQuotaRepairRequest(builder.build())
          .setClientId(clientId.toString())
          .build();
      OzoneManagerProtocolProtos.OMResponse response = submitRequest(omRequest, clientId);
      if (response != null && response.getSuccess()) {
        REPAIR_STATUS.updateStatus(builder, om.getMetadataManager());
      } else {
        LOG.error("update quota repair count response failed");
        REPAIR_STATUS.updateStatus("Response for update DB is failed");
        return false;
      }
    } catch (Exception exp) {
      LOG.error("quota repair count failed", exp);
      REPAIR_STATUS.updateStatus(exp.toString());
      return false;
    } finally {
      LOG.info("Completed quota repair task {}", REPAIR_STATUS);
      executor.shutdown();
      try {
        cleanTempCheckPointPath(om.getMetadataManager());
      } catch (Exception exp) {
        LOG.error("failed to cleanup", exp);
      }
      IN_PROGRESS.set(false);
    }
    return true;
  }

  private void repairActiveDb(
      OMMetadataManager metadataManager,
      OzoneManagerProtocolProtos.QuotaRepairRequest.Builder builder,
      List<String> buckets) throws Exception {
    Map<String, OmBucketInfo> nameBucketInfoMap = new HashMap<>();
    Map<String, OmBucketInfo> idBucketInfoMap = new HashMap<>();
    Map<String, OmBucketInfo> oriBucketInfoMap = new HashMap<>();
    prepareAllBucketInfo(nameBucketInfoMap, idBucketInfoMap, oriBucketInfoMap, metadataManager, buckets);
    if (nameBucketInfoMap.isEmpty()) {
      throw new OMException("no matching buckets", OMException.ResultCodes.BUCKET_NOT_FOUND);
    }

    repairCount(nameBucketInfoMap, idBucketInfoMap, metadataManager);

    // prepare and submit request to ratis
    for (Map.Entry<String, OmBucketInfo> entry : nameBucketInfoMap.entrySet()) {
      OmBucketInfo oriBucketInfo = oriBucketInfoMap.get(entry.getKey());
      OmBucketInfo updatedBuckedInfo = entry.getValue();
      boolean oldQuota = oriBucketInfo.getQuotaInBytes() == OLD_QUOTA_DEFAULT
          || oriBucketInfo.getQuotaInNamespace() == OLD_QUOTA_DEFAULT;
      if (!(oldQuota || isChange(oriBucketInfo, updatedBuckedInfo))) {
        continue;
      }
      OzoneManagerProtocolProtos.BucketQuotaCount.Builder bucketCountBuilder
          = OzoneManagerProtocolProtos.BucketQuotaCount.newBuilder();
      bucketCountBuilder.setVolName(updatedBuckedInfo.getVolumeName());
      bucketCountBuilder.setBucketName(updatedBuckedInfo.getBucketName());
      bucketCountBuilder.setDiffUsedBytes(updatedBuckedInfo.getUsedBytes() - oriBucketInfo.getUsedBytes());
      bucketCountBuilder.setDiffUsedNamespace(
          updatedBuckedInfo.getUsedNamespace() - oriBucketInfo.getUsedNamespace());
      bucketCountBuilder.setDiffSnapshotUsedBytes(
          updatedBuckedInfo.getSnapshotUsedBytes() - oriBucketInfo.getSnapshotUsedBytes());
      bucketCountBuilder.setDiffSnapshotUsedNamespace(
          updatedBuckedInfo.getSnapshotUsedNamespace() - oriBucketInfo.getSnapshotUsedNamespace());
      bucketCountBuilder.setSupportOldQuota(oldQuota);
      builder.addBucketCount(bucketCountBuilder.build());
    }

    // update volume to support quota
    if (buckets.isEmpty()) {
      builder.setSupportVolumeOldQuota(true);
    } else {
      builder.setSupportVolumeOldQuota(false);
    }
  }

  private OzoneManagerProtocolProtos.OMResponse submitRequest(
      OzoneManagerProtocolProtos.OMRequest omRequest, ClientId clientId) throws Exception {
    try {
      return OzoneManagerRatisUtils.submitRequest(om, omRequest, clientId, RUN_CNT.getAndIncrement());
    } catch (ServiceException e) {
      LOG.error("repair quota count " + omRequest.getCmdType() + " request failed.", e);
      throw e;
    }
  }
  
  private OMMetadataManager createActiveDBCheckpoint(
      OMMetadataManager omMetaManager, OzoneConfiguration conf) throws IOException {
    // cleanup
    String parentPath = cleanTempCheckPointPath(omMetaManager);

    // create snapshot
    DBCheckpoint checkpoint = omMetaManager.getStore().getCheckpoint(parentPath, true);
    return OmMetadataManagerImpl.createCheckpointMetadataManager(conf, checkpoint);
  }

  private static String cleanTempCheckPointPath(OMMetadataManager omMetaManager) throws IOException {
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
    FileUtils.forceMkdir(repairTmpPath);
    return repairTmpPath.toString();
  }

  private void prepareAllBucketInfo(
      Map<String, OmBucketInfo> nameBucketInfoMap, Map<String, OmBucketInfo> idBucketInfoMap,
      Map<String, OmBucketInfo> oriBucketInfoMap, OMMetadataManager metadataManager,
      List<String> buckets) throws IOException {
    if (!buckets.isEmpty()) {
      for (String bucketkey : buckets) {
        OmBucketInfo bucketInfo = metadataManager.getBucketTable().get(bucketkey);
        if (null == bucketInfo) {
          continue;
        }
        populateBucket(nameBucketInfoMap, idBucketInfoMap, oriBucketInfoMap, metadataManager, bucketInfo);
      }
      return;
    }
    try (TableIterator<String, OmBucketInfo> iterator = metadataManager.getBucketTable().valueIterator()) {
      while (iterator.hasNext()) {
        final OmBucketInfo bucketInfo = iterator.next();
        populateBucket(nameBucketInfoMap, idBucketInfoMap, oriBucketInfoMap, metadataManager, bucketInfo);
      }
    }
  }

  private static void populateBucket(
      Map<String, OmBucketInfo> nameBucketInfoMap, Map<String, OmBucketInfo> idBucketInfoMap,
      Map<String, OmBucketInfo> oriBucketInfoMap, OMMetadataManager metadataManager,
      OmBucketInfo bucketInfo) throws IOException {
    String bucketNameKey = buildNamePath(bucketInfo.getVolumeName(),
        bucketInfo.getBucketName());
    oriBucketInfoMap.put(bucketNameKey, bucketInfo.copyObject());
    bucketInfo.decrUsedBytes(bucketInfo.getUsedBytes(), false);
    bucketInfo.decrUsedNamespace(bucketInfo.getUsedNamespace(), false);
    resetSnapshotBucketQuota(bucketInfo);
    nameBucketInfoMap.put(bucketNameKey, bucketInfo);
    idBucketInfoMap.put(buildIdPath(metadataManager.getVolumeId(bucketInfo.getVolumeName()),
            bucketInfo.getObjectID()), bucketInfo);
  }

  private boolean isChange(OmBucketInfo lBucketInfo, OmBucketInfo rBucketInfo) {
    return lBucketInfo.getUsedNamespace() != rBucketInfo.getUsedNamespace()
        || lBucketInfo.getUsedBytes() != rBucketInfo.getUsedBytes()
        || lBucketInfo.getSnapshotUsedBytes() != rBucketInfo.getSnapshotUsedBytes()
        || lBucketInfo.getSnapshotUsedNamespace() != rBucketInfo.getSnapshotUsedNamespace();
  }

  private static void resetSnapshotBucketQuota(OmBucketInfo bucketInfo) {
    long snapBytes = bucketInfo.getSnapshotUsedBytes();
    if (snapBytes != 0) {
      bucketInfo.purgeSnapshotUsedBytes(snapBytes);
    }
    long snapNs = bucketInfo.getSnapshotUsedNamespace();
    if (snapNs != 0) {
      bucketInfo.purgeSnapshotUsedNamespace(snapNs);
    }
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
    Map<String, CountPair> snapshotDeletedKeyMap = new ConcurrentHashMap<>();
    Map<String, CountPair> snapshotDeletedDirMap = new ConcurrentHashMap<>();
    try {
      nameBucketInfoMap.keySet().stream().forEach(e -> keyCountMap.put(e,
          new CountPair()));
      idBucketInfoMap.keySet().stream().forEach(e -> fileCountMap.put(e,
          new CountPair()));
      idBucketInfoMap.keySet().stream().forEach(e -> directoryCountMap.put(e,
          new CountPair()));
      nameBucketInfoMap.keySet().forEach(k -> snapshotDeletedKeyMap.put(k, new CountPair()));
      idBucketInfoMap.keySet().forEach(k -> snapshotDeletedDirMap.put(k, new CountPair()));

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

      Map<Long, OmBucketInfo> bucketById = buildBucketByObjectId(idBucketInfoMap);

      tasks.add(executor.submit(() -> recalculateDeletedKeyUsages(
          metadataManager.getDeletedTable(), bucketById, snapshotDeletedKeyMap,
          "active DB checkpoint")));
      tasks.add(executor.submit(() -> recalculateDeletedDirNamespace(
          metadataManager.getDeletedDirTable(), snapshotDeletedDirMap,
          "active DB checkpoint")));
      tasks.add(executor.submit(() -> {
        try {
          recalculateSnapshotDbPendingDeleteQuota(nameBucketInfoMap, bucketById, metadataManager,
              snapshotDeletedKeyMap, snapshotDeletedDirMap);
        } catch (IOException ex) {
          throw new UncheckedIOException(ex);
        }
      }));

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
    mergeSnapshotDeletedTableCounts(nameBucketInfoMap, snapshotDeletedKeyMap);
    mergeDeletedDirSnapshotNamespace(idBucketInfoMap, snapshotDeletedDirMap);
    LOG.info("Completed quota repair counting for all keys, files and directories");
  }

  private static Map<Long, OmBucketInfo> buildBucketByObjectId(
      Map<String, OmBucketInfo> idBucketInfoMap) {
    Map<Long, OmBucketInfo> bucketById = new HashMap<>();
    for (OmBucketInfo bucketInfo : idBucketInfoMap.values()) {
      bucketById.putIfAbsent(bucketInfo.getObjectID(), bucketInfo);
    }
    return bucketById;
  }

  /**
   * Recompute pending-delete quota from each ACTIVE snapshot DB on repaired buckets' path chains.
   * Runs as an executor task in parallel with active-table scans.
   */
  private void recalculateSnapshotDbPendingDeleteQuota(
      Map<String, OmBucketInfo> nameBucketInfoMap,
      Map<Long, OmBucketInfo> bucketById,
      OMMetadataManager activeMetaManager,
      Map<String, CountPair> snapshotDeletedKeyMap,
      Map<String, CountPair> snapshotDeletedDirMap) throws IOException {
    LOG.info("Starting recalculate snapshot pending-delete from snapshot DBs");
    OMMetadataManager liveMetaManager = om.getMetadataManager();
    SnapshotChainManager chain =
        ((OmMetadataManagerImpl) liveMetaManager).getSnapshotChainManager();
    OmSnapshotManager snapshotManager = om.getOmSnapshotManager();
    Set<UUID> scannedSnapshotIds = new HashSet<>();

    for (OmBucketInfo bucket : nameBucketInfoMap.values()) {
      String snapshotPath = buildSnapshotPath(bucket.getVolumeName(), bucket.getBucketName());
      LinkedHashMap<UUID, SnapshotChainInfo> pathChain;
      try {
        pathChain = chain.getSnapshotChainPath(snapshotPath);
      } catch (IOException ex) {
        throw new IOException("Failed to read snapshot chain for path " + snapshotPath, ex);
      }
      if (pathChain == null || pathChain.isEmpty()) {
        continue;
      }
      for (UUID snapshotId : pathChain.keySet()) {
        if (!scannedSnapshotIds.add(snapshotId)) {
          continue;
        }
        SnapshotInfo snapshotInfo = loadActiveSnapshot(activeMetaManager, chain, snapshotId);
        if (snapshotInfo == null) {
          continue;
        }
        if (!snapshotInfo.getVolumeName().equals(bucket.getVolumeName())
            || !snapshotInfo.getBucketName().equals(bucket.getBucketName())) {
          continue;
        }
        if (!OmSnapshotManager.isSnapshotFlushedToDB(liveMetaManager, snapshotInfo)) {
          LOG.warn("Skipping snapshot {} for quota repair: create txn not flushed to active DB",
              snapshotInfo.getTableKey());
          continue;
        }
        String sourceLabel = "snapshot DB " + snapshotInfo.getTableKey();
        try (UncheckedAutoCloseableSupplier<OmSnapshot> snapshotRef =
                 snapshotManager.getSnapshot(snapshotId)) {
          scanDeletedTables(snapshotRef.get().getMetadataManager(), bucketById,
              snapshotDeletedKeyMap, snapshotDeletedDirMap, sourceLabel);
        }
      }
    }
    LOG.info("Recalculate snapshot pending-delete from snapshot DBs completed, snapshots scanned: {}",
        scannedSnapshotIds.size());
  }

  private static String buildSnapshotPath(String volumeName, String bucketName) {
    return volumeName + OM_KEY_PREFIX + bucketName;
  }

  private static SnapshotInfo loadActiveSnapshot(
      OMMetadataManager metadataManager,
      SnapshotChainManager chain,
      UUID snapshotId) throws IOException {
    String tableKey = chain.getTableKey(snapshotId);
    if (tableKey == null) {
      LOG.warn("Snapshot id {} is not present in snapshot chain table-key map", snapshotId);
      return null;
    }
    SnapshotInfo snapshotInfo = metadataManager.getSnapshotInfoTable().get(tableKey);
    if (snapshotInfo == null) {
      LOG.warn("Snapshot {} not found in snapshotInfoTable during quota repair", tableKey);
      return null;
    }
    if (snapshotInfo.getSnapshotStatus() != SNAPSHOT_ACTIVE) {
      return null;
    }
    return snapshotInfo;
  }

  private void scanDeletedTables(
      OMMetadataManager metadataManager,
      Map<Long, OmBucketInfo> bucketById,
      Map<String, CountPair> snapshotDeletedKeyMap,
      Map<String, CountPair> snapshotDeletedDirMap,
      String sourceLabel) throws UncheckedIOException {
    recalculateDeletedKeyUsages(metadataManager.getDeletedTable(), bucketById,
        snapshotDeletedKeyMap, sourceLabel);
    recalculateDeletedDirNamespace(metadataManager.getDeletedDirTable(),
        snapshotDeletedDirMap, sourceLabel);
  }

  private void recalculateDeletedKeyUsages(
      Table<String, RepeatedOmKeyInfo> deletedTable,
      Map<Long, OmBucketInfo> bucketById,
      Map<String, CountPair> snapshotCountByBucketNameKey,
      String sourceLabel)
      throws UncheckedIOException {
    LOG.info("Starting recalculate snapshot usages from deletedTable ({})", sourceLabel);

    int count = 0;
    long startTime = Time.monotonicNow();
    try (Table.KeyValueIterator<String, RepeatedOmKeyInfo> keyIter
        = deletedTable.iterator()) {
      while (keyIter.hasNext()) {
        Table.KeyValue<String, RepeatedOmKeyInfo> kv = keyIter.next();
        count++;
        RepeatedOmKeyInfo val = kv.getValue();
        OmBucketInfo bucket = bucketById.get(val.getBucketId());
        if (bucket == null) {
          continue;
        }
        String nameKey = buildNamePath(bucket.getVolumeName(), bucket.getBucketName());
        CountPair usage = snapshotCountByBucketNameKey.get(nameKey);
        if (usage == null) {
          continue;
        }
        usage.incrSpace(val.getTotalSize().getRight());
        usage.incrNamespace(val.getOmKeyInfoList().size());
      }
      LOG.info("Recalculate snapshot usages from deletedTable ({}) completed, count {} time {}ms",
          sourceLabel, count, (Time.monotonicNow() - startTime));
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  private void recalculateDeletedDirNamespace(
      Table<String, OmKeyInfo> deletedDirTable,
      Map<String, CountPair> snapshotDirNsByIdPrefix,
      String sourceLabel)
      throws UncheckedIOException {
    LOG.info("Starting recalculate snapshot namespace from deletedDirectoryTable ({})",
        sourceLabel);

    int count = 0;
    long startTime = Time.monotonicNow();
    try (Table.KeyValueIterator<String, OmKeyInfo> keyIter
        = deletedDirTable.iterator()) {
      while (keyIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
        count++;
        String prefix = getVolumeBucketPrefix(kv.getKey());
        CountPair usage = snapshotDirNsByIdPrefix.get(prefix);
        if (usage != null) {
          usage.incrNamespace(1L);
        }
      }
      LOG.info(
          "Recalculate snapshot namespace from deletedDirectoryTable ({}) completed, count {} time {}ms",
          sourceLabel, count, (Time.monotonicNow() - startTime));
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  private static synchronized void mergeSnapshotDeletedTableCounts(
      Map<String, OmBucketInfo> nameBucketInfoMap,
      Map<String, CountPair> counts) {
    for (Map.Entry<String, CountPair> entry : counts.entrySet()) {
      OmBucketInfo bucket = nameBucketInfoMap.get(entry.getKey());
      if (bucket != null) {
        bucket.incrSnapshotUsedBytes(entry.getValue().getSpace());
        bucket.incrSnapshotUsedNamespace(entry.getValue().getNamespace());
      }
    }
  }

  private static synchronized void mergeDeletedDirSnapshotNamespace(
      Map<String, OmBucketInfo> idBucketInfoMap,
      Map<String, CountPair> counts) {
    for (Map.Entry<String, CountPair> entry : counts.entrySet()) {
      OmBucketInfo bucket = idBucketInfoMap.get(entry.getKey());
      if (bucket != null) {
        bucket.incrSnapshotUsedNamespace(entry.getValue().getNamespace());
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
    long startTime = Time.monotonicNow();
    try (Table.KeyValueIterator<String, VALUE> keyIter
        = table.iterator(null, haveValue ? KEY_AND_VALUE : KEY_ONLY)) {
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
          count, (Time.monotonicNow() - startTime));
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

  /**
   * Repair status for last run.
   */
  public static class RepairStatus {
    private boolean isTriggered = false;
    private long taskId = 0;
    private long lastRunStartTime = 0;
    private long lastRunFinishedTime = 0;
    private String errorMsg = null;
    private Map<String, Map<String, Long>> bucketCountDiffMap = new ConcurrentHashMap<>();

    @Override
    public String toString() {
      if (!isTriggered) {
        return "{}";
      }
      Map<String, Object> status = new HashMap<>();
      status.put("taskId", taskId);
      status.put("lastRunStartTime", lastRunStartTime > 0 ? new java.util.Date(lastRunStartTime).toString() : "");
      status.put("lastRunFinishedTime", lastRunFinishedTime > 0 ? new java.util.Date(lastRunFinishedTime).toString()
          : "");
      status.put("errorMsg", errorMsg);
      status.put("bucketCountDiffMap", bucketCountDiffMap);
      try {
        return JsonUtils.toJsonString(status);
      } catch (IOException e) {
        LOG.error("error in generating status", e);
        return "{}";
      }
    }

    public void updateStatus(OzoneManagerProtocolProtos.QuotaRepairRequest.Builder builder,
                             OMMetadataManager metadataManager) {
      isTriggered = true;
      lastRunFinishedTime = System.currentTimeMillis();
      errorMsg = "";
      bucketCountDiffMap.clear();
      for (OzoneManagerProtocolProtos.BucketQuotaCount quotaCount : builder.getBucketCountList()) {
        String bucketKey = metadataManager.getBucketKey(quotaCount.getVolName(), quotaCount.getBucketName());
        ConcurrentHashMap<String, Long> diffCountMap = new ConcurrentHashMap<>();
        diffCountMap.put("DiffUsedBytes", quotaCount.getDiffUsedBytes());
        diffCountMap.put("DiffUsedNamespace", quotaCount.getDiffUsedNamespace());
        if (quotaCount.hasDiffSnapshotUsedBytes()) {
          diffCountMap.put("DiffSnapshotUsedBytes", quotaCount.getDiffSnapshotUsedBytes());
        }
        if (quotaCount.hasDiffSnapshotUsedNamespace()) {
          diffCountMap.put("DiffSnapshotUsedNamespace", quotaCount.getDiffSnapshotUsedNamespace());
        }
        bucketCountDiffMap.put(bucketKey, diffCountMap);
      }
    }

    public void updateStatus(String errMsg) {
      isTriggered = true;
      lastRunFinishedTime = System.currentTimeMillis();
      errorMsg = errMsg;
      bucketCountDiffMap.clear();
    }

    public void reset(long tskId) {
      isTriggered = true;
      taskId = tskId;
      lastRunStartTime = System.currentTimeMillis();
      lastRunFinishedTime = 0;
      errorMsg = "";
      bucketCountDiffMap.clear();
    }
  }
}
