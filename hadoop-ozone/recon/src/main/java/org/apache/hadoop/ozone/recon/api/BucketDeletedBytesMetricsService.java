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

package org.apache.hadoop.ozone.recon.api;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_MINIMUM_API_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_MINIMUM_API_DELAY_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_THREAD_COUNT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_THREAD_COUNT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_TIMEOUT_DEFAULT;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.recon.api.types.BucketDeletedBytesCompleteResponse;
import org.apache.hadoop.ozone.recon.api.types.BucketDeletedBytesMetrics;
import org.apache.hadoop.ozone.recon.api.types.BucketDeletedBytesProgressResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.apache.hadoop.ozone.recon.tasks.BucketDeletedBytesCollectionTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for collecting and managing the snapshot-trapped vs purgeable
 * deleted-bytes split across all buckets. Collects metrics asynchronously
 * from OM, one RPC per bucket, and provides aggregated and per-bucket results.
 */
@Singleton
public class BucketDeletedBytesMetricsService {

  private static final Logger LOG = LoggerFactory.getLogger(BucketDeletedBytesMetricsService.class);
  private static final int POLL_INTERVAL_MS = 200;

  private final ExecutorService executorService;
  private final ReconOMMetadataManager omMetadataManager;
  private final OzoneManagerServiceProvider ozoneManagerServiceProvider;
  private final int minimumApiDelayMs;
  private final int maximumTaskTimeout;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  private volatile MetricCollectionStatus currentStatus = MetricCollectionStatus.NOT_STARTED;
  private volatile String failedMessage = "Metrics collection task failed. Please retry after some time.";
  private Map<String, BucketDeletedBytesMetrics> perBucketMetrics = new HashMap<>();
  private long totalPurgeableSize = 0L;
  private long totalSnapshotTrappedSize = 0L;
  private long totalPurgeableKeys = 0L;
  private long totalPurgeableDirs = 0L;
  private long totalPurgeableKeySize = 0L;
  private long totalPurgeableDirSize = 0L;
  private int totalBucketsQueried;
  private int totalBucketQueriesFailed;
  private AtomicLong lastCollectionEndTime = new AtomicLong(0L);

  @Inject
  public BucketDeletedBytesMetricsService(
      ReconOMMetadataManager omMetadataManager,
      OzoneManagerServiceProvider ozoneManagerServiceProvider,
      OzoneConfiguration config) {

    this.omMetadataManager = omMetadataManager;
    this.ozoneManagerServiceProvider = ozoneManagerServiceProvider;
    this.minimumApiDelayMs = (int) config.getTimeDuration(
        OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_MINIMUM_API_DELAY,
        OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_MINIMUM_API_DELAY_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.maximumTaskTimeout = (int) config.getTimeDuration(
        OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_TIMEOUT,
        OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    this.lastCollectionEndTime.set(-minimumApiDelayMs);
    int corePoolSize = config.getInt(OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_THREAD_COUNT,
        OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_THREAD_COUNT_DEFAULT);
    corePoolSize = corePoolSize > 0
        ? corePoolSize
        : OZONE_RECON_BUCKET_DELETED_BYTES_COLLECTION_THREAD_COUNT_DEFAULT;
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("BucketDeletedBytesCollector-%d")
        .build();
    this.executorService = Executors.newFixedThreadPool(corePoolSize, threadFactory);
  }

  /**
   * Starts the metrics collection task if not already running and rate limit allows.
   */
  public void startTask() {
    if (!isRunning.compareAndSet(false, true)) {
      LOG.warn("Bucket deleted bytes collection already in progress, skipping");
      return;
    }

    if (System.currentTimeMillis() - lastCollectionEndTime.get() < minimumApiDelayMs) {
      LOG.debug("Rate limit active, skipping collection (delay: {}ms)", minimumApiDelayMs);
      isRunning.set(false);
      return;
    }

    List<OmBucketInfo> buckets;
    try {
      buckets = listAllBuckets();
    } catch (IOException | RuntimeException e) {
      // Any failure here must release isRunning, otherwise every later request is skipped as "in progress".
      LOG.error("Failed to list buckets for deleted bytes collection", e);
      resetState();
      currentStatus = MetricCollectionStatus.FAILED;
      failedMessage = e.getLocalizedMessage();
      isRunning.set(false);
      return;
    }

    if (buckets.isEmpty()) {
      LOG.warn("No buckets found to query");
      resetState();
      currentStatus = MetricCollectionStatus.FINISHED;
      isRunning.set(false);
      return;
    }

    currentStatus = MetricCollectionStatus.IN_PROGRESS;
    LOG.debug("Starting deleted bytes collection for {} buckets", buckets.size());

    CompletableFuture.runAsync(() -> collectMetrics(buckets), executorService)
        .exceptionally(throwable -> {
          LOG.error("Bucket deleted bytes collection failed", throwable);
          synchronized (BucketDeletedBytesMetricsService.this) {
            resetState();
            currentStatus = MetricCollectionStatus.FAILED;
            failedMessage = throwable.getLocalizedMessage();
            isRunning.set(false);
          }
          return null;
        });
  }

  private List<OmBucketInfo> listAllBuckets() throws IOException {
    List<OmBucketInfo> buckets = new ArrayList<>();
    Table<String, OmBucketInfo> bucketTable = omMetadataManager.getBucketTable();
    if (bucketTable == null) {
      // Recon's OM DB copy is not loaded yet, e.g. right after Recon startup.
      throw new IOException("Recon OM DB is not initialized yet. Please retry after some time.");
    }
    try (TableIterator<String, Table.KeyValue<String, OmBucketInfo>> iterator =
             bucketTable.iterator()) {
      while (iterator.hasNext()) {
        OmBucketInfo omBucketInfo = iterator.next().getValue();
        if (omBucketInfo != null) {
          buckets.add(omBucketInfo);
        }
      }
    }
    return buckets;
  }

  private void collectMetrics(List<OmBucketInfo> buckets) {
    try {
      CollectionContext context = submitCollectionTasks(buckets);
      processCollectionFutures(context);
      updateFinalState(context);
    } catch (Exception e) {
      resetState();
      currentStatus = MetricCollectionStatus.FAILED;
      failedMessage = e.getLocalizedMessage();
      isRunning.set(false);
    }
  }

  private CollectionContext submitCollectionTasks(List<OmBucketInfo> buckets) {
    Map<String, Future<BucketDeletedBytesMetrics>> futures = new HashMap<>();
    long submissionTime = System.currentTimeMillis();
    for (OmBucketInfo bucket : buckets) {
      String key = bucket.getVolumeName() + "/" + bucket.getBucketName();
      BucketDeletedBytesCollectionTask task = new BucketDeletedBytesCollectionTask(
          bucket.getVolumeName(), bucket.getBucketName(), ozoneManagerServiceProvider);
      futures.put(key, executorService.submit(task));
    }
    int totalQueried = futures.size();
    LOG.debug("Submitted {} bucket deleted bytes collection tasks", totalQueried);
    return new CollectionContext(totalQueried, futures, submissionTime);
  }

  private void processCollectionFutures(CollectionContext context) {
    while (!context.futures.isEmpty()) {
      long currentTime = System.currentTimeMillis();
      Iterator<Map.Entry<String, Future<BucketDeletedBytesMetrics>>> iterator =
          context.futures.entrySet().iterator();
      boolean processedAny = false;
      while (iterator.hasNext()) {
        Map.Entry<String, Future<BucketDeletedBytesMetrics>> entry = iterator.next();
        String key = entry.getKey();
        Future<BucketDeletedBytesMetrics> future = entry.getValue();
        if (checkAndHandleTimeout(key, future, context, currentTime)) {
          iterator.remove();
          processedAny = true;
          continue;
        }
        if (future.isDone()) {
          handleCompletedFuture(key, future, context);
          iterator.remove();
          processedAny = true;
        }
      }
      if (!context.futures.isEmpty() && !processedAny) {
        try {
          Thread.sleep(POLL_INTERVAL_MS);
        } catch (InterruptedException e) {
          LOG.warn("Collection polling interrupted");
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  private boolean checkAndHandleTimeout(
      String key, Future<BucketDeletedBytesMetrics> future, CollectionContext context, long currentTime) {
    long elapsedTime = currentTime - context.submissionTime;
    if (elapsedTime > maximumTaskTimeout && !future.isDone()) {
      LOG.warn("Task for bucket {} timed out after {}ms", key, elapsedTime);
      future.cancel(true);
      context.failed++;
      return true;
    }
    return false;
  }

  private void handleCompletedFuture(
      String key, Future<BucketDeletedBytesMetrics> future, CollectionContext context) {
    try {
      BucketDeletedBytesMetrics result = future.get();
      if (result.getPurgeableBytes() < 0 && result.getSnapshotTrappedBytes() < 0) {
        context.failed++;
      } else {
        context.totalPurgeableSize += Math.max(result.getPurgeableBytes(), 0L);
        context.totalSnapshotTrappedSize += Math.max(result.getSnapshotTrappedBytes(), 0L);
        context.totalPurgeableKeys += Math.max(result.getPurgeableKeys(), 0L);
        context.totalPurgeableDirs += Math.max(result.getPurgeableDirs(), 0L);
        // An OM that predates the key/dir bytes split reports -1; the split total is then unknown.
        if (result.getPurgeableKeyBytes() < 0 || result.getPurgeableDirBytes() < 0) {
          context.purgeableSplitUnknown = true;
        } else {
          context.totalPurgeableKeySize += result.getPurgeableKeyBytes();
          context.totalPurgeableDirSize += result.getPurgeableDirBytes();
        }
      }
      context.results.put(key, result);
      LOG.debug("Processed deleted bytes result for bucket {}", key);
    } catch (ExecutionException | InterruptedException e) {
      String errorType = e instanceof InterruptedException ? "interrupted" : "execution failed";
      LOG.error("Task {} for bucket {} failed", errorType, key, e);
      context.failed++;
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void updateFinalState(CollectionContext context) {
    synchronized (this) {
      perBucketMetrics = context.results;
      totalPurgeableSize = context.totalPurgeableSize;
      totalSnapshotTrappedSize = context.totalSnapshotTrappedSize;
      totalPurgeableKeys = context.totalPurgeableKeys;
      totalPurgeableDirs = context.totalPurgeableDirs;
      totalPurgeableKeySize = context.purgeableSplitUnknown ? -1L : context.totalPurgeableKeySize;
      totalPurgeableDirSize = context.purgeableSplitUnknown ? -1L : context.totalPurgeableDirSize;
      totalBucketsQueried = context.totalQueried;
      totalBucketQueriesFailed = context.failed;
      currentStatus = MetricCollectionStatus.FINISHED;
      isRunning.set(false);
      lastCollectionEndTime.set(System.currentTimeMillis());
    }

    LOG.debug("Bucket deleted bytes collection completed. Queried: {}, Failed: {}",
        context.totalQueried, context.failed);
  }

  private void resetState() {
    perBucketMetrics = new HashMap<>();
    totalPurgeableSize = 0L;
    totalSnapshotTrappedSize = 0L;
    totalPurgeableKeys = 0L;
    totalPurgeableDirs = 0L;
    totalPurgeableKeySize = 0L;
    totalPurgeableDirSize = 0L;
    totalBucketsQueried = 0;
    totalBucketQueriesFailed = 0;
  }

  /**
   * Returns either {@link BucketDeletedBytesCompleteResponse} when collection is
   * finished, or {@link BucketDeletedBytesProgressResponse} otherwise.
   */
  public Object getCollectedMetrics() {
    startTask();
    synchronized (this) {
      if (currentStatus == MetricCollectionStatus.FINISHED) {
        return new BucketDeletedBytesCompleteResponse(
            currentStatus,
            totalBucketsQueried,
            totalBucketQueriesFailed,
            totalPurgeableSize,
            totalSnapshotTrappedSize,
            totalPurgeableKeys,
            totalPurgeableDirs,
            totalPurgeableKeySize,
            totalPurgeableDirSize);
      }

      return new BucketDeletedBytesProgressResponse(
          currentStatus,
          buildProgressMessage(currentStatus, failedMessage));
    }
  }

  /**
   * Returns the per-bucket ("volume/bucket" -&gt; metrics) collected so far, along with the
   * current collection status. Buckets not yet collected are simply absent from the map.
   */
  public Map<String, BucketDeletedBytesMetrics> getPerBucketMetrics() {
    startTask();
    return perBucketMetrics;
  }

  public MetricCollectionStatus getCurrentStatus() {
    return currentStatus;
  }

  private static String buildProgressMessage(MetricCollectionStatus status, String failedMessage) {
    switch (status) {
    case IN_PROGRESS:
      return "Bucket deleted bytes collection task is currently running. Please wait for task to finish.";
    case FAILED:
      return failedMessage;
    case NOT_STARTED:
      return "Bucket deleted bytes collection task has not started yet. Please retry shortly.";
    default:
      return "Bucket deleted bytes collection task is not complete yet. Please retry shortly.";
    }
  }

  @PreDestroy
  public void shutdown() {
    LOG.info("Shutting down BucketDeletedBytesMetricsService");
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Status of metric collection task.
   */
  public enum MetricCollectionStatus {
    NOT_STARTED, IN_PROGRESS, FINISHED, FAILED
  }

  private static class CollectionContext {
    private final int totalQueried;
    private final Map<String, Future<BucketDeletedBytesMetrics>> futures;
    private final Map<String, BucketDeletedBytesMetrics> results = new HashMap<>();
    private final long submissionTime;
    private long totalPurgeableSize = 0L;
    private long totalSnapshotTrappedSize = 0L;
    private long totalPurgeableKeys = 0L;
    private long totalPurgeableDirs = 0L;
    private long totalPurgeableKeySize = 0L;
    private long totalPurgeableDirSize = 0L;
    private boolean purgeableSplitUnknown = false;
    private int failed = 0;

    CollectionContext(
        int totalQueried,
        Map<String, Future<BucketDeletedBytesMetrics>> futures,
        long submissionTime) {
      this.totalQueried = totalQueried;
      this.futures = futures;
      this.submissionTime = submissionTime;
    }
  }
}
