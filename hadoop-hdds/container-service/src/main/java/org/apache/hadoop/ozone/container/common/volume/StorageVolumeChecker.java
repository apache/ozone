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

package org.apache.hadoop.ozone.container.common.volume;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that encapsulates running disk checks against each volume and
 * allows retrieving a list of failed volumes. The class only detects failed
 * volumes and handling of failed volumes is responsibility of caller.
 */
public class StorageVolumeChecker {

  public static final int MAX_VOLUME_FAILURE_TOLERATED_LIMIT = -1;

  private static final Logger LOG =
      LoggerFactory.getLogger(StorageVolumeChecker.class);

  private AsyncChecker<Boolean, VolumeCheckResult> delegateChecker;

  private final BackgroundVolumeScannerMetrics metrics;

  /**
   * Max allowed time for a disk check in milliseconds. If the check
   * doesn't complete within this time we declare the disk as dead.
   */
  private final long maxAllowedTimeForCheckMs;

  /**
   * Minimum time between two successive disk checks of a volume.
   */
  private final long minDiskCheckGapMs;

  /**
   * Timestamp of the last check of all volume sets.
   */
  private long lastAllVolumeSetsCheckComplete;

  private final Timer timer;

  private final ExecutorService checkVolumeResultHandlerExecutorService;

  private final DatanodeConfiguration dnConf;

  /**
   * An executor for periodic disk checks.
   */
  private final ScheduledExecutorService diskCheckerservice;
  private ScheduledFuture<?> periodicDiskChecker;
  private final List<VolumeSet> registeredVolumeSets;

  private final AtomicBoolean started;

  /**
   * @param conf  Configuration object.
   * @param timer {@link Timer} object used for throttling checks.
   */
  public StorageVolumeChecker(ConfigurationSource conf, Timer timer,
      String threadNamePrefix) {

    metrics = BackgroundVolumeScannerMetrics.create();

    this.timer = timer;

    dnConf = conf.getObject(DatanodeConfiguration.class);

    maxAllowedTimeForCheckMs = dnConf.getDiskCheckTimeout().toMillis();

    minDiskCheckGapMs = dnConf.getDiskCheckMinGap().toMillis();

    lastAllVolumeSetsCheckComplete = timer.monotonicNow() - minDiskCheckGapMs;

    registeredVolumeSets = new ArrayList<>();

    delegateChecker = new ThrottledAsyncChecker<>(
        timer, minDiskCheckGapMs, maxAllowedTimeForCheckMs,
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setNameFormat(threadNamePrefix + "DataNodeDiskChecker" +
                    "Thread-%d")
                .setDaemon(true)
                .build()));

    checkVolumeResultHandlerExecutorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
            .setNameFormat(threadNamePrefix + "VolumeCheckResultHandler" +
                "Thread-%d")
            .setDaemon(true)
            .build());

    ThreadFactory threadFactory = r -> {
      Thread t = new Thread(r, threadNamePrefix + "PeriodicHDDSVolumeChecker");
      t.setDaemon(true);
      return t;
    };
    this.diskCheckerservice = Executors.newSingleThreadScheduledExecutor(
        threadFactory);

    started = new AtomicBoolean(false);
  }

  public void start() {
    if (started.compareAndSet(false, true)) {
      long periodicDiskCheckIntervalMinutes =
          dnConf.getPeriodicDiskCheckIntervalMinutes();
      if (periodicDiskCheckIntervalMinutes < 0) {
        LOG.warn("StorageVolumeChecker is DISABLED because " +
            "hdds.datanode.periodic.disk.check.interval.minutes = {} (negative).", periodicDiskCheckIntervalMinutes);
        return;
      }
      periodicDiskChecker =
          diskCheckerservice.scheduleWithFixedDelay(this::checkAllVolumeSets,
              periodicDiskCheckIntervalMinutes,
              periodicDiskCheckIntervalMinutes, TimeUnit.MINUTES);
    }
  }

  public synchronized void registerVolumeSet(VolumeSet volumeSet) {
    registeredVolumeSets.add(volumeSet);
  }

  public synchronized void checkAllVolumeSets() {
    final long gap = timer.monotonicNow() - lastAllVolumeSetsCheckComplete;
    if (gap < minDiskCheckGapMs) {
      metrics.incNumIterationsSkipped();
      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Skipped checking all volumes, time since last check {} is less " +
                "than the minimum gap between checks ({} ms).",
            gap, minDiskCheckGapMs);
      }
      return;
    }

    try {
      long totalVolumesScanned = 0;
      for (VolumeSet volSet : registeredVolumeSets) {
        long volumeCount = volSet.getVolumesList().size();
        if (volSet instanceof MutableVolumeSet) {
          StorageVolume.VolumeType type = ((MutableVolumeSet) volSet).getVolumeType();
          switch (type) {
          case DATA_VOLUME:
            metrics.incNumDataVolumeScans(volumeCount);
            break;
          case META_VOLUME:
            metrics.incNumMetadataVolumeScans(volumeCount);
            break;
          default:
            LOG.warn("Unknown volume type: {}", type);
            break;
          }
        }
        volSet.checkAllVolumes(this);
        totalVolumesScanned += volSet.getVolumesList().size();
      }
      metrics.setNumVolumesScannedInLastIteration(totalVolumesScanned);
      metrics.incNumScanIterations();
      lastAllVolumeSetsCheckComplete = timer.monotonicNow();
    } catch (IOException e) {
      LOG.warn("Exception while checking disks", e);
    }
  }

  /**
   * Run checks against all volumes.
   * <p>
   * This check may be performed at service startup and subsequently at
   * regular intervals to detect and handle failed volumes.
   *
   * @param volumes - Set of volumes to be checked. This set must be immutable
   *                for the duration of the check else the results will be
   *                unexpected.
   * @return set of failed volumes.
   */
  public Set<? extends StorageVolume> checkAllVolumes(
      Collection<? extends StorageVolume> volumes)
      throws InterruptedException {
    final Set<StorageVolume> healthyVolumes = ConcurrentHashMap.newKeySet();
    final Set<StorageVolume> failedVolumes = ConcurrentHashMap.newKeySet();
    final Set<StorageVolume> allVolumes = new HashSet<>();

    final AtomicLong numVolumes = new AtomicLong(volumes.size());
    final CountDownLatch latch = new CountDownLatch(1);

    for (StorageVolume v : volumes) {
      Optional<ListenableFuture<VolumeCheckResult>> olf =
          delegateChecker.schedule(v, null);
      if (olf.isPresent()) {
        LOG.info("Scheduled health check for volume {}", v);
        allVolumes.add(v);
        Futures.addCallback(olf.get(),
            new ResultHandler(v, healthyVolumes, failedVolumes,
                numVolumes, (ignored1, ignored2) -> latch.countDown()),
            MoreExecutors.directExecutor());
      } else {
        if (v instanceof HddsVolume) {
          ((HddsVolume) v).getVolumeInfoStats().incNumScansSkipped();
        }
        if (numVolumes.decrementAndGet() == 0) {
          latch.countDown();
        }
      }
    }

    // Wait until our timeout elapses, after which we give up on
    // the remaining volumes.
    if (!latch.await(maxAllowedTimeForCheckMs, TimeUnit.MILLISECONDS)) {
      LOG.warn("checkAllVolumes timed out after {} ms",
          maxAllowedTimeForCheckMs);
    }

    synchronized (this) {
      // All volumes that have not been detected as healthy should be
      // considered failed. This is a superset of 'failedVolumes'.
      //
      // Make a copy under the mutex as Sets.difference() returns a view
      // of a potentially changing set.
      return new HashSet<>(Sets.difference(allVolumes, healthyVolumes));
    }
  }

  /**
   * A callback interface that is supplied the result of running an
   * async disk check on multiple volumes.
   */
  public interface Callback {
    /**
     * @param healthyVolumes set of volumes that passed disk checks.
     * @param failedVolumes  set of volumes that failed disk checks.
     */
    void call(Set<StorageVolume> healthyVolumes,
        Set<StorageVolume> failedVolumes) throws IOException;
  }

  /**
   * Check a single volume asynchronously, returning a {@link ListenableFuture}
   * that can be used to retrieve the final result.
   * <p>
   * If the volume cannot be referenced then it is already closed and
   * cannot be checked. No error is propagated to the callback.
   *
   * @param volume   the volume that is to be checked.
   * @param callback callback to be invoked when the volume check completes.
   * @return true if the check was scheduled and the callback will be invoked.
   * false otherwise.
   */
  public boolean checkVolume(final StorageVolume volume, Callback callback) {
    if (volume == null) {
      LOG.debug("Cannot schedule check on null volume");
      return false;
    }

    Optional<ListenableFuture<VolumeCheckResult>> olf =
        delegateChecker.schedule(volume, null);
    if (olf.isPresent()) {
      Futures.addCallback(olf.get(),
          new ResultHandler(volume,
              ConcurrentHashMap.newKeySet(), ConcurrentHashMap.newKeySet(),
              new AtomicLong(1), callback),
          checkVolumeResultHandlerExecutorService
      );
      return true;
    }
    return false;
  }

  /**
   * A callback to process the results of checking a volume.
   */
  private static class ResultHandler
      implements FutureCallback<VolumeCheckResult> {
    private final StorageVolume volume;
    private final Set<StorageVolume> failedVolumes;
    private final Set<StorageVolume> healthyVolumes;
    private final AtomicLong volumeCounter;

    @Nullable
    private final Callback callback;

    /**
     * @param healthyVolumes set of healthy volumes. If the disk check is
     *                       successful, add the volume here.
     * @param failedVolumes  set of failed volumes. If the disk check fails,
     *                       add the volume here.
     * @param volumeCounter  volumeCounter used to trigger callback invocation.
     * @param callback       invoked when the volumeCounter reaches 0.
     */
    ResultHandler(StorageVolume volume,
        Set<StorageVolume> healthyVolumes,
        Set<StorageVolume> failedVolumes,
        AtomicLong volumeCounter,
        @Nullable Callback callback) {
      this.volume = volume;
      this.healthyVolumes = healthyVolumes;
      this.failedVolumes = failedVolumes;
      this.volumeCounter = volumeCounter;
      this.callback = callback;
    }

    @Override
    public void onSuccess(@Nullable VolumeCheckResult result) {
      if (result == null) {
        LOG.error("Unexpected empty health check result for volume {}", volume);
        markHealthy();
      } else {
        switch (result) {
        case HEALTHY:
        case DEGRADED:
          // Ozone does not currently use this state.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Volume {} is {}.", volume, result);
          }
          markHealthy();
          break;
        case FAILED:
          LOG.warn("Volume {} detected as being unhealthy", volume);
          markFailed();
          break;
        default:
          LOG.error("Unexpected health check result {} for volume {}", result,
              volume);
          markHealthy();
          break;
        }
      }
      cleanup();
    }

    @Override
    public void onFailure(@Nonnull Throwable t) {
      Throwable exception = (t instanceof ExecutionException) ?
          t.getCause() : t;
      LOG.warn("Exception running disk checks against volume {}",
          volume, exception);
      // If the scan was interrupted, do not count it as a volume failure.
      // This should only happen if the volume checker is being shut down.
      if (!(t instanceof InterruptedException)) {
        markFailed();
        cleanup();
      }
    }

    private void markHealthy() {
      healthyVolumes.add(volume);
    }

    private void markFailed() {
      failedVolumes.add(volume);
    }

    private void cleanup() {
      invokeCallback();
    }

    private void invokeCallback() {
      try {
        final long remaining = volumeCounter.decrementAndGet();
        if (callback != null && remaining == 0) {
          callback.call(healthyVolumes, failedVolumes);
        }
      } catch (Exception e) {
        // Propagating this exception is unlikely to be helpful.
        LOG.warn("Unexpected exception", e);
      }
    }
  }

  /**
   * Shutdown the checker and its associated ExecutorService.
   * <p>
   * See {@link ExecutorService#awaitTermination} for the interpretation
   * of the parameters.
   */
  public void shutdownAndWait(int gracePeriod, TimeUnit timeUnit) {
    if (started.compareAndSet(true, false)) {
      if (periodicDiskChecker != null) {
        periodicDiskChecker.cancel(true);
      }
      diskCheckerservice.shutdownNow();
      checkVolumeResultHandlerExecutorService.shutdownNow();
      metrics.unregister();
      try {
        delegateChecker.shutdownAndWait(gracePeriod, timeUnit);
      } catch (InterruptedException e) {
        LOG.warn("{} interrupted during shutdown.",
            this.getClass().getSimpleName());
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * This method is for testing only.
   *
   * @param testDelegate
   */
  @VisibleForTesting
  void setDelegateChecker(
      AsyncChecker<Boolean, VolumeCheckResult> testDelegate) {
    delegateChecker = testDelegate;
  }

  @VisibleForTesting
  public BackgroundVolumeScannerMetrics getMetrics() {
    return metrics;
  }
}
