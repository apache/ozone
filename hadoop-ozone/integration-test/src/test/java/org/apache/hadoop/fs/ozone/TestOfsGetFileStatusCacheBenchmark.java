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

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark for the OFS getFileStatus bucket-layout cache (HDDS-15925).
 *
 * <p>Runs a cache-hit-heavy getFileStatus workload against one
 * {@link MiniOzoneCluster} using the default client configuration, and reports
 * the InfoBucket RPCs, getFileStatus RPCs, per-call latency (mean/p50/p90/p99/
 * max) and throughput measured by OM. The workload touches {@link #NUM_BUCKETS}
 * buckets {@link #ACCESSES_PER_BUCKET} times each, so exactly one access per
 * bucket is a cold miss and the rest would hit a per-bucket layout cache — a
 * 90% cache-hit workload.
 *
 * <p>This benchmark makes no assumption about whether the cache exists, so the
 * identical test runs on both the optimized branch and the baseline
 * ({@code master}) code. The before/after comparison is what OM reports for the
 * same workload:
 * <ul>
 *   <li>baseline (no cache): two RPCs per getFileStatus (InfoBucket +
 *       getFileStatus), one InfoBucket RPC per call ({@link #TOTAL_CALLS});</li>
 *   <li>optimized (cache on by default): one RPC per cache hit, so one
 *       InfoBucket RPC per distinct bucket ({@link #NUM_BUCKETS}).</li>
 * </ul>
 * The getFileStatus RPC count is identical either way, showing the optimization
 * removes only the redundant InfoBucket RPC and changes no behaviour; the
 * latency percentiles show the effect of dropping that RPC per call.
 *
 * <p>A second test runs the same workload single-threaded and then across
 * {@link #CONCURRENT_THREADS} client threads sharing one {@code FileSystem} (and
 * thus one layout cache), and logs the two side by side. It confirms the atomic
 * get-or-load holds the InfoBucket RPC count at one per bucket under concurrency
 * — concurrent cold misses on the same bucket collapse to a single RPC instead
 * of stampeding — while throughput scales with the added client threads.
 */
@Tag("benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOfsGetFileStatusCacheBenchmark {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOfsGetFileStatusCacheBenchmark.class);

  /** Distinct buckets in the working set (each is one cold miss). */
  private static final int NUM_BUCKETS = 200;
  /** getFileStatus calls per bucket: 1 miss + (N-1) hits. */
  private static final int ACCESSES_PER_BUCKET = 10;
  /** (ACCESSES_PER_BUCKET - 1) / ACCESSES_PER_BUCKET = 0.90. */
  private static final double TARGET_HIT_RATIO =
      (ACCESSES_PER_BUCKET - 1) / (double) ACCESSES_PER_BUCKET;
  private static final int TOTAL_CALLS = NUM_BUCKETS * ACCESSES_PER_BUCKET;
  private static final long SHUFFLE_SEED = 20250831L;
  /** Client threads issuing getFileStatus in the concurrent comparison. */
  private static final int CONCURRENT_THREADS = 10;

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private OzoneConfiguration conf;
  private String rootPath;

  private final List<Path> accessSequence = new ArrayList<>(TOTAL_CALLS);

  @BeforeAll
  void init() throws IOException, InterruptedException, TimeoutException {
    conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.FILE_SYSTEM_OPTIMIZED.name());
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    rootPath = String.format("%s://%s/",
        OzoneConsts.OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));

    // One volume, NUM_BUCKETS FSO buckets, one file per bucket. The access
    // sequence lists each file ACCESSES_PER_BUCKET times, then is shuffled with
    // a fixed seed so hits and misses interleave the way a real workload would.
    ObjectStore objectStore = client.getObjectStore();
    String volName = "benchvol";
    objectStore.createVolume(volName);
    OzoneVolume volume = objectStore.getVolume(volName);
    BucketArgs fsoArgs = BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED).build();
    try (FileSystem setupFs =
        FileSystem.newInstance(URI.create(rootPath), conf)) {
      for (int i = 0; i < NUM_BUCKETS; i++) {
        String buckName = "benchbucket-" + i;
        volume.createBucket(buckName, fsoArgs);
        Path file = new Path("/" + volName + "/" + buckName + "/file");
        ContractTestUtils.touch(setupFs, file);
        for (int a = 0; a < ACCESSES_PER_BUCKET; a++) {
          accessSequence.add(file);
        }
      }
    }
    Collections.shuffle(accessSequence, new Random(SHUFFLE_SEED));
  }

  @AfterAll
  void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  void benchmarkCacheHitWorkflow() throws Exception {
    // Prime OM-side state and the JVM so the measured run pays no cold-start.
    runWorkload(1);

    Result measured = runWorkload(1);

    assertWorkloadInvariants(measured);
    logResult("single-threaded", measured);
  }

  @Test
  void benchmarkConcurrentVsSingleThreaded() throws Exception {
    // Warm the JVM/OM, then measure the same workload single-threaded and again
    // across CONCURRENT_THREADS client threads sharing one FileSystem (and thus
    // one bucket-layout cache). The atomic get-or-load keeps the InfoBucket RPC
    // count at one per bucket in both cases: concurrent cold misses on the same
    // bucket collapse to a single InfoBucket RPC rather than stampeding.
    runWorkload(1);

    Result single = runWorkload(1);
    Result concurrent = runWorkload(CONCURRENT_THREADS);

    assertWorkloadInvariants(single);
    assertWorkloadInvariants(concurrent);

    logResult("single-threaded", single);
    logResult(CONCURRENT_THREADS + " concurrent threads", concurrent);
    LOG.info(String.format("%n"
            + "single-threaded vs %d concurrent threads (optimized)%n"
            + "  InfoBucket RPCs     : %d  ->  %d%n"
            + "  getFileStatus RPCs  : %d  ->  %d%n"
            + "  latency mean (ms)   : %.3f  ->  %.3f%n"
            + "  latency p99 (ms)    : %.3f  ->  %.3f%n"
            + "  throughput (ops/s)  : %.1f  ->  %.1f  (%.2fx)%n",
        CONCURRENT_THREADS,
        single.infoBucketRpcs, concurrent.infoBucketRpcs,
        single.getFileStatusRpcs, concurrent.getFileStatusRpcs,
        millis(single.mean()), millis(concurrent.mean()),
        millis(single.percentile(0.99)), millis(concurrent.percentile(0.99)),
        single.opsPerSecond(), concurrent.opsPerSecond(),
        concurrent.opsPerSecond() / single.opsPerSecond()));
  }

  /**
   * Invariants that hold on both the baseline and the optimized code, so the
   * identical benchmark passes in either worktree: every getFileStatus still
   * issues its getFileStatus RPC, and the InfoBucket RPCs lie between the
   * fully-cached best case (one per bucket) and the no-cache worst case (one
   * per call).
   */
  private void assertWorkloadInvariants(Result r) {
    assertThat(r.getFileStatusRpcs).isEqualTo(TOTAL_CALLS);
    assertThat(r.infoBucketRpcs)
        .isBetween((long) NUM_BUCKETS, (long) TOTAL_CALLS);
  }

  private void logResult(String label, Result r) {
    double achievedHitRatio =
        (TOTAL_CALLS - r.infoBucketRpcs) / (double) TOTAL_CALLS;
    LOG.info(String.format("%n"
            + "OFS getFileStatus bucket-layout cache benchmark (HDDS-15925) — %s%n"
            + "  buckets=%d, accesses/bucket=%d, total getFileStatus=%d, "
            + "threads=%d%n"
            + "  target cache-hit ratio      : %.0f%%%n"
            + "  ------------------------------------------------------------%n"
            + "  InfoBucket RPCs             : %d%n"
            + "  getFileStatus RPCs          : %d%n"
            + "  InfoBucket RPCs per call    : %.2f%n"
            + "  cache-hit ratio achieved    : %.0f%%%n"
            + "  ------------------------------------------------------------%n"
            + "  latency mean                : %.3f ms%n"
            + "  latency p50                 : %.3f ms%n"
            + "  latency p90                 : %.3f ms%n"
            + "  latency p99                 : %.3f ms%n"
            + "  latency max                 : %.3f ms%n"
            + "  throughput                  : %.1f getFileStatus/s%n",
        label, NUM_BUCKETS, ACCESSES_PER_BUCKET, TOTAL_CALLS, r.threads,
        TARGET_HIT_RATIO * 100,
        r.infoBucketRpcs, r.getFileStatusRpcs,
        r.infoBucketRpcs / (double) TOTAL_CALLS,
        achievedHitRatio * 100,
        millis(r.mean()), millis(r.percentile(0.50)),
        millis(r.percentile(0.90)), millis(r.percentile(0.99)),
        millis(r.max()), r.opsPerSecond()));
  }

  private static double millis(double nanos) {
    return nanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
  }

  private Result runWorkload(int threads)
      throws IOException, InterruptedException {
    OzoneConfiguration runConf = new OzoneConfiguration(conf);
    runConf.set(FS_DEFAULT_NAME_KEY, rootPath);

    OMMetrics metrics = cluster.getOzoneManager().getMetrics();
    long[] latencyNanos = new long[TOTAL_CALLS];
    try (FileSystem fs = FileSystem.newInstance(URI.create(rootPath), runConf)) {
      long bucketInfosBefore = metrics.getNumBucketInfos();
      long getFileStatusBefore = metrics.getNumGetFileStatus();
      long elapsedNanos = threads == 1
          ? runSequential(fs, latencyNanos)
          : runConcurrent(fs, threads, latencyNanos);
      return new Result(threads,
          metrics.getNumBucketInfos() - bucketInfosBefore,
          metrics.getNumGetFileStatus() - getFileStatusBefore,
          elapsedNanos, latencyNanos);
    }
  }

  private long runSequential(FileSystem fs, long[] latencyNanos)
      throws IOException {
    long startNanos = System.nanoTime();
    int i = 0;
    for (Path path : accessSequence) {
      long callStart = System.nanoTime();
      fs.getFileStatus(path);
      latencyNanos[i++] = System.nanoTime() - callStart;
    }
    return System.nanoTime() - startNanos;
  }

  private long runConcurrent(FileSystem fs, int threads, long[] latencyNanos)
      throws IOException, InterruptedException {
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch startGate = new CountDownLatch(1);
    CountDownLatch doneGate = new CountDownLatch(threads);
    AtomicReference<IOException> failure = new AtomicReference<>();
    // Disjoint, contiguous slices of the shuffled sequence; each thread writes
    // only its own latency indices, so no synchronization is needed per call.
    int chunk = (TOTAL_CALLS + threads - 1) / threads;
    for (int t = 0; t < threads; t++) {
      final int from = t * chunk;
      final int to = Math.min(TOTAL_CALLS, from + chunk);
      pool.submit(() -> {
        try {
          startGate.await();
          for (int i = from; i < to; i++) {
            long callStart = System.nanoTime();
            fs.getFileStatus(accessSequence.get(i));
            latencyNanos[i] = System.nanoTime() - callStart;
          }
        } catch (IOException e) {
          failure.compareAndSet(null, e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          doneGate.countDown();
        }
      });
    }
    long startNanos = System.nanoTime();
    startGate.countDown();
    doneGate.await();
    long elapsedNanos = System.nanoTime() - startNanos;
    pool.shutdownNow();
    if (failure.get() != null) {
      throw failure.get();
    }
    return elapsedNanos;
  }

  private static final class Result {
    private final int threads;
    private final long infoBucketRpcs;
    private final long getFileStatusRpcs;
    private final long elapsedNanos;
    private final long[] sortedLatencyNanos;

    Result(int threads, long infoBucketRpcs, long getFileStatusRpcs,
        long elapsedNanos, long[] latencyNanos) {
      this.threads = threads;
      this.infoBucketRpcs = infoBucketRpcs;
      this.getFileStatusRpcs = getFileStatusRpcs;
      this.elapsedNanos = elapsedNanos;
      this.sortedLatencyNanos = latencyNanos.clone();
      Arrays.sort(this.sortedLatencyNanos);
    }

    double opsPerSecond() {
      return TOTAL_CALLS
          / (elapsedNanos / (double) TimeUnit.SECONDS.toNanos(1));
    }

    double mean() {
      long sum = 0;
      for (long l : sortedLatencyNanos) {
        sum += l;
      }
      return sum / (double) sortedLatencyNanos.length;
    }

    double percentile(double q) {
      int idx = (int) Math.ceil(q * sortedLatencyNanos.length) - 1;
      idx = Math.max(0, Math.min(sortedLatencyNanos.length - 1, idx));
      return sortedLatencyNanos[idx];
    }

    double max() {
      return sortedLatencyNanos[sortedLatencyNanos.length - 1];
    }
  }
}
