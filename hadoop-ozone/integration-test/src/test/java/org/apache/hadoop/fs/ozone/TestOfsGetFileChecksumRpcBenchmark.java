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
 * Benchmark for the OFS getFileChecksum OM RPC count (HDDS-15951).
 *
 * <p>Adapted from the benchmark in PR #11369 so the same workload runs unchanged on the baseline
 * ({@code master}: InfoVolume + InfoBucket + LookupKey) and on the server-side fix (a single LookupFile).
 * The only changes are in the accounting: key reads are counted as LookupKey + LookupFile, because the fix
 * moves the read from one to the other, and the InfoBucket invariant no longer assumes at least one RPC per
 * bucket, because the fix removes that RPC entirely instead of caching it.
 *
 * <p>{@link #NUM_BUCKETS} buckets are touched {@link #ACCESSES_PER_BUCKET} times each from a shuffled access
 * sequence, which is the shape PR #11369's per-bucket layout cache was built for (90% of calls would hit it).
 * Keeping that shape makes the three arms directly comparable. PR #11369 drops InfoVolume unconditionally and
 * caches InfoBucket, so it costs (2 - hitRatio) RPCs per call: 1.10 at this workload's 90% hit ratio, rising to
 * 2.00 for a cold or scanning client. The server-side fix costs exactly 1.00 regardless of hit ratio.
 */
@Tag("benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOfsGetFileChecksumRpcBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(TestOfsGetFileChecksumRpcBenchmark.class);

  /** Distinct buckets in the working set. */
  private static final int NUM_BUCKETS = 200;
  /** getFileChecksum calls per bucket. */
  private static final int ACCESSES_PER_BUCKET = 10;
  private static final int TOTAL_CALLS = NUM_BUCKETS * ACCESSES_PER_BUCKET;
  private static final long SHUFFLE_SEED = 20250831L;
  /** Client threads issuing getFileChecksum in the concurrent comparison. */
  private static final int CONCURRENT_THREADS = 10;
  /** Size of each file, so every call computes a real checksum over one small block. */
  private static final int FILE_SIZE = 4 * 1024;

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private OzoneConfiguration conf;
  private String rootPath;

  private final List<Path> accessSequence = new ArrayList<>(TOTAL_CALLS);

  @BeforeAll
  void init() throws IOException, InterruptedException, TimeoutException {
    conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT, BucketLayout.FILE_SYSTEM_OPTIMIZED.name());
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    rootPath = String.format("%s://%s/", OzoneConsts.OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));

    ObjectStore objectStore = client.getObjectStore();
    String volName = "benchvol";
    objectStore.createVolume(volName);
    OzoneVolume volume = objectStore.getVolume(volName);
    BucketArgs fsoArgs = BucketArgs.newBuilder().setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED).build();
    byte[] data = ContractTestUtils.dataset(FILE_SIZE, 'a', 26);
    try (FileSystem setupFs = FileSystem.newInstance(URI.create(rootPath), conf)) {
      for (int i = 0; i < NUM_BUCKETS; i++) {
        String buckName = "benchbucket-" + i;
        volume.createBucket(buckName, fsoArgs);
        Path file = new Path("/" + volName + "/" + buckName + "/file");
        ContractTestUtils.createFile(setupFs, file, true, data);
        for (int a = 0; a < ACCESSES_PER_BUCKET; a++) {
          accessSequence.add(file);
        }
      }
      assertThat(setupFs.getFileChecksum(accessSequence.get(0))).isNotNull();
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
  void benchmarkGetFileChecksum() throws Exception {
    // Prime OM-side state and the JVM so the measured run pays no cold-start.
    runWorkload(1);

    Result single = runWorkload(1);
    Result concurrent = runWorkload(CONCURRENT_THREADS);

    assertWorkloadInvariants(single);
    assertWorkloadInvariants(concurrent);

    logResult("single-threaded", single);
    logResult(CONCURRENT_THREADS + " concurrent threads", concurrent);
  }

  /**
   * Invariants that hold on both the baseline and the fixed code, so the identical benchmark passes in either
   * worktree: every getFileChecksum still issues exactly one key-read RPC, whether that is LookupKey (baseline)
   * or LookupFile (fixed), and the volume and bucket RPCs stay within one per call.
   */
  private void assertWorkloadInvariants(Result r) {
    assertThat(r.lookupKeyRpcs + r.lookupFileRpcs).isEqualTo(TOTAL_CALLS);
    assertThat(r.bucketInfoRpcs).isBetween(0L, (long) TOTAL_CALLS);
    assertThat(r.volumeInfoRpcs).isBetween(0L, (long) TOTAL_CALLS);
  }

  private void logResult(String label, Result r) {
    long omRpcs = r.volumeInfoRpcs + r.bucketInfoRpcs + r.lookupKeyRpcs + r.lookupFileRpcs;
    LOG.info(String.format("%n"
            + "BENCHRESULT OFS getFileChecksum OM RPC benchmark (HDDS-15951) - %s%n"
            + "  buckets=%d, accesses/bucket=%d, total getFileChecksum=%d, threads=%d, file size=%d bytes%n"
            + "  ------------------------------------------------------------%n"
            + "  InfoVolume RPCs             : %d%n"
            + "  InfoBucket RPCs             : %d%n"
            + "  LookupKey RPCs              : %d%n"
            + "  LookupFile RPCs             : %d%n"
            + "  total OM RPCs               : %d%n"
            + "  OM RPCs per call            : %.2f%n"
            + "  ------------------------------------------------------------%n"
            + "  latency mean                : %.3f ms%n"
            + "  latency p50                 : %.3f ms%n"
            + "  latency p90                 : %.3f ms%n"
            + "  latency p99                 : %.3f ms%n"
            + "  latency max                 : %.3f ms%n"
            + "  throughput                  : %.1f getFileChecksum/s%n",
        label, NUM_BUCKETS, ACCESSES_PER_BUCKET, TOTAL_CALLS, r.threads, FILE_SIZE,
        r.volumeInfoRpcs, r.bucketInfoRpcs, r.lookupKeyRpcs, r.lookupFileRpcs,
        omRpcs, omRpcs / (double) TOTAL_CALLS,
        millis(r.mean()), millis(r.percentile(0.50)),
        millis(r.percentile(0.90)), millis(r.percentile(0.99)),
        millis(r.max()), r.opsPerSecond()));
  }

  private static double millis(double nanos) {
    return nanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
  }

  private Result runWorkload(int threads) throws IOException, InterruptedException {
    OzoneConfiguration runConf = new OzoneConfiguration(conf);
    runConf.set(FS_DEFAULT_NAME_KEY, rootPath);

    OMMetrics metrics = cluster.getOzoneManager().getMetrics();
    long[] latencyNanos = new long[TOTAL_CALLS];
    // A new FileSystem instance per run, so each run starts with no client-side state carried over.
    try (FileSystem fs = FileSystem.newInstance(URI.create(rootPath), runConf)) {
      long volumeInfosBefore = metrics.getNumVolumeInfos();
      long bucketInfosBefore = metrics.getNumBucketInfos();
      long keyLookupsBefore = metrics.getNumKeyLookups();
      long lookupFilesBefore = metrics.getNumLookupFile();
      long elapsedNanos = threads == 1
          ? runSequential(fs, latencyNanos)
          : runConcurrent(fs, threads, latencyNanos);
      return new Result(threads,
          metrics.getNumVolumeInfos() - volumeInfosBefore,
          metrics.getNumBucketInfos() - bucketInfosBefore,
          metrics.getNumKeyLookups() - keyLookupsBefore,
          metrics.getNumLookupFile() - lookupFilesBefore,
          elapsedNanos, latencyNanos);
    }
  }

  private long runSequential(FileSystem fs, long[] latencyNanos) throws IOException {
    long startNanos = System.nanoTime();
    int i = 0;
    for (Path path : accessSequence) {
      long callStart = System.nanoTime();
      fs.getFileChecksum(path);
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
    // Disjoint, contiguous slices of the shuffled sequence; each thread writes only its own latency indices, so no
    // synchronization is needed per call.
    int chunk = (TOTAL_CALLS + threads - 1) / threads;
    for (int t = 0; t < threads; t++) {
      final int from = t * chunk;
      final int to = Math.min(TOTAL_CALLS, from + chunk);
      pool.submit(() -> {
        try {
          startGate.await();
          for (int i = from; i < to; i++) {
            long callStart = System.nanoTime();
            fs.getFileChecksum(accessSequence.get(i));
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
    private final long volumeInfoRpcs;
    private final long bucketInfoRpcs;
    private final long lookupKeyRpcs;
    private final long lookupFileRpcs;
    private final long elapsedNanos;
    private final long[] sortedLatencyNanos;

    @SuppressWarnings("checkstyle:ParameterNumber")
    Result(int threads, long volumeInfoRpcs, long bucketInfoRpcs, long lookupKeyRpcs, long lookupFileRpcs,
        long elapsedNanos, long[] latencyNanos) {
      this.threads = threads;
      this.volumeInfoRpcs = volumeInfoRpcs;
      this.bucketInfoRpcs = bucketInfoRpcs;
      this.lookupKeyRpcs = lookupKeyRpcs;
      this.lookupFileRpcs = lookupFileRpcs;
      this.elapsedNanos = elapsedNanos;
      this.sortedLatencyNanos = latencyNanos.clone();
      Arrays.sort(this.sortedLatencyNanos);
    }

    double opsPerSecond() {
      return TOTAL_CALLS / (elapsedNanos / (double) TimeUnit.SECONDS.toNanos(1));
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
