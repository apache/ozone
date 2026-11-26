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
 * Benchmark for the OFS rename OM RPC count on FSO buckets (HDDS-16951).
 *
 * <p>The same workload runs unchanged on the baseline ({@code master}: InfoBucket + RenameKey per rename) and on the
 * fix (a single RenameKey that asks OM to reject a bucket that is not FILE_SYSTEM_OPTIMIZED). It follows
 * {@link TestOfsOpenRpcBenchmark}: each of {@link #NUM_BUCKETS} buckets holds one file, which is renamed back and
 * forth between two names {@link #ACCESSES_PER_BUCKET} times, in a shuffled order, single-threaded and with
 * {@link #CONCURRENT_THREADS} client threads. Each thread owns a disjoint set of buckets, so no two renames of the
 * same file run at once and every rename succeeds.
 */
@Tag("benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOfsRenameRpcBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(TestOfsRenameRpcBenchmark.class);

  /** Distinct buckets in the working set. */
  private static final int NUM_BUCKETS = 200;
  /** rename calls per bucket. */
  private static final int ACCESSES_PER_BUCKET = 10;
  private static final int TOTAL_CALLS = NUM_BUCKETS * ACCESSES_PER_BUCKET;
  private static final long SHUFFLE_SEED = 20250831L;
  /** Client threads issuing rename in the concurrent comparison; divides NUM_BUCKETS. */
  private static final int CONCURRENT_THREADS = 10;

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private OzoneConfiguration conf;
  private String rootPath;

  /** Per bucket, the two names the file is renamed between, and which of them it has now. */
  private final Path[][] names = new Path[NUM_BUCKETS][2];
  private final int[] current = new int[NUM_BUCKETS];

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
    try (FileSystem setupFs = FileSystem.newInstance(URI.create(rootPath), conf)) {
      for (int i = 0; i < NUM_BUCKETS; i++) {
        String buckName = "benchbucket-" + i;
        volume.createBucket(buckName, fsoArgs);
        names[i][0] = new Path("/" + volName + "/" + buckName + "/dir/file-a");
        names[i][1] = new Path("/" + volName + "/" + buckName + "/dir/file-b");
        ContractTestUtils.touch(setupFs, names[i][0]);
      }
      assertThat(setupFs.exists(names[0][0])).isTrue();
    }
  }

  @AfterAll
  void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  void benchmarkRename() throws Exception {
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
   * Invariants of the workload: every rename issues one RenameKey, and the bucket and volume RPCs stay within one per
   * rename.
   */
  private void assertWorkloadInvariants(Result r) {
    assertThat(r.renameRpcs).isEqualTo(TOTAL_CALLS);
    assertThat(r.bucketInfoRpcs).isBetween(0L, (long) TOTAL_CALLS);
    assertThat(r.volumeInfoRpcs).isBetween(0L, (long) TOTAL_CALLS);
  }

  private void logResult(String label, Result r) {
    long omRpcs = r.volumeInfoRpcs + r.bucketInfoRpcs + r.renameRpcs;
    LOG.info(String.format("%n"
            + "BENCHRESULT OFS rename OM RPC benchmark (HDDS-16951) - %s%n"
            + "  buckets=%d, accesses/bucket=%d, total rename=%d, threads=%d%n"
            + "  ------------------------------------------------------------%n"
            + "  InfoVolume RPCs             : %d%n"
            + "  InfoBucket RPCs             : %d%n"
            + "  RenameKey RPCs              : %d%n"
            + "  total OM RPCs               : %d%n"
            + "  OM RPCs per call            : %.2f%n"
            + "  ------------------------------------------------------------%n"
            + "  latency mean                : %.3f ms%n"
            + "  latency p50                 : %.3f ms%n"
            + "  latency p90                 : %.3f ms%n"
            + "  latency p99                 : %.3f ms%n"
            + "  latency max                 : %.3f ms%n"
            + "  throughput                  : %.1f rename/s%n",
        label, NUM_BUCKETS, ACCESSES_PER_BUCKET, TOTAL_CALLS, r.threads,
        r.volumeInfoRpcs, r.bucketInfoRpcs, r.renameRpcs,
        omRpcs, omRpcs / (double) TOTAL_CALLS,
        millis(r.mean()), millis(r.percentile(0.50)),
        millis(r.percentile(0.90)), millis(r.percentile(0.99)),
        millis(r.max()), r.opsPerSecond()));
  }

  private static double millis(double nanos) {
    return nanos / (double) TimeUnit.MILLISECONDS.toNanos(1);
  }

  /**
   * The bucket sequence of each thread: the buckets {@code b} with {@code b % threads == t}, each
   * {@link #ACCESSES_PER_BUCKET} times, shuffled.
   */
  private static List<List<Integer>> bucketSequences(int threads) {
    Random random = new Random(SHUFFLE_SEED);
    List<List<Integer>> sequences = new ArrayList<>(threads);
    for (int t = 0; t < threads; t++) {
      List<Integer> sequence = new ArrayList<>();
      for (int b = t; b < NUM_BUCKETS; b += threads) {
        for (int a = 0; a < ACCESSES_PER_BUCKET; a++) {
          sequence.add(b);
        }
      }
      Collections.shuffle(sequence, random);
      sequences.add(sequence);
    }
    return sequences;
  }

  /** Renames the file of the bucket to its other name; only the thread that owns the bucket calls this. */
  private void renameOnce(FileSystem fs, int bucket) throws IOException {
    int from = current[bucket];
    if (!fs.rename(names[bucket][from], names[bucket][1 - from])) {
      throw new IOException("rename failed: " + names[bucket][from]);
    }
    current[bucket] = 1 - from;
  }

  private Result runWorkload(int threads) throws IOException, InterruptedException {
    OzoneConfiguration runConf = new OzoneConfiguration(conf);
    runConf.set(FS_DEFAULT_NAME_KEY, rootPath);

    OMMetrics metrics = cluster.getOzoneManager().getMetrics();
    List<List<Integer>> sequences = bucketSequences(threads);
    long[] latencyNanos = new long[TOTAL_CALLS];
    // A new FileSystem instance per run, so each run starts with no client-side state carried over.
    try (FileSystem fs = FileSystem.newInstance(URI.create(rootPath), runConf)) {
      long volumeInfosBefore = metrics.getNumVolumeInfos();
      long bucketInfosBefore = metrics.getNumBucketInfos();
      long renamesBefore = metrics.getNumKeyRenames();
      long elapsedNanos = runThreads(fs, sequences, latencyNanos);
      return new Result(threads,
          metrics.getNumVolumeInfos() - volumeInfosBefore,
          metrics.getNumBucketInfos() - bucketInfosBefore,
          metrics.getNumKeyRenames() - renamesBefore,
          elapsedNanos, latencyNanos);
    }
  }

  private long runThreads(FileSystem fs, List<List<Integer>> sequences, long[] latencyNanos)
      throws IOException, InterruptedException {
    int threads = sequences.size();
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch startGate = new CountDownLatch(1);
    CountDownLatch doneGate = new CountDownLatch(threads);
    AtomicReference<IOException> failure = new AtomicReference<>();
    // Each thread writes only its own contiguous range of latency indices, so no synchronization is needed per call.
    int offset = 0;
    for (List<Integer> sequence : sequences) {
      final int from = offset;
      offset += sequence.size();
      pool.submit(() -> {
        try {
          startGate.await();
          int i = from;
          for (int bucket : sequence) {
            long callStart = System.nanoTime();
            renameOnce(fs, bucket);
            latencyNanos[i++] = System.nanoTime() - callStart;
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
    private final long renameRpcs;
    private final long elapsedNanos;
    private final long[] sortedLatencyNanos;

    Result(int threads, long volumeInfoRpcs, long bucketInfoRpcs, long renameRpcs, long elapsedNanos,
        long[] latencyNanos) {
      this.threads = threads;
      this.volumeInfoRpcs = volumeInfoRpcs;
      this.bucketInfoRpcs = bucketInfoRpcs;
      this.renameRpcs = renameRpcs;
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
