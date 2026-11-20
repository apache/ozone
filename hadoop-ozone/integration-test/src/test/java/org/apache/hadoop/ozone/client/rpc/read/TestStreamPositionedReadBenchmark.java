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

package org.apache.hadoop.ozone.client.rpc.read;

import static org.apache.hadoop.hdds.client.ECReplicationConfig.EcCodec.RS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmarks positioned streaming reads (ozone.client.stream.readblock.enable) with and without reusing the stream of
 * a positioned read for the next one (ozone.client.stream.read.pread.reuse.enabled, HDDS-17752), on an EC and a
 * RATIS/THREE key. Both settings run in the same JVM against the same cluster; within each round they alternate which
 * runs first.
 */
@Tag("benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestStreamPositionedReadBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(TestStreamPositionedReadBenchmark.class);

  private static final int MB = 1024 * 1024;
  private static final int KB = 1024;
  private static final ECReplicationConfig RS_3_2 = new ECReplicationConfig(3, 2, RS, MB);
  private static final ReplicationConfig RATIS_THREE = RatisReplicationConfig.getInstance(ReplicationFactor.THREE);
  private static final int KEY_SIZE = 48 * MB;
  private static final int WARMUP_ROUNDS = 1;
  private static final int ROUNDS = Integer.getInteger("bench.rounds", 5);
  private static final String VOLUME = "benchvol";
  private static final String BUCKET = "benchbucket";

  private MiniOzoneCluster cluster;
  private OzoneClient oneShotClient;
  private OzoneClient reuseClient;
  private byte[] data;

  @BeforeAll
  void init() throws Exception {
    cluster = MiniOzoneCluster.newBuilder(new OzoneConfiguration()).setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();
    oneShotClient = newClient(false);
    reuseClient = newClient(true);
    ObjectStore store = oneShotClient.getObjectStore();
    store.createVolume(VOLUME);
    store.getVolume(VOLUME).createBucket(BUCKET);
    data = new byte[KEY_SIZE];
    new Random(20261108L).nextBytes(data);
    for (ReplicationConfig replication : new ReplicationConfig[] {RS_3_2, RATIS_THREE}) {
      try (OutputStream out = bucket(oneShotClient).createKey(keyName(replication), KEY_SIZE, replication,
          Collections.emptyMap())) {
        out.write(data);
      }
    }
  }

  @AfterAll
  void shutdown() {
    IOUtils.closeQuietly(oneShotClient, reuseClient);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private OzoneClient newClient(boolean reuse) throws IOException {
    OzoneConfiguration clientConf = new OzoneConfiguration(cluster.getConf());
    OzoneClientConfig clientConfig = clientConf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamReadBlock(true);
    clientConfig.setStreamReadPreadReuseEnabled(reuse);
    clientConf.setFromObject(clientConfig);
    return OzoneClientFactory.getRpcClient(clientConf);
  }

  private static OzoneBucket bucket(OzoneClient client) throws Exception {
    return client.getObjectStore().getVolume(VOLUME).getBucket(BUCKET);
  }

  private static String keyName(ReplicationConfig replication) {
    return "key-" + replication.getReplication();
  }

  @Test
  void benchmarkPositionedStreamReads() throws Exception {
    StringBuilder report = new StringBuilder(String.format("%n"
        + "BENCHRESULT positioned streaming reads, one-shot stream vs reused stream (HDDS-17752), 5 datanodes,"
        + " key size=%d MB, rounds=%d (after %d warm-up), arms alternate within a round%n"
        + "  ops/s = median of rounds; latency over all rounds; CPU = process CPU (client and datanodes) per read%n"
        + "  %-12s %-6s %-7s | %10s %10s %10s %9s | %10s %10s %10s %9s | %8s%n",
        KEY_SIZE / MB, ROUNDS, WARMUP_ROUNDS, "key", "size", "threads",
        "ops/s", "p50 ms", "p99 ms", "CPU us", "ops/s", "p50 ms", "p99 ms", "CPU us", "speedup"));
    report.append(String.format("  %-27s | %-43s | %-43s |%n", "", "one-shot stream (reuse off)",
        "reused stream (reuse on)"));
    for (ReplicationConfig replication : new ReplicationConfig[] {RS_3_2, RATIS_THREE}) {
      for (int size : new int[] {4 * KB, 64 * KB, MB}) {
        for (int threads : new int[] {1, 8, 32}) {
          report.append(benchmarkCell(replication, size, threads));
        }
      }
    }
    LOG.info(report.toString());
  }

  private String benchmarkCell(ReplicationConfig replication, int size, int threads) throws Exception {
    int preadsPerThread = size >= MB ? 50 : 200;
    Arm oneShot = new Arm(oneShotClient);
    Arm reuse = new Arm(reuseClient);
    for (int round = -WARMUP_ROUNDS; round < ROUNDS; round++) {
      Arm first = (round & 1) == 0 ? oneShot : reuse;
      Arm second = first == oneShot ? reuse : oneShot;
      for (Arm arm : new Arm[] {first, second}) {
        arm.run(keyName(replication), size, threads, preadsPerThread, round);
      }
    }
    String sizeName = size >= MB ? size / MB + "MB" : size / KB + "KB";
    return String.format("  %-12s %-6s %-7d | %s | %s | %7.2fx%n", replication.getReplication(), sizeName, threads,
        oneShot.summary(), reuse.summary(), reuse.medianRate() / oneShot.medianRate());
  }

  /** The results of one setting for one cell. */
  private final class Arm {
    private final OzoneClient client;
    private final List<Double> rates = new ArrayList<>();
    private final List<Long> latencies = new ArrayList<>();
    private long cpuNanos;
    private long ops;

    Arm(OzoneClient client) {
      this.client = client;
    }

    void run(String key, int size, int threads, int preadsPerThread, int round) throws Exception {
      List<Long> roundLatencies = new ArrayList<>();
      long cpuBefore = processCpuNanos();
      double rate = TestStreamPositionedReadBenchmark.this.run(client, key, size, threads, preadsPerThread, round,
          roundLatencies);
      long cpu = processCpuNanos() - cpuBefore;
      if (round >= 0) {
        rates.add(rate);
        latencies.addAll(roundLatencies);
        cpuNanos += cpu;
        ops += (long) threads * preadsPerThread;
      }
    }

    double medianRate() {
      return median(rates);
    }

    String summary() {
      return String.format("%10.0f %10.3f %10.3f %9.1f", medianRate(), millis(percentile(latencies, 0.50)),
          millis(percentile(latencies, 0.99)), cpuNanos / 1000.0 / ops);
    }
  }

  /**
   * Opens the key and runs preadsPerThread positioned reads of the given size at random offsets on each thread, all
   * on the same stream, and returns the aggregate positioned reads per second.
   */
  private double run(OzoneClient client, String key, int size, int threads, int preadsPerThread, int round,
      List<Long> latencies) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try (OzoneInputStream stream = bucket(client).readKey(key)) {
      // a small positioned read first, so opening the key is not measured
      stream.readFully(0, ByteBuffer.allocate(1));
      CountDownLatch ready = new CountDownLatch(threads);
      CountDownLatch start = new CountDownLatch(1);
      List<Future<List<Long>>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        long seed = 31L * round + t;
        futures.add(pool.submit((Callable<List<Long>>) () -> {
          SplittableRandom random = new SplittableRandom(seed);
          ByteBuffer buffer = ByteBuffer.allocate(size);
          List<Long> own = new ArrayList<>(preadsPerThread);
          ready.countDown();
          start.await();
          for (int i = 0; i < preadsPerThread; i++) {
            int offset = random.nextInt(KEY_SIZE - size);
            buffer.clear();
            long begin = System.nanoTime();
            stream.readFully(offset, buffer);
            own.add(System.nanoTime() - begin);
            if (i == 0) {
              assertArrayEquals(Arrays.copyOfRange(data, offset, offset + size), buffer.array());
            }
          }
          return own;
        }));
      }
      ready.await();
      long begin = System.nanoTime();
      start.countDown();
      for (Future<List<Long>> future : futures) {
        latencies.addAll(future.get(10, TimeUnit.MINUTES));
      }
      long elapsed = System.nanoTime() - begin;
      return threads * preadsPerThread * 1e9 / elapsed;
    } finally {
      pool.shutdownNow();
    }
  }

  private static long processCpuNanos() {
    return ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
        .getProcessCpuTime();
  }

  private static double median(List<Double> values) {
    List<Double> sorted = new ArrayList<>(values);
    Collections.sort(sorted);
    int n = sorted.size();
    return n % 2 == 1 ? sorted.get(n / 2) : (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2;
  }

  private static long percentile(List<Long> values, double p) {
    List<Long> sorted = new ArrayList<>(values);
    Collections.sort(sorted);
    return sorted.get(Math.min(sorted.size() - 1, (int) Math.ceil(p * sorted.size()) - 1));
  }

  private static double millis(long nanos) {
    return nanos / 1e6;
  }
}
