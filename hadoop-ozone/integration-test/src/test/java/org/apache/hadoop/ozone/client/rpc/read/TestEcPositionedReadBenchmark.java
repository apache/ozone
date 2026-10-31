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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientMetrics;
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
 * Benchmark of positioned reads on one open stream of a healthy EC key, by 1 and by several threads.
 * Reports the latency of a positioned read and the GetBlock and ReadChunk calls it needs.
 */
@Tag("benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestEcPositionedReadBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(TestEcPositionedReadBenchmark.class);

  private static final int MB = 1024 * 1024;
  private static final int KB = 1024;
  private static final ECReplicationConfig RS_3_2 = new ECReplicationConfig(3, 2, RS, MB);
  /** 16 stripes of rs-3-2, one block group. */
  private static final int KEY_SIZE = 48 * MB;
  private static final int WARMUP_ROUNDS = 1;
  private static final int ROUNDS = 5;
  private static final int THREADS = 8;
  private static final String VOLUME = "benchvol";
  private static final String BUCKET = "benchbucket";
  private static final String KEY = "key";

  private MiniOzoneCluster cluster;
  // clients reading the cells of a multi-cell read one at a time, and in parallel
  private OzoneClient serialClient;
  private OzoneClient parallelClient;
  private byte[] data;

  @BeforeAll
  void init() throws Exception {
    cluster = MiniOzoneCluster.newBuilder(new OzoneConfiguration()).setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();
    serialClient = newClient(false);
    parallelClient = newClient(true);
    ObjectStore store = serialClient.getObjectStore();
    store.createVolume(VOLUME);
    store.getVolume(VOLUME).createBucket(BUCKET);
    data = new byte[KEY_SIZE];
    new Random(20261026L).nextBytes(data);
    try (OutputStream out = bucket(serialClient).createKey(KEY, KEY_SIZE, RS_3_2, Collections.emptyMap())) {
      out.write(data);
    }
  }

  @AfterAll
  void shutdown() {
    IOUtils.closeQuietly(serialClient, parallelClient);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private OzoneClient newClient(boolean parallelRead) throws IOException {
    OzoneConfiguration clientConf = new OzoneConfiguration(cluster.getConf());
    OzoneClientConfig clientConfig = clientConf.getObject(OzoneClientConfig.class);
    clientConfig.setEcParallelReadEnabled(parallelRead);
    clientConf.setFromObject(clientConfig);
    return OzoneClientFactory.getRpcClient(clientConf);
  }

  private static OzoneBucket bucket(OzoneClient client) throws Exception {
    return client.getObjectStore().getVolume(VOLUME).getBucket(BUCKET);
  }

  @Test
  void benchmarkPositionedReads() throws Exception {
    StringBuilder report = new StringBuilder();
    for (boolean parallelRead : new boolean[] {false, true}) {
      report.append(benchmark(parallelRead ? parallelClient : serialClient, parallelRead));
    }
    LOG.info(report.toString());
  }

  private String benchmark(OzoneClient client, boolean parallelRead) throws Exception {
    StringBuilder report = new StringBuilder(String.format("%n"
        + "BENCHRESULT EC healthy positioned reads (HDDS-16870) - parallel cell reads %s, one open stream,"
        + " 5 datanodes, %s%n"
        + "  key size=%d MB, rounds=%d (after %d warm-up); ops/s = median of rounds; latency over all rounds%n"
        + "  %-6s %-7s %8s %10s %10s %10s %12s %12s%n",
        parallelRead ? "on" : "off", RS_3_2.getReplication(), KEY_SIZE / MB, ROUNDS, WARMUP_ROUNDS,
        "size", "threads", "preads", "ops/s", "p50 ms", "p99 ms", "GetBlock/op", "ReadChunk/op"));
    for (int size : new int[] {4 * KB, 64 * KB, MB, 4 * MB}) {
      for (int threads : new int[] {1, THREADS}) {
        int preadsPerThread = size >= 4 * MB ? 25 : size >= MB ? 50 : 250;
        List<Double> rates = new ArrayList<>();
        List<Long> latencies = new ArrayList<>();
        long getBlocks = 0;
        long readChunks = 0;
        for (int round = -WARMUP_ROUNDS; round < ROUNDS; round++) {
          XceiverClientMetrics metrics = XceiverClientManager.getXceiverClientMetrics();
          long getBlockBefore = metrics.getContainerOpCountMetrics(ContainerProtos.Type.GetBlock);
          long readChunkBefore = metrics.getContainerOpCountMetrics(ContainerProtos.Type.ReadChunk);
          List<Long> roundLatencies = new ArrayList<>();
          double rate = run(client, size, threads, preadsPerThread, round, roundLatencies);
          if (round >= 0) {
            rates.add(rate);
            latencies.addAll(roundLatencies);
            getBlocks += metrics.getContainerOpCountMetrics(ContainerProtos.Type.GetBlock) - getBlockBefore;
            readChunks += metrics.getContainerOpCountMetrics(ContainerProtos.Type.ReadChunk) - readChunkBefore;
          }
        }
        long ops = (long) ROUNDS * threads * preadsPerThread;
        report.append(String.format("  %-6s %-7d %8d %10.0f %10.3f %10.3f %12.3f %12.3f%n",
            size >= MB ? size / MB + "MB" : size / KB + "KB", threads, threads * preadsPerThread,
            median(rates), millis(percentile(latencies, 0.50)), millis(percentile(latencies, 0.99)),
            (double) getBlocks / ops, (double) readChunks / ops));
      }
    }
    return report.toString();
  }

  /**
   * Opens the key and runs preadsPerThread positioned reads of the given size at random offsets on each thread, all
   * on the same stream, and returns the aggregate positioned reads per second.
   */
  private double run(OzoneClient client, int size, int threads, int preadsPerThread, int round, List<Long> latencies)
      throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try (OzoneInputStream stream = bucket(client).readKey(KEY)) {
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
      return (double) threads * preadsPerThread / (elapsed / (double) TimeUnit.SECONDS.toNanos(1));
    } finally {
      pool.shutdownNow();
    }
  }

  private static double median(List<Double> values) {
    List<Double> sorted = new ArrayList<>(values);
    Collections.sort(sorted);
    return sorted.get(sorted.size() / 2);
  }

  private static long percentile(List<Long> values, double p) {
    List<Long> sorted = new ArrayList<>(values);
    Collections.sort(sorted);
    int index = (int) Math.ceil(p * sorted.size()) - 1;
    return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
  }

  private static double millis(long nanos) {
    return nanos / 1e6;
  }
}
