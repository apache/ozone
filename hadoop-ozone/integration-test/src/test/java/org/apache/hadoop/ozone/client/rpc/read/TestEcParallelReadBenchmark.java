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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark for reading the cells of a healthy EC block in parallel (HDDS-16867).
 *
 * <p>Every EC key is read through two sets of clients of the same cluster: with
 * {@code ozone.client.ec.parallel.read.enabled=false} (cells are read one at a time) and with it set to true.
 * Each client has its own datanode connections. Rounds alternate the order of the two arms, after a warm-up, and
 * the median throughput of the rounds is reported; latency percentiles are taken over all measured rounds.
 *
 * <p>The benchmark runs on a 5 and on a 9 datanode cluster. rs-6-3 needs 9 datanodes, so on 5 datanodes only
 * rs-3-2 is measured; there every datanode holds a cell of every key, while on 9 datanodes the cells of rs-3-2
 * keys are spread over more datanodes.
 *
 * <p>All datanodes run in this JVM and are reached over loopback, so a datanode round trip costs far less than
 * on a real network. The speed-up measured here comes from overlapping the datanode read, the transfer and the
 * client-side checksum verification of different cells; on a real network the serial round trips that the
 * parallel path removes cost more, so this is a lower bound. With several readers, the readers and all the
 * datanodes share the CPUs of one machine.
 *
 * <p>Run with:
 * <pre>
 *   mvn -pl :ozone-integration-test test -Dtest=TestEcParallelReadBenchmark \
 *     -Dexcluded-test-groups= -DskipShade -DskipRecon
 * </pre>
 */
@Tag("benchmark")
public class TestEcParallelReadBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(TestEcParallelReadBenchmark.class);

  private static final int MB = 1024 * 1024;
  private static final int EC_CHUNK_SIZE = MB;
  /** Size of each key: 16 stripes of rs-3-2, 8 of rs-6-3. */
  private static final int KEY_SIZE = 48 * MB;
  private static final int WARMUP_ROUNDS = 2;
  private static final int ROUNDS = 7;
  /** Concurrent readers, and the number of clients and keys per policy in the client-per-thread benchmark. */
  private static final int READERS = 4;
  private static final ECReplicationConfig RS_3_2 = new ECReplicationConfig(3, 2, RS, EC_CHUNK_SIZE);
  private static final ECReplicationConfig RS_6_3 = new ECReplicationConfig(6, 3, RS, EC_CHUNK_SIZE);

  @Nested
  class FiveDatanodes extends ClusterBenchmark {
    FiveDatanodes() {
      super(5, Collections.singletonList(RS_3_2));
    }
  }

  @Nested
  class NineDatanodes extends ClusterBenchmark {
    NineDatanodes() {
      super(9, Arrays.asList(RS_3_2, RS_6_3));
    }
  }

  /**
   * The benchmarks on one cluster.
   */
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  abstract class ClusterBenchmark {

    private final int datanodes;
    private final List<ECReplicationConfig> policies;
    private final String volumeName = "benchvol";
    private final String bucketName = "benchbucket";
    private MiniOzoneCluster cluster;
    // READERS clients per arm; the shared-client benchmark uses the first of each
    private final List<OzoneClient> serialClients = new ArrayList<>();
    private final List<OzoneClient> parallelClients = new ArrayList<>();
    // CRC32 of each key, by policy then reader
    private final List<long[]> keyCrcs = new ArrayList<>();

    ClusterBenchmark(int datanodes, List<ECReplicationConfig> policies) {
      this.datanodes = datanodes;
      this.policies = policies;
    }

    @BeforeAll
    void init() throws Exception {
      cluster = MiniOzoneCluster.newBuilder(new OzoneConfiguration())
          .setNumDatanodes(datanodes)
          .build();
      cluster.waitForClusterToBeReady();
      for (int i = 0; i < READERS; i++) {
        serialClients.add(newClient(false));
        parallelClients.add(newClient(true));
      }

      ObjectStore objectStore = serialClients.get(0).getObjectStore();
      objectStore.createVolume(volumeName);
      objectStore.getVolume(volumeName).createBucket(bucketName);
      OzoneBucket bucket = bucket(serialClients.get(0));
      Random random = new Random(20261012L);
      byte[] data = new byte[KEY_SIZE];
      for (ECReplicationConfig policy : policies) {
        long[] crcs = new long[READERS];
        for (int i = 0; i < READERS; i++) {
          random.nextBytes(data);
          try (OutputStream out = bucket.createKey(keyName(policy, i), KEY_SIZE, policy, Collections.emptyMap())) {
            out.write(data);
          }
          crcs[i] = crc(data, KEY_SIZE);
        }
        keyCrcs.add(crcs);
      }
    }

    private OzoneClient newClient(boolean parallelRead) throws IOException {
      OzoneConfiguration clientConf = new OzoneConfiguration(cluster.getConf());
      OzoneClientConfig clientConfig = clientConf.getObject(OzoneClientConfig.class);
      clientConfig.setEcParallelReadEnabled(parallelRead);
      clientConf.setFromObject(clientConfig);
      return OzoneClientFactory.getRpcClient(clientConf);
    }

    @AfterAll
    void shutdown() {
      IOUtils.closeQuietly(serialClients);
      IOUtils.closeQuietly(parallelClients);
      if (cluster != null) {
        cluster.shutdown();
      }
    }

    /**
     * One client per arm, read by 1 and by {@link #READERS} threads that all read the same key.
     */
    @Test
    void benchmarkSharedClient() throws Exception {
      StringBuilder report = new StringBuilder(String.format("%n"
          + "BENCHRESULT EC healthy-read parallel cells (HDDS-16867) - shared client, %d datanodes%n"
          + "  key size=%d MB, cell=%d KB, rounds=%d (after %d warm-up), median MB/s%n"
          + "  %-12s %-8s %-8s %12s %12s %8s%n",
          datanodes, KEY_SIZE / MB, EC_CHUNK_SIZE / 1024, ROUNDS, WARMUP_ROUNDS,
          "policy", "buffer", "readers", "serial", "parallel", "speedup"));
      for (int p = 0; p < policies.size(); p++) {
        ECReplicationConfig policy = policies.get(p);
        verifyKeys(p);
        for (int bufferSize : bufferSizes(policy)) {
          for (int readers : new int[] {1, READERS}) {
            Comparison c = compare(arm -> {
              List<OzoneClient> clients = Collections.nCopies(readers, arm.get(0));
              List<String> keys = Collections.nCopies(readers, keyName(policy, 0));
              return readConcurrently(clients, keys, bufferSize);
            });
            report.append(String.format("  %-12s %-8s %-8d %12.1f %12.1f %7.2fx%n",
                policy.getReplication(), bufferSize / MB + "MB", readers,
                c.serialThroughput(), c.parallelThroughput(), c.speedup()));
          }
        }
      }
      LOG.info(report.toString());
      assertParallelReadersStarted();
    }

    /**
     * {@link #READERS} clients per arm, each read by its own thread and reading its own key.
     */
    @Test
    void benchmarkClientPerThread() throws Exception {
      StringBuilder report = new StringBuilder(String.format("%n"
          + "BENCHRESULT EC healthy-read parallel cells (HDDS-16867) - %d clients x 1 thread, %d datanodes%n"
          + "  key size=%d MB per client, cell=%d KB, rounds=%d (after %d warm-up)%n"
          + "  throughput = median aggregate MB/s; file = time to read one key; call = one read() call%n"
          + "  %-12s %-6s | %8s %8s %7s | %15s %15s | %15s %15s%n",
          READERS, datanodes, KEY_SIZE / MB, EC_CHUNK_SIZE / 1024, ROUNDS, WARMUP_ROUNDS,
          "policy", "buffer", "serial", "parallel", "speedup",
          "file p50 ms s/p", "file p99 ms s/p", "call p50 ms s/p", "call p99 ms s/p"));
      for (int p = 0; p < policies.size(); p++) {
        ECReplicationConfig policy = policies.get(p);
        verifyKeys(p);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < READERS; i++) {
          keys.add(keyName(policy, i));
        }
        for (int bufferSize : bufferSizes(policy)) {
          Comparison c = compare(arm -> readConcurrently(arm, keys, bufferSize));
          report.append(String.format("  %-12s %-6s | %8.1f %8.1f %6.2fx | %7.1f/%-7.1f %7.1f/%-7.1f"
                  + " | %7.2f/%-7.2f %7.2f/%-7.2f%n",
              policy.getReplication(), bufferSize / MB + "MB",
              c.serialThroughput(), c.parallelThroughput(), c.speedup(),
              millis(c.serialFiles.percentile(0.50)), millis(c.parallelFiles.percentile(0.50)),
              millis(c.serialFiles.percentile(0.99)), millis(c.parallelFiles.percentile(0.99)),
              millis(c.serialCalls.percentile(0.50)), millis(c.parallelCalls.percentile(0.50)),
              millis(c.serialCalls.percentile(0.99)), millis(c.parallelCalls.percentile(0.99))));
        }
      }
      if (!policies.contains(RS_6_3)) {
        report.append(String.format("  %s needs 9 datanodes, not measured on %d%n",
            RS_6_3.getReplication(), datanodes));
      }
      LOG.info(report.toString());
      assertParallelReadersStarted();
    }

    /** Runs the workload on both arms, alternating which goes first, and keeps the measured rounds. */
    private Comparison compare(Workload workload) throws Exception {
      Comparison c = new Comparison();
      for (int round = -WARMUP_ROUNDS; round < ROUNDS; round++) {
        boolean serialFirst = (round & 1) == 0;
        Run first = workload.run(serialFirst ? serialClients : parallelClients);
        Run second = workload.run(serialFirst ? parallelClients : serialClients);
        if (round >= 0) {
          c.add(serialFirst ? first : second, serialFirst ? second : first);
        }
      }
      return c;
    }

    /**
     * Reads keys.get(i) through clients.get(i) on its own thread, all threads starting together, and returns the
     * aggregate throughput with the per-file and per-call latencies.
     */
    private Run readConcurrently(List<OzoneClient> clients, List<String> keys, int bufferSize) throws Exception {
      int readers = clients.size();
      ExecutorService pool = Executors.newFixedThreadPool(readers);
      try {
        CountDownLatch ready = new CountDownLatch(readers);
        CountDownLatch start = new CountDownLatch(1);
        List<Callable<Latencies>> reads = new ArrayList<>();
        for (int i = 0; i < readers; i++) {
          OzoneClient client = clients.get(i);
          String key = keys.get(i);
          reads.add(() -> {
            ready.countDown();
            start.await();
            return drain(client, key, bufferSize);
          });
        }
        List<Future<Latencies>> futures = new ArrayList<>();
        for (Callable<Latencies> read : reads) {
          futures.add(pool.submit(read));
        }
        ready.await();
        long startNanos = System.nanoTime();
        start.countDown();
        Run run = new Run();
        for (Future<Latencies> f : futures) {
          Latencies l = f.get();
          run.files.add(l.file);
          run.calls.addAll(l.calls);
        }
        long elapsed = System.nanoTime() - startNanos;
        run.throughput = (double) readers * KEY_SIZE / MB / (elapsed / (double) TimeUnit.SECONDS.toNanos(1));
        return run;
      } finally {
        pool.shutdownNow();
      }
    }

    private Latencies drain(OzoneClient client, String key, int bufferSize) throws IOException {
      byte[] buf = new byte[bufferSize];
      Latencies latencies = new Latencies();
      long total = 0;
      long fileStart = System.nanoTime();
      try (InputStream in = bucket(client).readKey(key)) {
        while (true) {
          long callStart = System.nanoTime();
          int n = in.read(buf);
          if (n <= 0) {
            break;
          }
          latencies.calls.add(System.nanoTime() - callStart);
          total += n;
        }
      }
      latencies.file = System.nanoTime() - fileStart;
      assertEquals(KEY_SIZE, total);
      return latencies;
    }

    /** Every key of the policy returns the written data through both arms. */
    private void verifyKeys(int policyIndex) throws IOException {
      ECReplicationConfig policy = policies.get(policyIndex);
      byte[] all = new byte[KEY_SIZE];
      for (List<OzoneClient> arm : Arrays.asList(serialClients, parallelClients)) {
        for (int i = 0; i < READERS; i++) {
          try (InputStream in = bucket(arm.get(i)).readKey(keyName(policy, i))) {
            int offset = 0;
            int n;
            while (offset < KEY_SIZE && (n = in.read(all, offset, Math.min(3 * MB, KEY_SIZE - offset))) > 0) {
              offset += n;
            }
            assertEquals(KEY_SIZE, offset);
          }
          assertEquals(keyCrcs.get(policyIndex)[i], crc(all, KEY_SIZE), keyName(policy, i));
        }
      }
    }

    private OzoneBucket bucket(OzoneClient client) throws IOException {
      return client.getObjectStore().getVolume(volumeName).getBucket(bucketName);
    }
  }

  /** One cell (control, never parallel), one stripe, and a large buffer spanning several stripes. */
  private static int[] bufferSizes(ECReplicationConfig policy) {
    return new int[] {EC_CHUNK_SIZE, EC_CHUNK_SIZE * policy.getData(), 8 * MB};
  }

  private static void assertParallelReadersStarted() {
    assertThat(Thread.getAllStackTraces().keySet())
        .anyMatch(t -> t.getName().startsWith("ec-parallel-reader-"));
  }

  private static String keyName(ECReplicationConfig policy, int index) {
    return "key-" + policy.getReplication() + "-" + index;
  }

  private static long crc(byte[] data, int length) {
    CRC32 crc = new CRC32();
    crc.update(data, 0, length);
    return crc.getValue();
  }

  private static double millis(double nanos) {
    return nanos / TimeUnit.MILLISECONDS.toNanos(1);
  }

  private static double median(List<Double> values) {
    List<Double> sorted = new ArrayList<>(values);
    Collections.sort(sorted);
    return sorted.get(sorted.size() / 2);
  }

  /** A workload run on one arm, given that arm's clients. */
  @FunctionalInterface
  private interface Workload {
    Run run(List<OzoneClient> clients) throws Exception;
  }

  /** The latencies of one thread reading one key. */
  private static final class Latencies {
    private long file;
    private final List<Long> calls = new ArrayList<>();
  }

  /** One measured run of a workload on one arm. */
  private static final class Run {
    private double throughput;
    private final List<Long> files = new ArrayList<>();
    private final List<Long> calls = new ArrayList<>();
  }

  /** Nanosecond samples with a percentile. */
  private static final class Samples {
    private final List<Long> values = new ArrayList<>();

    double percentile(double p) {
      List<Long> sorted = new ArrayList<>(values);
      Collections.sort(sorted);
      int index = (int) Math.ceil(p * sorted.size()) - 1;
      return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }
  }

  /** The measured rounds of both arms. */
  private static final class Comparison {
    private final List<Double> serialThroughputs = new ArrayList<>();
    private final List<Double> parallelThroughputs = new ArrayList<>();
    private final Samples serialFiles = new Samples();
    private final Samples parallelFiles = new Samples();
    private final Samples serialCalls = new Samples();
    private final Samples parallelCalls = new Samples();

    void add(Run serial, Run parallel) {
      serialThroughputs.add(serial.throughput);
      parallelThroughputs.add(parallel.throughput);
      serialFiles.values.addAll(serial.files);
      parallelFiles.values.addAll(parallel.files);
      serialCalls.values.addAll(serial.calls);
      parallelCalls.values.addAll(parallel.calls);
    }

    double serialThroughput() {
      return median(serialThroughputs);
    }

    double parallelThroughput() {
      return median(parallelThroughputs);
    }

    double speedup() {
      return parallelThroughput() / serialThroughput();
    }
  }
}
