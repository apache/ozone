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

package org.apache.hadoop.hdds.scm;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test reproducing HDDS-14793: intermittent NPE in
 * {@link XceiverClientGrpc#sendCommandAsync} caused by a race condition
 * between the two non-atomic map updates in
 * {@link XceiverClientGrpc#connectToDatanode}.
 *
 * <p>Root cause: {@code channels} and {@code asyncStubs} are two separate
 * {@link java.util.concurrent.ConcurrentHashMap}s updated in sequence. There
 * is a window between
 * <pre>
 *   channels.computeIfAbsent(dnId, createChannel)  // DN becomes "visible"
 *   asyncStubs.computeIfAbsent(dnId, newStub)       // stub added afterwards
 * </pre>
 * during which a second thread can observe a live channel in {@code channels}
 * (and therefore skip reconnect in {@code checkOpen}), then call
 * {@code asyncStubs.get(dnId)} and receive {@code null}, causing an NPE.
 *
 * <p>How the race is triggered naturally here:
 * <ol>
 *   <li>An EC-3-2 pipeline has 5 DataNodes. {@link XceiverClientGrpc#connect}
 *       pre-connects only {@code pipeline.getFirstNode()} (DN-0).</li>
 *   <li>{@link NUM_READ_THREADS} concurrent threads all begin reading files
 *       at the same instant (enforced by a {@link CyclicBarrier}), sharing
 *       the same {@link OzoneClient} and therefore the same cached
 *       {@link ECXceiverClientGrpc} instance.</li>
 *   <li>Every file spans at least two EC data chunks, so each read must
 *       contact DN-1 (second data node) in addition to the pre-connected
 *       DN-0. All threads arrive at their first attempt to connect to DN-1
 *       at roughly the same time.</li>
 *   <li>Thread-A wins {@code channels.computeIfAbsent(DN-1, ...)} and
 *       inserts the channel. Before Thread-A can run
 *       {@code asyncStubs.computeIfAbsent(DN-1, ...)}, Thread-B calls
 *       {@code checkOpen(DN-1)}, observes the channel already present,
 *       skips reconnect, and then executes
 *       {@code asyncStubs.get(DN-1)} — which returns {@code null} — causing
 *       the NPE.</li>
 * </ol>
 *
 * <p>This mirrors the robot test failure seen in production:
 * {@code hadoop-ozone/dist/src/main/smoketest/freon/metadata-generate.robot}
 * "[Read] File in FILE_SYSTEM_OPTIMIZED Bucket".
 *
 * <p>Note: because the race window between the two map operations is very
 * small (nanoseconds of CPU time), a single run may not always trigger the
 * NPE. However, when the bug is present, this test will fail on a significant
 * fraction of runs. With the fix applied (the two maps are collapsed into a
 * single {@code ConcurrentHashMap} of a compound value holding both the
 * channel and the stub), this test should pass deterministically.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestXceiverClientGrpcConcurrentConnect {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestXceiverClientGrpcConcurrentConnect.class);

  /**
   * EC-3-2 needs at least 5 DataNodes. Having all 5 active means DN-1 through
   * DN-4 are NOT pre-connected after {@link XceiverClientGrpc#connect()},
   * giving the race more opportunities to manifest.
   */
  private static final int NUM_DATANODES = 5;

  /**
   * 1 MB EC chunk size. Each data file spans {@code 2 * CHUNK_SIZE + 1} bytes
   * so that it occupies a full chunk on DN-0 and a non-empty chunk on DN-1.
   * This guarantees that every read must contact the non-pre-connected DN-1.
   */
  private static final int EC_CHUNK_SIZE = 1024 * 1024;

  /**
   * File size is chosen so that the EC stripe uses both DN-0 (full chunk) and
   * DN-1 (partial chunk).  DN-0 is pre-connected; DN-1 is not.
   * All {@link #NUM_READ_THREADS} threads racing to connect to DN-1 for the
   * first time is what triggers the NPE.
   */
  private static final int FILE_SIZE = EC_CHUNK_SIZE + 1;

  /**
   * Number of pre-written files. Each thread reads a distinct file, which
   * means all threads need DN-1 simultaneously — maximising the race window.
   */
  private static final int NUM_FILES = 20;

  /**
   * Number of concurrent reading threads. A value significantly larger than
   * the number of unconnected DNs (4) increases the probability that at
   * least two threads are scheduled within the nanosecond race window between
   * {@code channels.computeIfAbsent} and {@code asyncStubs.computeIfAbsent}.
   */
  private static final int NUM_READ_THREADS = 32;

  private static final ECReplicationConfig EC_CONFIG =
      new ECReplicationConfig(3, 2,
          ECReplicationConfig.EcCodec.RS, EC_CHUNK_SIZE);

  private static final String VOLUME_NAME = "vol-concurrent-read-test";
  private static final String BUCKET_NAME = "bucket-ec-fso-concurrent";

  private MiniOzoneCluster cluster;

  private static void readFully(OzoneBucket bucket, String fileName)
      throws Exception {
    try (OzoneInputStream in = bucket.readFile(fileName)) {
      byte[] buf = new byte[8192];
      //noinspection StatementWithEmptyBody
      while (in.read(buf) != -1) {
        // drain the stream
      }
    }
  }

  /**
   * Blocks at the barrier, converting checked exceptions to unchecked.
   */
  private static void awaitBarrier(CyclicBarrier barrier) {
    try {
      barrier.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (BrokenBarrierException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Runs {@code task}, catching any {@link Throwable} and returning it.
   * Returns {@code null} on success.
   */
  private static Throwable captureThrowable(ThrowingRunnable task) {
    try {
      task.run();
      return null;
    } catch (Throwable t) {
      LOG.error("Thread failed during concurrent read", t);
      return t;
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static List<String> collectFailures(
      List<Future<Throwable>> futures) throws Exception {
    List<String> failures = new ArrayList<>();
    for (Future<Throwable> future : futures) {
      Throwable t = future.get();
      if (t != null) {
        failures.add(t.toString());
      }
    }
    return failures;
  }

  private static String fileName(int index) {
    return String.format("file-%04d", index);
  }

  @BeforeAll
  void startClusterAndWriteFiles() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    // Allow the EC-3-2-1024k replication config used in this test.
    conf.set("ozone.replication.allowed-configs",
        "(^((STANDALONE|RATIS)/(ONE|THREE))|(EC/(3-2|6-3|10-4)-(512|1024|2048|4096|1)k)$)");
    conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);

    // Fast heartbeat so the cluster reaches ready state quickly.
    conf.setTimeDuration(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL,
        500, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL,
        1, TimeUnit.SECONDS);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(NUM_DATANODES)
        .build();
    cluster.waitForClusterToBeReady();
    // Wait for a Ratis THREE pipeline to be ready (needed for the DN pool).
    cluster.waitForPipelineTobeReady(
        HddsProtos.ReplicationFactor.THREE, 120_000);

    // Write files using a dedicated client that is closed before reads.
    // This ensures the reading client starts with zero pre-existing connections.
    try (OzoneClient writeClient = cluster.newClient()) {
      OzoneVolume volume = prepareVolume(writeClient);
      OzoneBucket bucket = prepareBucket(volume);
      writeTestFiles(bucket);
    }
  }

  @AfterAll
  void shutdownCluster() {
    IOUtils.closeQuietly(cluster);
  }

  /**
   * Verifies that concurrent reads from an EC-3-2 FILE_SYSTEM_OPTIMIZED bucket
   * do not produce a NullPointerException due to the race condition in
   * {@link XceiverClientGrpc#connectToDatanode}.
   *
   * <p>All {@link #NUM_READ_THREADS} threads share a single {@link OzoneClient}
   * (and therefore the same cached {@link ECXceiverClientGrpc} instance).
   * A {@link CyclicBarrier} forces all threads to start their first read at
   * exactly the same instant, maximising the probability that multiple threads
   * simultaneously attempt the first connection to DN-1.
   */
  @Test
  public void testConcurrentFirstConnectionToDatanodeDoesNotCauseNpe()
      throws Exception {

    // A fresh client has a fresh XceiverClientManager.
    // ECXceiverClientGrpc.connect() will pre-connect only to DN-0.
    // All reads also need DN-1 (FILE_SIZE > EC_CHUNK_SIZE), so all threads
    // race on the first connection to DN-1.
    try (OzoneClient readClient = cluster.newClient()) {
      OzoneBucket bucket = readClient.getObjectStore()
          .getVolume(VOLUME_NAME)
          .getBucket(BUCKET_NAME);

      CyclicBarrier barrier = new CyclicBarrier(NUM_READ_THREADS);
      ExecutorService executor = Executors.newFixedThreadPool(NUM_READ_THREADS);
      List<Future<Throwable>> futures = new ArrayList<>(NUM_READ_THREADS);

      for (int t = 0; t < NUM_READ_THREADS; t++) {
        final String fileName = fileName(t % NUM_FILES);
        futures.add(executor.submit(() -> captureThrowable(() -> {
          // All threads cross the barrier together so their first reads —
          // and therefore their first connection attempts to DN-1 — are
          // concurrent, hitting the race window between
          // channels.computeIfAbsent and asyncStubs.computeIfAbsent.
          awaitBarrier(barrier);
          readFully(bucket, fileName);
        })));
      }

      executor.shutdown();
      boolean terminated = executor.awaitTermination(120, TimeUnit.SECONDS);
      assertThat(terminated).as("Executor timed out").isTrue();

      List<String> failures = collectFailures(futures);
      assertThat(failures)
          .as("Concurrent reads produced failures. Without the fix, "
              + "asyncStubs.get(dnId) returns null when channels already has "
              + "an entry for dnId but asyncStubs has not yet been updated "
              + "(race between channels.computeIfAbsent and "
              + "asyncStubs.computeIfAbsent in XceiverClientGrpc). "
              + "Failures: " + failures)
          .isEmpty();
    }
  }

  private OzoneVolume prepareVolume(OzoneClient client) throws Exception {
    client.getObjectStore().createVolume(VOLUME_NAME);
    return client.getObjectStore().getVolume(VOLUME_NAME);
  }

  private OzoneBucket prepareBucket(OzoneVolume volume) throws Exception {
    volume.createBucket(BUCKET_NAME, BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .setDefaultReplicationConfig(new DefaultReplicationConfig(EC_CONFIG))
        .build());
    return volume.getBucket(BUCKET_NAME);
  }

  private void writeTestFiles(OzoneBucket bucket) throws Exception {
    byte[] content = new byte[FILE_SIZE];
    new Random().nextBytes(content);
    for (int i = 0; i < NUM_FILES; i++) {
      try (OzoneOutputStream out =
               bucket.createFile(fileName(i), FILE_SIZE, EC_CONFIG,
                   true, false)) {
        out.write(content);
      }
    }
    LOG.info("Wrote {} test files of {} bytes each to EC-3-2 bucket {}",
        NUM_FILES, FILE_SIZE, BUCKET_NAME);
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
