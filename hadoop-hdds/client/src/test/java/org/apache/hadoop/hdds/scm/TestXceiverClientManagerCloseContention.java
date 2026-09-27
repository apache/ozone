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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.XceiverClientManager.ScmClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientManager.XceiverClientManagerConfigBuilder;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Regression test for HDDS-15849.
 *
 * <p>HDDS-14571 changed {@link XceiverClientGrpc#close()} from a forced
 * {@code shutdownNow()} (which cancels in-flight RPCs and terminates in ~1ms)
 * to a graceful {@code shutdown()} followed by a polling loop that always
 * sleeps at least {@code SHUTDOWN_WAIT_INTERVAL_MILLIS} (100ms) before it can
 * observe channel termination. Every {@code close()} therefore costs &ge;100ms
 * instead of ~1ms.
 *
 * <p>The damage is amplified by <em>where</em> {@code close()} runs. Clients
 * are torn down through {@link XceiverClientManager}'s cache eviction, and the
 * whole acquire / evict / close sequence executes under the {@code clientCache}
 * monitor:
 * <pre>
 *   acquireClient()                    // synchronized (clientCache)
 *     -&gt; getClient()
 *       -&gt; clientCache.get(key, newClient)
 *          -&gt; (Guava evicts an entry on the calling thread)
 *             -&gt; RemovalListener.onRemoval()   // synchronized (clientCache)
 *                -&gt; XceiverClientSpi.setEvicted() -&gt; cleanup() -&gt; close()
 * </pre>
 * So the blocking graceful-shutdown wait runs while the {@code clientCache}
 * monitor is held, and every other thread in {@code acquireClient()} /
 * {@code releaseClient()} serializes behind it.
 *
 * <p>This test exercises the <b>real</b> {@code XceiverClientManager} and
 * {@code XceiverClientGrpc.close()} — nothing is emulated. Real (lazily
 * connected, plaintext) gRPC channels are created against random local
 * datanode addresses; no live datanode is required because
 * {@code close()} only shuts the channels down. {@code maxCacheSize == 1} plus
 * a distinct pipeline per iteration forces the previously cached client to be
 * evicted and closed on essentially every acquisition — mirroring the per-file
 * client churn of EC checksum collection.
 *
 * <p>Run it against the pre-fix {@code close()} and the wall-clock is dominated
 * by serialized 100ms sleeps ({@code ~ evictions * 100ms}); run it against the
 * fix (immediate {@code shutdownNow()}) and per-close cost collapses. The
 * assertion below encodes the fixed expectation, so this test FAILS on the
 * regression and PASSES once HDDS-15849 restores immediate termination.
 */
class TestXceiverClientManagerCloseContention {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestXceiverClientManagerCloseContention.class);

  private static final int THREADS = 8;
  private static final int CYCLES_PER_THREAD = 8;

  /**
   * Upper bound on acceptable average wall-clock cost per eviction-driven
   * close(). The pre-fix code has a hard &ge;100ms floor per close (the
   * mandatory Thread.sleep in the termination polling loop); immediate
   * shutdownNow() is well under a millisecond. 40ms sits safely between the
   * two so the assertion is not timing-flaky.
   */
  private static final long MAX_MILLIS_PER_CLOSE = 40;

  @Test
  void evictionDrivenCloseDoesNotSerializeAcquisition() throws Exception {
    ConfigurationSource conf = new OzoneConfiguration();
    // maxCacheSize == 1 => each acquisition of a new pipeline evicts (and thus
    // closes) the previously cached client.
    ScmClientConfig clientConf = new XceiverClientManagerConfigBuilder()
        .setMaxCacheSize(1)
        .setStaleThresholdMs(TimeUnit.SECONDS.toMillis(30))
        .build();

    try (XceiverClientManager manager =
             new XceiverClientManager(conf, clientConf, (ClientTrustManager) null)) {

      ExecutorService pool = Executors.newFixedThreadPool(THREADS);
      CountDownLatch start = new CountDownLatch(1);
      long wallStartNanos;
      try {
        List<Future<?>> futures = IntStream.range(0, THREADS)
            .mapToObj(t -> pool.submit(() -> {
              start.await();
              for (int i = 0; i < CYCLES_PER_THREAD; i++) {
                // Distinct pipeline per cycle => distinct cache key => the
                // previously cached client is evicted and closed.
                Pipeline pipeline = MockPipeline.createPipeline(1);
                XceiverClientSpi client = manager.acquireClient(pipeline);
                manager.releaseClient(client, false);
              }
              return null;
            }))
            .collect(Collectors.toList());

        wallStartNanos = System.nanoTime();
        start.countDown();
        for (Future<?> f : futures) {
          f.get(120, TimeUnit.SECONDS);
        }
      } finally {
        pool.shutdownNow();
      }
      long wallMillis =
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - wallStartNanos);

      // recordStats() is enabled on the cache; each eviction drives one close().
      long evictions = manager.getClientCache().stats().evictionCount();
      long millisPerClose = evictions == 0 ? 0 : wallMillis / evictions;

      LOG.info("threads={} cyclesPerThread={} acquisitions={} evictions(closes)={}",
          THREADS, CYCLES_PER_THREAD, THREADS * CYCLES_PER_THREAD, evictions);
      LOG.info("wall={}ms  ms-per-close={}ms  (regression floor is ~100ms/close)",
          wallMillis, millisPerClose);

      assertThat(evictions)
          .as("cache eviction should have driven close() calls")
          .isPositive();

      assertThat(millisPerClose)
          .as("HDDS-15849: eviction-driven close() must not serialize "
              + "acquisition under the clientCache monitor; the pre-fix "
              + "graceful-shutdown wait forces a ~100ms floor per close")
          .isLessThan(MAX_MILLIS_PER_CLOSE);
    }
  }
}
