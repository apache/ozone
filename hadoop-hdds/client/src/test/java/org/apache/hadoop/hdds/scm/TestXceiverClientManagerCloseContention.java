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
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
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
 * <p>The damage was amplified by <em>where</em> {@code close()} ran. Clients
 * are torn down through {@link XceiverClientManager}'s cache eviction, and the
 * whole acquire / evict / close sequence executed under the {@code clientCache}
 * monitor:
 * <pre>
 *   acquireClient()                    // synchronized (clientCache)
 *     -&gt; getClient()
 *       -&gt; clientCache.get(key, newClient)
 *          -&gt; (Guava evicts an entry on the calling thread)
 *             -&gt; RemovalListener.onRemoval()   // synchronized (clientCache)
 *                -&gt; XceiverClientSpi.setEvicted() -&gt; cleanup() -&gt; close()
 * </pre>
 * So the blocking graceful-shutdown wait ran while the {@code clientCache}
 * monitor was held, and every other thread in {@code acquireClient()} /
 * {@code releaseClient()} serialized behind it.
 *
 * <p>The manager now closes evicted clients one at a time on a background
 * thread, so a slow {@code close()} delays the closes queued behind it.
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
 * <p>Each eviction-driven {@code close()} is timed on its own. Against the
 * pre-fix {@code close()} every one of them takes at least 100ms; with the fix
 * (immediate {@code shutdownNow()}) they take a few milliseconds. Timing the
 * closes themselves keeps client creation and the queueing of closes on the
 * manager's single close thread out of the measurement, so a slow machine does
 * not push the result towards the regression (HDDS-17634).
 */
class TestXceiverClientManagerCloseContention {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestXceiverClientManagerCloseContention.class);

  private static final int THREADS = 8;
  private static final int CYCLES_PER_THREAD = 8;

  /**
   * Upper bound on the median duration of one eviction-driven close(). The
   * pre-fix code has a hard &ge;100ms floor on every close (the mandatory
   * Thread.sleep in the termination polling loop), so its median is above it;
   * immediate shutdownNow() takes a few milliseconds.
   */
  private static final long MAX_MEDIAN_MILLIS_PER_CLOSE = 50;

  @Test
  void evictionDrivenCloseDoesNotSerializeAcquisition() throws Exception {
    ConfigurationSource conf = new OzoneConfiguration();
    // maxCacheSize == 1 => each acquisition of a new pipeline evicts (and thus
    // closes) the previously cached client.
    ScmClientConfig clientConf = new XceiverClientManagerConfigBuilder()
        .setMaxCacheSize(1)
        .setStaleThresholdMs(TimeUnit.SECONDS.toMillis(30))
        .build();
    // duration of each close() that actually closed a client
    List<Long> closeNanos = Collections.synchronizedList(new ArrayList<>());

    try (XceiverClientManager manager = newManagerTimingCloses(conf, clientConf, closeNanos)) {

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
      // closing the manager waits for the closes still queued, so that all of them are timed
      manager.close();
      long wallMillis =
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - wallStartNanos);

      // recordStats() is enabled on the cache; each eviction drives one close().
      long evictions = manager.getClientCache().stats().evictionCount();
      List<Long> sortedMillis;
      synchronized (closeNanos) {
        sortedMillis = closeNanos.stream().map(TimeUnit.NANOSECONDS::toMillis).sorted().collect(Collectors.toList());
      }
      long medianMillisPerClose = sortedMillis.isEmpty() ? 0 : sortedMillis.get(sortedMillis.size() / 2);

      LOG.info("threads={} cyclesPerThread={} acquisitions={} evictions={} closes={}",
          THREADS, CYCLES_PER_THREAD, THREADS * CYCLES_PER_THREAD, evictions, sortedMillis.size());
      LOG.info("wall={}ms  close median={}ms max={}ms  (regression floor is ~100ms/close)",
          wallMillis, medianMillisPerClose, sortedMillis.isEmpty() ? 0 : sortedMillis.get(sortedMillis.size() - 1));

      assertThat(evictions)
          .as("cache eviction should have driven close() calls")
          .isPositive();
      assertThat((long) sortedMillis.size())
          .as("every evicted client should have been closed")
          .isGreaterThanOrEqualTo(evictions);

      assertThat(medianMillisPerClose)
          .as("HDDS-15849: eviction-driven close() must terminate the channel "
              + "immediately; the pre-fix graceful-shutdown wait forces a ~100ms "
              + "floor per close")
          .isLessThan(MAX_MEDIAN_MILLIS_PER_CLOSE);
    }
  }

  /**
   * A manager that creates the same real {@link XceiverClientGrpc} it would otherwise, but adds the duration of each
   * close() that actually closes a client to {@code closeNanos}.
   */
  private static XceiverClientManager newManagerTimingCloses(ConfigurationSource conf, ScmClientConfig clientConf,
      List<Long> closeNanos) throws IOException {
    return new XceiverClientManager(conf, clientConf, (ClientTrustManager) null) {
      @Override
      protected XceiverClientSpi newClient(Pipeline pipeline, DatanodeDetails dn) throws IOException {
        XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf) {
          @Override
          public void close() {
            final boolean open = !isClosed();
            final long startNanos = System.nanoTime();
            super.close();
            if (open) {
              closeNanos.add(System.nanoTime() - startNanos);
            }
          }
        };
        try {
          client.connect();
        } catch (Exception e) {
          throw new IOException(e);
        }
        return client;
      }
    };
  }

  /**
   * A client whose close() is slow, like XceiverClientRatis waiting for channel termination, must be closed
   * off the caller thread and outside the clientCache lock.
   */
  @Test
  void slowCloseOfEvictedClientDoesNotBlockCaller() throws Exception {
    ScmClientConfig clientConf = new XceiverClientManagerConfigBuilder()
        .setMaxCacheSize(1)
        .setStaleThresholdMs(TimeUnit.SECONDS.toMillis(30))
        .build();
    CountDownLatch closeStarted = new CountDownLatch(1);
    CountDownLatch finishClose = new CountDownLatch(1);
    AtomicReference<Thread> closeThread = new AtomicReference<>();
    AtomicBoolean closedUnderCacheLock = new AtomicBoolean();
    AtomicBoolean closed = new AtomicBoolean();

    try (XceiverClientManager manager = new XceiverClientManager(new OzoneConfiguration(), clientConf, null) {
      @Override
      protected XceiverClientSpi newClient(Pipeline pipeline, DatanodeDetails dn) {
        XceiverClientSpi client = mock(XceiverClientSpi.class,
            withSettings().useConstructor().defaultAnswer(CALLS_REAL_METHODS));
        doAnswer(invocation -> {
          closeThread.set(Thread.currentThread());
          closedUnderCacheLock.set(Thread.holdsLock(getClientCache()));
          closeStarted.countDown();
          finishClose.await(10, TimeUnit.SECONDS);
          closed.set(true);
          return null;
        }).when(client).close();
        return client;
      }
    }) {
      XceiverClientSpi client1 = manager.acquireClient(MockPipeline.createPipeline(1));
      manager.releaseClient(client1, false);
      // evicts client1, which has no reference left and so gets closed
      XceiverClientSpi client2 = manager.acquireClient(MockPipeline.createPipeline(1));

      assertThat(closeStarted.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(closeThread.get()).isNotSameAs(Thread.currentThread());
      assertThat(closedUnderCacheLock).isFalse();
      // the close of client1 is still in progress, yet the cache is usable
      assertThat(closed).isFalse();
      manager.releaseClient(client2, false);
      finishClose.countDown();
    }
    // closing the manager waits for pending closes
    assertThat(closed).isTrue();
  }

  @Test
  void managerClosesCachedClientsInParallel() throws Exception {
    CountDownLatch closeStarted = new CountDownLatch(3);
    AtomicBoolean allClosesOverlapped = new AtomicBoolean(true);
    AtomicBoolean closedUnderCacheLock = new AtomicBoolean();
    ScmClientConfig clientConf = new XceiverClientManagerConfigBuilder()
        .setMaxCacheSize(3)
        .setStaleThresholdMs(TimeUnit.SECONDS.toMillis(30))
        .build();
    try (XceiverClientManager manager = new XceiverClientManager(new OzoneConfiguration(), clientConf, null) {
      @Override
      protected XceiverClientSpi newClient(Pipeline pipeline, DatanodeDetails dn) {
        XceiverClientSpi client = mock(XceiverClientSpi.class,
            withSettings().useConstructor().defaultAnswer(CALLS_REAL_METHODS));
        doAnswer(invocation -> {
          if (Thread.holdsLock(getClientCache())) {
            closedUnderCacheLock.set(true);
          }
          closeStarted.countDown();
          if (!closeStarted.await(10, TimeUnit.SECONDS)) {
            allClosesOverlapped.set(false);
          }
          return null;
        }).when(client).close();
        return client;
      }
    }) {
      for (int i = 0; i < 3; i++) {
        XceiverClientSpi client = manager.acquireClient(MockPipeline.createPipeline(1));
        manager.releaseClient(client, false);
      }
    }
    assertThat(closeStarted.getCount()).isZero();
    assertThat(allClosesOverlapped).isTrue();
    assertThat(closedUnderCacheLock).isFalse();
  }
}
