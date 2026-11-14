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

package org.apache.hadoop.hdds.client;

import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Benchmark for the replication config resolution done on every ofs/o3fs {@code create()}:
 * {@code OzoneClientUtils.resolveClientSideReplicationConfig} calls {@link ReplicationConfig#adjustReplication}
 * when the client configures a replication, and {@link ReplicationConfig#parse} otherwise. Both validate the result
 * against {@code ozone.replication.allowed-configs}.
 *
 * <p>Only public API is used, so the same class can be run before and after a change for an A/B comparison.
 * It reports time and allocated bytes per call on one thread, and throughput with several threads.
 *
 * <p>Run with:
 * <pre>
 *   mvn test -pl :hdds-common \
 *     -Dtest=ReplicationConfigParseBenchmark \
 *     -Dgroups=benchmark -Dexcluded-test-groups= \
 *     -Dsurefire.failIfNoSpecifiedTests=false
 * </pre>
 */
@Tag("benchmark")
public class ReplicationConfigParseBenchmark {

  private static final int[] THREAD_COUNTS = {1, 4, 8, 16};
  private static final int ROUNDS = 5;
  private static final int WARMUP_MS = 2_000;
  private static final int MEASURE_MS = 1_000;
  /** Calls per {@code System.nanoTime()} poll to amortise the timer cost. */
  private static final int BATCH = 1_000;

  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static final ReplicationConfig RATIS_THREE =
      ReplicationConfig.parse(ReplicationType.RATIS, "THREE", CONF);

  /** Keeps the JIT from eliminating the measured calls. */
  private static volatile int sink;

  @Test
  public void benchmarkSingleThread() {
    System.out.println();
    System.out.printf("=== Single thread, %d rounds of %d ms ===%n", ROUNDS, MEASURE_MS);
    System.out.printf("%-20s  %12s  %14s%n", "Operation", "ns/call", "bytes/call");
    singleThread("parse", () -> ReplicationConfig.parse(ReplicationType.RATIS, "3", CONF));
    singleThread("adjustReplication", () -> ReplicationConfig.adjustReplication(RATIS_THREE, (short) 1, CONF));
  }

  @Test
  public void benchmarkThroughput() throws Exception {
    System.out.println();
    System.out.printf("=== parse throughput, best of %d rounds of %d ms ===%n", ROUNDS, MEASURE_MS);
    System.out.printf("%-9s  %14s%n", "Threads", "calls/ms");
    for (int threads : THREAD_COUNTS) {
      double best = 0;
      for (int round = 0; round < ROUNDS; round++) {
        best = Math.max(best, throughput(threads,
            () -> ReplicationConfig.parse(ReplicationType.RATIS, "3", CONF)));
      }
      System.out.printf("%-9d  %14.1f%n", threads, best);
    }
  }

  private static void singleThread(String name, Supplier<ReplicationConfig> op) {
    com.sun.management.ThreadMXBean threadBean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    long tid = Thread.currentThread().getId();
    runFor(op, WARMUP_MS);
    double bestNs = Double.MAX_VALUE;
    double bytes = 0;
    for (int round = 0; round < ROUNDS; round++) {
      long bytesBefore = threadBean.getThreadAllocatedBytes(tid);
      long start = System.nanoTime();
      long calls = runFor(op, MEASURE_MS);
      long elapsed = System.nanoTime() - start;
      bytes = (double) (threadBean.getThreadAllocatedBytes(tid) - bytesBefore) / calls;
      bestNs = Math.min(bestNs, (double) elapsed / calls);
    }
    System.out.printf("%-20s  %12.1f  %14.0f%n", name, bestNs, bytes);
  }

  private static double throughput(int threads, Supplier<ReplicationConfig> op) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    LongAdder total = new LongAdder();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    try {
      for (int t = 0; t < threads; t++) {
        pool.submit(() -> {
          try {
            start.await();
            runFor(op, WARMUP_MS / 4);
            total.add(runFor(op, MEASURE_MS));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
        });
      }
      start.countDown();
      if (!done.await(60, TimeUnit.SECONDS)) {
        throw new AssertionError("benchmark threads did not finish within 60 s");
      }
    } finally {
      pool.shutdownNow();
    }
    return (double) total.sum() / MEASURE_MS;
  }

  /** Calls {@code op} in batches for {@code millis} and returns the number of calls. */
  private static long runFor(Supplier<ReplicationConfig> op, int millis) {
    long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
    long calls = 0;
    int h = 0;
    while (System.nanoTime() < end) {
      for (int i = 0; i < BATCH; i++) {
        h += op.get().getRequiredNodes();
      }
      calls += BATCH;
    }
    sink = h;
    return calls;
  }
}
