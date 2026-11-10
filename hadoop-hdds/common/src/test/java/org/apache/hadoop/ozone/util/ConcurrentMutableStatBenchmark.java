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

package org.apache.hadoop.ozone.util;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Scalability benchmark comparing {@link MutableStat} and
 * {@link ConcurrentMutableStat} under two load patterns that model the
 * thundering-herd observed when many threads release an
 * {@code OzoneManagerLock} read-lock at the same time.
 *
 * <p><b>Burst</b>: all N threads release simultaneously via a
 * {@link java.util.concurrent.CyclicBarrier}. Reports wall time for the full
 * burst to complete. {@code MutableStat} serialises the work — burst time
 * scales linearly with N. {@code ConcurrentMutableStat} uses cell-striped
 * accumulators — burst time scales sub-linearly.
 *
 * <p><b>Steady-state</b>: all N threads call {@code add()} in a tight loop for
 * a fixed window. Reports total ops/ms. {@code MutableStat} throughput is
 * capped by the serialised mutex regardless of thread count.
 * {@code ConcurrentMutableStat} throughput grows with N.
 *
 * <p>Each measurement method uses its own concrete stat type so the JIT can
 * devirtualize {@code add()} monomorphically and eliminate dispatch overhead.
 *
 * <p>Run with:
 * <pre>
 *   mvn test -pl :hdds-common \
 *     -Dtest=ConcurrentMutableStatBenchmark \
 *     -Dgroups=benchmark -Dexcluded-test-groups= \
 *     -Dsurefire.failIfNoSpecifiedTests=false
 * </pre>
 */
@Tag("benchmark")
public class ConcurrentMutableStatBenchmark {

  private static final int[] THREAD_COUNTS  = {1, 10, 20, 40, 60, 80};

  // Burst scenario
  /** Number of {@code add()} calls each thread makes per burst. */
  private static final int ADDS_PER_BURST   = 500;
  private static final int WARMUP_BURSTS    = 200;
  private static final int MEASURE_BURSTS   = 300;

  // Steady-state scenario
  /** Warm-up window per thread before the measured window starts. */
  private static final int STEADY_WARMUP_MS  = 200;
  /** Measurement window per thread for steady-state throughput. */
  private static final int STEADY_MEASURE_MS = 500;
  /** Ops per {@code System.nanoTime()} poll to amortise timer-call overhead. */
  private static final int STEADY_BATCH      = 1_000;

  @Test
  public void benchmarkBurstScalability() throws Exception {
    System.out.println();
    System.out.printf("%-9s  %-24s  %-32s  %s%n",
        "Threads", "MutableStat µs/burst", "ConcurrentMutableStat µs/burst", "Speedup");
    System.out.println(
        "--------------------------------------------------------------------------------------------");

    for (int threads : THREAD_COUNTS) {
      double baseUs = measureMutableStatBurst(threads);
      double concUs = measureConcurrentMutableStatBurst(threads);
      double speedup = baseUs > 0 ? baseUs / concUs : Double.NaN;
      System.out.printf("%-9d  %-24s  %-32s  %.1fx%n",
          threads, formatUs(baseUs), formatUs(concUs), speedup);
    }
  }

  private static double measureMutableStatBurst(int threads) throws Exception {
    MutableStat stat = new MutableStat("base", "baseline", "Ops", "Time", false);
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CyclicBarrier barrier = new CyclicBarrier(threads + 1);

    for (int t = 0; t < threads; t++) {
      pool.submit(() -> {
        try {
          while (!Thread.currentThread().isInterrupted()) {
            barrier.await();
            for (int i = 0; i < ADDS_PER_BURST; i++) {
              stat.add(ThreadLocalRandom.current().nextLong(1, 10_000));
            }
            barrier.await();
          }
        } catch (InterruptedException | BrokenBarrierException e) {
          Thread.currentThread().interrupt();
        }
      });
    }

    runBursts(barrier, WARMUP_BURSTS);
    long ns = runBursts(barrier, MEASURE_BURSTS);
    pool.shutdownNow();
    pool.awaitTermination(5, TimeUnit.SECONDS);
    return (double) ns / MEASURE_BURSTS / 1000.0;
  }

  private static double measureConcurrentMutableStatBurst(int threads) throws Exception {
    ConcurrentMutableStat stat =
        new ConcurrentMutableStat("conc", "concurrent", "Ops", "Time", false);
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CyclicBarrier barrier = new CyclicBarrier(threads + 1);

    for (int t = 0; t < threads; t++) {
      pool.submit(() -> {
        try {
          while (!Thread.currentThread().isInterrupted()) {
            barrier.await();
            for (int i = 0; i < ADDS_PER_BURST; i++) {
              stat.add(ThreadLocalRandom.current().nextLong(1, 10_000));
            }
            barrier.await();
          }
        } catch (InterruptedException | BrokenBarrierException e) {
          Thread.currentThread().interrupt();
        }
      });
    }

    runBursts(barrier, WARMUP_BURSTS);
    long ns = runBursts(barrier, MEASURE_BURSTS);
    pool.shutdownNow();
    pool.awaitTermination(5, TimeUnit.SECONDS);
    return (double) ns / MEASURE_BURSTS / 1000.0;
  }

  @Test
  public void benchmarkSteadyStateThroughput() throws Exception {
    System.out.println();
    System.out.printf("=== Steady-State (continuous load, %d ms window) ===%n", STEADY_MEASURE_MS);
    System.out.printf("%-9s  %-24s  %-32s  %s%n",
        "Threads", "MutableStat ops/ms", "ConcurrentMutableStat ops/ms", "Speedup");
    System.out.println(
        "--------------------------------------------------------------------------------------------");

    for (int threads : THREAD_COUNTS) {
      double baseOps = measureMutableStatSteady(threads);
      double concOps = measureConcurrentMutableStatSteady(threads);
      double speedup = baseOps > 0 ? concOps / baseOps : Double.NaN;
      System.out.printf("%-9d  %-24s  %-32s  %.1fx%n",
          threads, formatOpsMs(baseOps), formatOpsMs(concOps), speedup);
    }
  }

  private static double measureMutableStatSteady(int threads) throws Exception {
    MutableStat stat = new MutableStat("base", "baseline", "Ops", "Time", false);
    LongAdder totalOps = new LongAdder();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done  = new CountDownLatch(threads);
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    for (int t = 0; t < threads; t++) {
      pool.submit(() -> {
        try {
          start.await();
          ThreadLocalRandom rng = ThreadLocalRandom.current();
          long warmupEnd  = System.nanoTime() + (long) STEADY_WARMUP_MS  * 1_000_000L;
          long measureEnd = warmupEnd          + (long) STEADY_MEASURE_MS * 1_000_000L;
          while (System.nanoTime() < warmupEnd) {
            for (int i = 0; i < STEADY_BATCH; i++) {
              stat.add(rng.nextLong(1, 10_000));
            }
          }
          long ops = 0;
          while (System.nanoTime() < measureEnd) {
            for (int i = 0; i < STEADY_BATCH; i++) {
              stat.add(rng.nextLong(1, 10_000));
            }
            ops += STEADY_BATCH;
          }
          totalOps.add(ops);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
    }
    start.countDown();
    if (!done.await(60, TimeUnit.SECONDS)) {
      throw new AssertionError("steady-state threads did not finish within 60 s");
    }
    pool.shutdownNow();
    pool.awaitTermination(5, TimeUnit.SECONDS);
    return (double) totalOps.sum() / STEADY_MEASURE_MS;
  }

  private static double measureConcurrentMutableStatSteady(int threads) throws Exception {
    ConcurrentMutableStat stat =
        new ConcurrentMutableStat("conc", "concurrent", "Ops", "Time", false);
    LongAdder totalOps = new LongAdder();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done  = new CountDownLatch(threads);
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    for (int t = 0; t < threads; t++) {
      pool.submit(() -> {
        try {
          start.await();
          ThreadLocalRandom rng = ThreadLocalRandom.current();
          long warmupEnd  = System.nanoTime() + (long) STEADY_WARMUP_MS  * 1_000_000L;
          long measureEnd = warmupEnd          + (long) STEADY_MEASURE_MS * 1_000_000L;
          while (System.nanoTime() < warmupEnd) {
            for (int i = 0; i < STEADY_BATCH; i++) {
              stat.add(rng.nextLong(1, 10_000));
            }
          }
          long ops = 0;
          while (System.nanoTime() < measureEnd) {
            for (int i = 0; i < STEADY_BATCH; i++) {
              stat.add(rng.nextLong(1, 10_000));
            }
            ops += STEADY_BATCH;
          }
          totalOps.add(ops);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
    }
    start.countDown();
    if (!done.await(60, TimeUnit.SECONDS)) {
      throw new AssertionError("steady-state threads did not finish within 60 s");
    }
    pool.shutdownNow();
    pool.awaitTermination(5, TimeUnit.SECONDS);
    return (double) totalOps.sum() / STEADY_MEASURE_MS;
  }

  private static String formatOpsMs(double opsPerMs) {
    if (opsPerMs >= 1_000_000) {
      return String.format("%.2f G ops/ms", opsPerMs / 1_000_000);
    } else if (opsPerMs >= 1_000) {
      return String.format("%.2f k ops/ms", opsPerMs / 1_000);
    }
    return String.format("%.2f ops/ms", opsPerMs);
  }

  /**
   * Runs {@code bursts} rounds through the driver side of the barrier and
   * returns total elapsed nanoseconds.
   */
  private static long runBursts(CyclicBarrier barrier, int bursts) throws Exception {
    long total = 0;
    for (int i = 0; i < bursts; i++) {
      long t0 = System.nanoTime();
      barrier.await();
      barrier.await();
      total += System.nanoTime() - t0;
    }
    return total;
  }

  private static String formatUs(double us) {
    if (us >= 1000.0) {
      return String.format("%.2f ms", us / 1000.0);
    }
    return String.format("%.2f µs", us);
  }
}
