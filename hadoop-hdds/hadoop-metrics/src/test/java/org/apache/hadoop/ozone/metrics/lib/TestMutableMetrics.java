/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.metrics.lib;

import static org.apache.hadoop.ozone.metrics.lib.Interns.info;
import static org.apache.hadoop.ozone.metrics.impl.MsInfo.Context;
import static org.apache.hadoop.ozone.metrics.util.MetricsAsserts.anyInfo;
import static org.apache.hadoop.ozone.metrics.util.MetricsAsserts.assertCounter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.AdditionalMatchers.eq;
import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.AdditionalMatchers.leq;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.ozone.metrics.MetricsCollector;
import org.apache.hadoop.ozone.metrics.MetricsInfo;
import org.apache.hadoop.ozone.metrics.MetricsRecordBuilder;
import org.apache.hadoop.ozone.metrics.util.MetricsAsserts;
import org.apache.hadoop.ozone.metrics.util.Quantile;
import com.google.common.math.Stats;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test metrics record builder interface and mutable metrics
 */
public class TestMutableMetrics {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestMutableMetrics.class);
  private static final double EPSILON = 0.00001;
  private static final int SLEEP_TIME_MS = 6 * 1000; // 6 seconds.
  private static final int SAMPLE_COUNT = 1000;

  /**
   * Test the snapshot method
   */
  @Test public void testSnapshot() {
    MetricsRecordBuilder mb = mockMetricsRecordBuilder();

    MetricsRegistry registry = new MetricsRegistry("test");
    registry.newCounter("c1", "int counter", 1);
    registry.newCounter("c2", "long counter", 2L);
    registry.newGauge("g1", "int gauge", 3);
    registry.newGauge("g2", "long gauge", 4L);
    registry.newGauge("g3", "float gauge", 5f);
    registry.newStat("s1", "stat", "Ops", "Time", true).add(0);
    registry.newRate("s2", "stat", false).add(0);

    registry.snapshot(mb, true);

    MutableStat s2 = (MutableStat) registry.get("s2");

    s2.snapshot(mb, true); // should get the same back.
    s2.add(1);
    s2.snapshot(mb, true); // should get new interval values back

    verify(mb).addCounter(info("c1", "int counter"), 1);
    verify(mb).addCounter(info("c2", "long counter"), 2L);
    verify(mb).addGauge(info("g1", "int gauge"), 3);
    verify(mb).addGauge(info("g2", "long gauge"), 4L);
    verify(mb).addGauge(info("g3", "float gauge"), 5f);
    verify(mb).addCounter(info("S1NumOps", "Number of ops for stat"), 1L);
    verify(mb).addGauge(eq(info("S1AvgTime", "Average time for stat")),
                           eq(0.0, EPSILON));
    verify(mb).addGauge(eq(info("S1StdevTime",
                                "Standard deviation of time for stat")),
                           eq(0.0, EPSILON));
    verify(mb).addGauge(eq(info("S1IMinTime",
                                "Interval min time for stat")),
                           eq(0.0, EPSILON));
    verify(mb).addGauge(eq(info("S1IMaxTime",
                                "Interval max time for stat")),
                           eq(0.0, EPSILON));
    verify(mb).addGauge(eq(info("S1MinTime","Min time for stat")),
                           eq(0.0, EPSILON));
    verify(mb).addGauge(eq(info("S1MaxTime","Max time for stat")),
                           eq(0.0, EPSILON));
    verify(mb).addGauge(
        eq(info("S1INumOps", "Interval number of ops for stat")),
        eq(1L));

    verify(mb, times(2))
        .addCounter(info("S2NumOps", "Number of ops for stat"), 1L);
    verify(mb, times(2)).addGauge(eq(info("S2AvgTime",
                                          "Average time for stat")),
                                  eq(0.0, EPSILON));
    verify(mb).addCounter(info("S2NumOps", "Number of ops for stat"), 2L);
    verify(mb).addGauge(eq(info("S2AvgTime", "Average time for stat")),
                           eq(1.0, EPSILON));

    // Add one more sample to s1 and verify that total number of ops
    // has increased to 2, but interval number is 1 for both intervals.
    MutableStat s1 = (MutableStat) registry.get("s1");
    s1.add(0);
    registry.snapshot(mb, true);
    verify(mb).addCounter(info("S1NumOps", "Number of ops for stat"), 2L);
    verify(mb, times(2)).addGauge(
        (eq(info("S1INumOps", "Interval number of ops for stat"))),
        eq(1L));
  }

  interface TestProtocol {
    void foo();
    void bar();
  }

  @Test public void testMutableRates() {
    MetricsRecordBuilder rb = mockMetricsRecordBuilder();
    MetricsRegistry registry = new MetricsRegistry("test");
    MutableRates rates = new MutableRates(registry);

    rates.init(TestProtocol.class);
    registry.snapshot(rb, false);

    assertCounter("FooNumOps", 0L,  rb);
    assertGauge("FooAvgTime", 0.0, rb);
    assertCounter("BarNumOps", 0L, rb);
    assertGauge("BarAvgTime", 0.0, rb);
  }

  @Test public void testMutableRatesWithAggregationInit() {
    MetricsRecordBuilder rb = mockMetricsRecordBuilder();
    MutableRatesWithAggregation rates = new MutableRatesWithAggregation();

    rates.init(TestProtocol.class);
    rates.snapshot(rb, false);

    assertCounter("FooNumOps", 0L, rb);
    assertGauge("FooAvgTime", 0.0, rb);
    assertCounter("BarNumOps", 0L, rb);
    assertGauge("BarAvgTime", 0.0, rb);
  }

  @Test public void testMutableRatesWithAggregationInitWithArray() {
    MetricsRecordBuilder rb = mockMetricsRecordBuilder();
    MutableRatesWithAggregation rates = new MutableRatesWithAggregation();

    rates.init(new String[]{"Foo", "Bar"});
    rates.snapshot(rb, false);

    assertCounter("FooNumOps", 0L, rb);
    assertGauge("FooAvgTime", 0.0, rb);
    assertCounter("BarNumOps", 0L, rb);
    assertGauge("BarAvgTime", 0.0, rb);
  }

  @Test public void testMutableRatesWithAggregationSingleThread() {
    MutableRatesWithAggregation rates = new MutableRatesWithAggregation();

    rates.add("foo", 1);
    rates.add("bar", 5);

    MetricsRecordBuilder rb = mockMetricsRecordBuilder();
    rates.snapshot(rb, false);
    assertCounter("FooNumOps", 1L, rb);
    assertGauge("FooAvgTime", 1.0, rb);
    assertCounter("BarNumOps", 1L, rb);
    assertGauge("BarAvgTime", 5.0, rb);

    rates.add("foo", 1);
    rates.add("foo", 3);
    rates.add("bar", 6);

    rb = mockMetricsRecordBuilder();
    rates.snapshot(rb, false);
    assertCounter("FooNumOps", 3L, rb);
    assertGauge("FooAvgTime", 2.0, rb);
    assertCounter("BarNumOps", 2L, rb);
    assertGauge("BarAvgTime", 6.0, rb);
  }

  @Test public void testMutableRatesWithAggregationManyThreads()
      throws InterruptedException {
    final MutableRatesWithAggregation rates = new MutableRatesWithAggregation();

    final int n = 10;
    long[] opCount = new long[n];
    double[] opTotalTime = new double[n];

    for (int i = 0; i < n; i++) {
      opCount[i] = 0;
      opTotalTime[i] = 0;
      // Initialize so that the getLongCounter() method doesn't complain
      rates.add("metric" + i, 0);
    }

    Thread[] threads = new Thread[n];
    final CountDownLatch firstAddsFinished = new CountDownLatch(threads.length);
    final CountDownLatch firstSnapshotsFinished = new CountDownLatch(1);
    final CountDownLatch secondAddsFinished =
        new CountDownLatch(threads.length);
    final CountDownLatch secondSnapshotsFinished = new CountDownLatch(1);
    long seed = new Random().nextLong();
    LOG.info("Random seed = " + seed);
    final Random sleepRandom = new Random(seed);
    for (int tIdx = 0; tIdx < threads.length; tIdx++) {
      final int threadIdx = tIdx;
      threads[threadIdx] = new Thread() {
        @Override
        public void run() {
          try {
            for (int i = 0; i < 1000; i++) {
              rates.add("metric" + (i % n), (i / n) % 2 == 0 ? 1 : 2);
              // Sleep so additions can be interleaved with snapshots
              Thread.sleep(sleepRandom.nextInt(5));
            }
            firstAddsFinished.countDown();

            // Make sure all threads stay alive long enough for the first
            // snapshot to complete; else their metrics may be lost to GC
            firstSnapshotsFinished.await();

            // Let half the threads continue with more metrics and let half die
            if (threadIdx % 2 == 0) {
              for (int i = 0; i < 1000; i++) {
                rates.add("metric" + (i % n), (i / n) % 2 == 0 ? 1 : 2);
              }
              secondAddsFinished.countDown();
              secondSnapshotsFinished.await();
            } else {
              secondAddsFinished.countDown();
            }
          } catch (InterruptedException e) {
            // Ignore
          }
        }
      };
    }
    for (Thread t : threads) {
      t.start();
    }
    // Snapshot concurrently with additions but aggregate the totals into
    // opCount / opTotalTime
    for (int i = 0; i < 100; i++) {
      snapshotMutableRatesWithAggregation(rates, opCount, opTotalTime);
      Thread.sleep(sleepRandom.nextInt(20));
    }
    firstAddsFinished.await();
    // Final snapshot to grab any remaining metrics and then verify that
    // the totals are as expected
    snapshotMutableRatesWithAggregation(rates, opCount, opTotalTime);
    for (int i = 0; i < n; i++) {
      assertEquals(1001, opCount[i], "metric" + i + " count");
      assertEquals(1500, opTotalTime[i], 1.0, "metric" + i + " total");
    }
    firstSnapshotsFinished.countDown();

    // After half of the threads die, ensure that the remaining ones still
    // add metrics correctly and that snapshot occurs correctly
    secondAddsFinished.await();
    snapshotMutableRatesWithAggregation(rates, opCount, opTotalTime);
    for (int i = 0; i < n; i++) {
      assertEquals(1501, opCount[i], "metric" + i + " count");
      assertEquals(2250, opTotalTime[i], 1.0, "metric" + i + " total");
    }
    secondSnapshotsFinished.countDown();
  }

  private static void snapshotMutableRatesWithAggregation(
          MutableRatesWithAggregation rates, long[] opCount, double[] opTotalTime) {
    MetricsRecordBuilder rb = mockMetricsRecordBuilder();
    rates.snapshot(rb, true);
    for (int i = 0; i < opCount.length; i++) {
      long prevOpCount = opCount[i];
      long newOpCount = MetricsAsserts.getLongCounter("Metric" + i + "NumOps", rb);
      opCount[i] = newOpCount;
      double avgTime = MetricsAsserts.getDoubleGauge("Metric" + i + "AvgTime", rb);
      opTotalTime[i] += avgTime * (newOpCount - prevOpCount);
    }
  }

  /**
   * MutableStat should output 0 instead of the previous state when there is no change.
   */
  @Test public void testMutableWithoutChanged() {
    MetricsRecordBuilder builderWithChange = mockMetricsRecordBuilder();
    MetricsRecordBuilder builderWithoutChange = mockMetricsRecordBuilder();
    MetricsRegistry registry = new MetricsRegistry("test");
    MutableStat stat = registry.newStat("Test", "Test", "Ops", "Val", true);
    stat.add(1000, 1000);
    stat.add(1000, 2000);
    registry.snapshot(builderWithChange, true);

    assertCounter("TestNumOps", 2000L, builderWithChange);
    MetricsAsserts.assertGauge("TestINumOps", 2000L, builderWithChange);
    assertGauge("TestAvgVal", 1.5, builderWithChange);

    registry.snapshot(builderWithoutChange, true);
    MetricsAsserts.assertGauge("TestINumOps", 0L, builderWithoutChange);
    assertGauge("TestAvgVal", 0.0, builderWithoutChange);
  }

  @Test
  public void testDuplicateMetrics() {
    MutableRatesWithAggregation rates = new MutableRatesWithAggregation();
    MutableRatesWithAggregation deferredRpcRates =
        new MutableRatesWithAggregation();
    Class<?> protocol = Long.class;
    rates.init(protocol);
    deferredRpcRates.init(protocol, "Deferred");
    MetricsRecordBuilder rb = mockMetricsRecordBuilder();
    rates.snapshot(rb, true);
    deferredRpcRates.snapshot(rb, true);
    verify(rb, times(1))
        .addCounter(info("GetLongNumOps", "Number of ops for getLong"), 0L);
    verify(rb, times(1)).addCounter(
        info("DeferredGetLongNumOps", "Number of ops for deferredGetLong"), 0L);

    // Add some samples and verify
    rb = mockMetricsRecordBuilder();
    rates.add("testRpcMethod", 10);
    deferredRpcRates.add("testRpcMethod", 100);
    deferredRpcRates.add("testRpcMethod", 500);
    rates.snapshot(rb, true);
    deferredRpcRates.snapshot(rb, true);

    assertCounter("TestRpcMethodNumOps", 1L, rb);
    assertGauge("TestRpcMethodAvgTime", 10.0, rb);
    assertCounter("DeferredTestRpcMethodNumOps", 2L, rb);
    assertGauge("DeferredTestRpcMethodAvgTime", 300.0, rb);
  }

  /**
   * Tests that when using {@link MutableStat#add(long, long)}, even with a high
   * sample count, the mean does not lose accuracy. This also validates that
   * the std dev is correct, assuming samples of equal value.
   */
  @Test
  public void testMutableStatWithBulkAdd() {
    List<Long> samples = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      samples.add(1000L);
    }
    for (int i = 0; i < 1000; i++) {
      samples.add(2000L);
    }
    Stats stats = Stats.of(samples);

    for (int bulkSize : new int[] {1, 10, 100, 1000}) {
      MetricsRecordBuilder rb = mockMetricsRecordBuilder();
      MetricsRegistry registry = new MetricsRegistry("test");
      MutableStat stat = registry.newStat("Test", "Test", "Ops", "Val", true);

      for (int i = 0; i < samples.size(); i += bulkSize) {
        stat.add(bulkSize, samples
            .subList(i, i + bulkSize)
            .stream()
            .mapToLong(Long::longValue)
            .sum()
        );
      }
      registry.snapshot(rb, false);

      assertCounter("TestNumOps", 2000L, rb);
      assertGauge("TestAvgVal", stats.mean(), rb);
      assertGauge("TestStdevVal", stats.sampleStandardDeviation(), rb);
    }
  }

  @Test
  public void testLargeMutableStatAdd() {
    MetricsRecordBuilder rb = mockMetricsRecordBuilder();
    MetricsRegistry registry = new MetricsRegistry("test");
    MutableStat stat = registry.newStat("Test", "Test", "Ops", "Val", true);

    long sample = 1000000000000009L;
    for (int i = 0; i < 100; i++) {
      stat.add(1, sample);
    }
    registry.snapshot(rb, false);

    assertCounter("TestNumOps", 100L, rb);
    assertGauge("TestAvgVal", (double) sample, rb);
    assertGauge("TestStdevVal", 0.0, rb);
  }

  private static void assertGauge(String name, double expected, MetricsRecordBuilder rb) {
    assertThat(MetricsAsserts.getDoubleGauge(name, rb)).as(name)
        .isCloseTo(expected, Offset.offset(EPSILON));
  }
  /**
   * Ensure that quantile estimates from {@link MutableQuantiles} are within
   * specified error bounds.
   */
  @Test
  @Timeout(value = 30)
  public void testMutableQuantilesError() throws Exception {
    MetricsRecordBuilder mb = mockMetricsRecordBuilder();
    MetricsRegistry registry = new MetricsRegistry("test");
    // Use a 5s rollover period
    MutableQuantiles quantiles = registry.newQuantiles("foo", "stat", "Ops",
        "Latency", 5);
    // Push some values in and wait for it to publish
    long startTimeMS = System.currentTimeMillis();
    for (long i = 1; i <= SAMPLE_COUNT; i++) {
      quantiles.add(i);
      quantiles.add(1001 - i);
    }
    long endTimeMS = System.currentTimeMillis();

    Thread.sleep(SLEEP_TIME_MS - (endTimeMS - startTimeMS));

    registry.snapshot(mb, false);

    // Print out the snapshot
    Map<Quantile, Long> previousSnapshot = quantiles.previousSnapshot;
    for (Entry<Quantile, Long> item : previousSnapshot.entrySet()) {
      System.out.println(String.format("Quantile %.2f has value %d",
          item.getKey().quantile, item.getValue()));
    }

    // Verify the results are within our requirements
    verify(mb).addGauge(info("FooNumOps", "Number of ops for stat with 5s interval"), 2000L);
    Quantile[] quants = MutableQuantiles.QUANTILES;
    String name = "Foo%dthPercentileLatency";
    String desc = "%d percentile latency with 5 second interval for stat";
    for (Quantile q : quants) {
      int percentile = (int) (100 * q.quantile);
      int error = (int) (1000 * q.error);
      String n = String.format(name, percentile);
      String d = String.format(desc, percentile);
      long expected = (long) (q.quantile * 1000);
      verify(mb).addGauge(eq(info(n, d)), leq(expected + error));
      verify(mb).addGauge(eq(info(n, d)), geq(expected - error));
    }
  }

  /**
   * Ensure that quantile estimates from {@link org.apache.hadoop.metrics2.lib.MutableInverseQuantiles} are within
   * specified error bounds.
   */
  @Test
  @Timeout(value = 30)
  public void testMutableInverseQuantilesError() throws Exception {
    MetricsRecordBuilder mb = mockMetricsRecordBuilder();
    MetricsRegistry registry = new MetricsRegistry("test");
    // Use a 5s rollover period
    MutableQuantiles inverseQuantiles = registry.newInverseQuantiles("foo", "stat", "Ops",
        "Latency", 5);
    // Push some values in and wait for it to publish
    long startTimeMS = System.currentTimeMillis();
    for (long i = 1; i <= SAMPLE_COUNT; i++) {
      inverseQuantiles.add(i);
      inverseQuantiles.add(1001 - i);
    }
    long endTimeMS = System.currentTimeMillis();

    Thread.sleep(SLEEP_TIME_MS - (endTimeMS - startTimeMS));

    registry.snapshot(mb, false);

    // Verify the results are within our requirements
    verify(mb).addGauge(
        info("FooNumOps", "Number of ops for stat with 5s interval"), 2000L);
    Quantile[] inverseQuants = MutableInverseQuantiles.INVERSE_QUANTILES;
    String name = "Foo%dthInversePercentileLatency";
    String desc = "%d inverse percentile latency with 5 second interval for stat";
    for (Quantile q : inverseQuants) {
      int inversePercentile = (int) (100 * (1 - q.quantile));
      int error = (int) (1000 * q.error);
      String n = String.format(name, inversePercentile);
      String d = String.format(desc, inversePercentile);
      long expected = (long) (q.quantile * 1000);
      verify(mb).addGauge(eq(info(n, d)), leq(expected + error));
      verify(mb).addGauge(eq(info(n, d)), geq(expected - error));
    }
  }

  /**
   * Test that {@link MutableQuantiles} rolls the window over at the specified
   * interval.
   */
  @Test
  @Timeout(value = 30)
  public void testMutableQuantilesRollover() throws Exception {
    MetricsRecordBuilder mb = mockMetricsRecordBuilder();
    MetricsRegistry registry = new MetricsRegistry("test");
    // Use a 5s rollover period
    MutableQuantiles quantiles = registry.newQuantiles("foo", "stat", "Ops",
        "Latency", 5);

    Quantile[] quants = MutableQuantiles.QUANTILES;
    String name = "Foo%dthPercentileLatency";
    String desc = "%d percentile latency with 5 second interval for stat";

    // Push values for three intervals
    long startTimeMS = System.currentTimeMillis();
    for (int i = 1; i <= 3; i++) {
      // Insert the values
      for (long j = 1; j <= SAMPLE_COUNT; j++) {
        quantiles.add(i);
      }
      // Sleep until 1s after the next 5s interval, to let the metrics
      // roll over
      long sleepTimeMS = startTimeMS + (5000L * i) + 1000 - System.currentTimeMillis();
      Thread.sleep(sleepTimeMS);
      // Verify that the window reset, check it has the values we pushed in
      registry.snapshot(mb, false);
      for (Quantile q : quants) {
        int percentile = (int) (100 * q.quantile);
        String n = String.format(name, percentile);
        String d = String.format(desc, percentile);
        verify(mb).addGauge(info(n, d), (long) i);
      }
    }

    // Verify the metrics were added the right number of times
    verify(mb, times(3)).addGauge(
        info("FooNumOps", "Number of ops for stat with 5s interval"), 1000L);
    for (Quantile q : quants) {
      int percentile = (int) (100 * q.quantile);
      String n = String.format(name, percentile);
      String d = String.format(desc, percentile);
      verify(mb, times(3)).addGauge(eq(info(n, d)), anyLong());
    }
  }

  /**
   * Test that {@link MutableInverseQuantiles} rolls the window over at the specified
   * interval.
   */
  @Test
  @Timeout(value = 30)
  public void testMutableInverseQuantilesRollover() throws Exception {
    MetricsRecordBuilder mb = mockMetricsRecordBuilder();
    MetricsRegistry registry = new MetricsRegistry("test");
    // Use a 5s rollover period
    MutableQuantiles inverseQuantiles = registry.newInverseQuantiles("foo", "stat", "Ops",
        "Latency", 5);

    Quantile[] quants = MutableInverseQuantiles.INVERSE_QUANTILES;
    String name = "Foo%dthInversePercentileLatency";
    String desc = "%d inverse percentile latency with 5 second interval for stat";

    // Push values for three intervals
    long startTimeMS = System.currentTimeMillis();
    for (int i = 1; i <= 3; i++) {
      // Insert the values
      for (long j = 1; j <= SAMPLE_COUNT; j++) {
        inverseQuantiles.add(i);
      }
      // Sleep until 1s after the next 5s interval, to let the metrics
      // roll over
      long sleepTimeMS = startTimeMS + (5000L * i) + 1000 - System.currentTimeMillis();
      Thread.sleep(sleepTimeMS);
      // Verify that the window reset, check it has the values we pushed in
      registry.snapshot(mb, false);
      for (Quantile q : quants) {
        int inversePercentile = (int) (100 * (1 - q.quantile));
        String n = String.format(name, inversePercentile);
        String d = String.format(desc, inversePercentile);
        verify(mb).addGauge(info(n, d), (long) i);
      }
    }

    // Verify the metrics were added the right number of times
    verify(mb, times(3)).addGauge(
        info("FooNumOps", "Number of ops for stat with 5s interval"), 1000L);

    for (Quantile q : quants) {
      int inversePercentile = (int) (100 * (1 - q.quantile));
      String n = String.format(name, inversePercentile);
      String d = String.format(desc, inversePercentile);
      verify(mb, times(3)).addGauge(eq(info(n, d)), anyLong());
    }
  }

  /**
   * Test that {@link org.apache.hadoop.metrics2.lib.MutableQuantiles} rolls over correctly even if no items.
   * have been added to the window
   */
  @Test
  @Timeout(value = 30)
  public void testMutableQuantilesEmptyRollover() throws Exception {
    MetricsRecordBuilder mb = mockMetricsRecordBuilder();
    MetricsRegistry registry = new MetricsRegistry("test");
    // Use a 5s rollover period
    MutableQuantiles quantiles = registry.newQuantiles("foo", "stat", "Ops",
        "Latency", 5);

    // Check it initially
    quantiles.snapshot(mb, true);
    verify(mb).addGauge(
        (MetricsInfo) info("FooNumOps", "Number of ops for stat with 5s interval"), 0L);
    Thread.sleep(SLEEP_TIME_MS);
    quantiles.snapshot(mb, false);
    verify(mb, times(2)).addGauge(
        (MetricsInfo) info("FooNumOps", "Number of ops for stat with 5s interval"), 0L);
  }

  /**
   * Test that {@link MutableInverseQuantiles} rolls over correctly even if no items
   * have been added to the window
   */
  @Test
  @Timeout(value = 30)
  public void testMutableInverseQuantilesEmptyRollover() throws Exception {
    MetricsRecordBuilder mb = mockMetricsRecordBuilder();
    MetricsRegistry registry = new MetricsRegistry("test");
    // Use a 5s rollover period
    MutableQuantiles inverseQuantiles = registry.newInverseQuantiles("foo", "stat", "Ops",
        "Latency", 5);

    // Check it initially
    inverseQuantiles.snapshot(mb, true);
    verify(mb).addGauge(info("FooNumOps", "Number of ops for stat with 5s interval"), 0L);
    Thread.sleep(SLEEP_TIME_MS);
    inverseQuantiles.snapshot(mb, false);
    verify(mb, times(2)).addGauge(
        info("FooNumOps", "Number of ops for stat with 5s interval"), 0L);
  }

  /**
   * Test {@link MutableGaugeFloat#incr()}.
   */
  @Test
  @Timeout(value = 30)
  public void testMutableGaugeFloat() {
    MutableGaugeFloat mgf = new MutableGaugeFloat(Context, 3.2f);
    assertEquals(3.2f, mgf.value(), 0.0);
    mgf.incr();
    assertEquals(4.2f, mgf.value(), 0.0);
    mgf.set(Float.NaN);
    assertEquals(Float.NaN, mgf.value(), 0.0);
  }

  /**
   * Duplicate of the class {@link org.apache.ozone.test.MetricsAsserts} method due to incompatible types
   * Should be deleted when HDDS-12799 is in progress
   */
  private static MetricsRecordBuilder mockMetricsRecordBuilder() {
    final MetricsCollector mc = mock(MetricsCollector.class);
    MetricsRecordBuilder rb = mock(MetricsRecordBuilder.class, new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        String methodName = invocation.getMethod().getName();
        return methodName.equals("parent") || methodName.equals("endRecord") ?
            mc : invocation.getMock();
      }
    });
    when(mc.addRecord(anyString())).thenReturn(rb);
    when(mc.addRecord(anyInfo())).thenReturn(rb);
    return rb;
  }
}
