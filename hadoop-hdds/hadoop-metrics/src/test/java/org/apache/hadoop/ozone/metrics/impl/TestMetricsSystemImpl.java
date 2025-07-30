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

package org.apache.hadoop.ozone.metrics.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import java.util.stream.StreamSupport;
import org.apache.hadoop.ozone.metrics.*;
import org.apache.hadoop.ozone.metrics.annotation.Metric;
import org.apache.hadoop.ozone.metrics.annotation.Metrics;
import org.apache.hadoop.ozone.metrics.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.metrics.lib.MetricsRegistry;
import org.apache.hadoop.ozone.metrics.lib.MutableCounterLong;
import org.apache.hadoop.ozone.metrics.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.metrics.lib.MutableRate;
import org.apache.hadoop.ozone.metrics.util.TestHelper;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import static org.apache.hadoop.ozone.metrics.lib.Interns.info;
import static org.apache.hadoop.ozone.metrics.lib.Interns.tag;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Iterables;

import org.apache.commons.configuration2.SubsetConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the MetricsSystemImpl class
 */
@ExtendWith(MockitoExtension.class)
public class TestMetricsSystemImpl {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMetricsSystemImpl.class);

  static { DefaultMetricsSystem.setMiniClusterMode(true); }
  
  @Captor private ArgumentCaptor<MetricsRecord> r1;
  @Captor private ArgumentCaptor<MetricsRecord> r2;
  private static String hostname = MetricsSystemImpl.getHostname();

  public static class TestSink implements MetricsSink {

    private List<Iterable<AbstractMetric>> metricValues = new ArrayList<>();

    @Override public void putMetrics(MetricsRecord record) {
      LOG.debug(record.toString());
      metricValues.add(record.metrics());
    }

    @Override public void flush() {}

    @Override public void init(SubsetConfiguration conf) {
      LOG.debug(MetricsConfig.toString(conf));
    }

    List<Iterable<AbstractMetric>> getMetricValues() {
      return metricValues;
    }
  }

  @Test public void testInitFirstVerifyStopInvokedImmediately() throws Exception {
    DefaultMetricsSystem.shutdown();
    new ConfigBuilder().add("*.period", 8)
        //.add("test.sink.plugin.urls", getPluginUrlsAsString())
        .add("test.sink.test.class", TestSink.class.getName())
        .add("test.*.source.filter.exclude", "s0")
        .add("test.source.s1.metric.filter.exclude", "X*")
        .add("test.sink.sink1.metric.filter.exclude", "Y*")
        .add("test.sink.sink2.metric.filter.exclude", "Y*")
        .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    ms.start();
    ms.register("s0", "s0 desc", new TestSource("s0rec"));
    TestSource s1 = ms.register("s1", "s1 desc", new TestSource("s1rec"));
    s1.c1.incr();
    s1.xxx.incr();
    s1.g1.set(2);
    s1.yyy.incr(2);
    s1.s1.add(0);
    MetricsSink sink1 = mock(MetricsSink.class);
    MetricsSink sink2 = mock(MetricsSink.class);
    ms.registerSink("sink1", "sink1 desc", sink1);
    ms.registerSink("sink2", "sink2 desc", sink2);
    ms.publishMetricsNow(); // publish the metrics
    ms.stop();
    ms.shutdown();

    //When we call stop, at most two sources will be consumed by each sink thread.
    verify(sink1, atMost(2)).putMetrics(r1.capture());
    List<MetricsRecord> mr1 = r1.getAllValues();
    verify(sink2, atMost(2)).putMetrics(r2.capture());
    List<MetricsRecord> mr2 = r2.getAllValues();
    if (mr1.size() != 0 && mr2.size() != 0) {
      checkMetricsRecords(mr1);
      assertEquals(mr1, mr2, "output");
    } else if (mr1.size() != 0) {
      checkMetricsRecords(mr1);
    } else if (mr2.size() != 0) {
      checkMetricsRecords(mr2);
    }
  }

  @Test public void testInitFirstVerifyCallBacks() throws Exception {
    DefaultMetricsSystem.shutdown(); 
    new ConfigBuilder().add("*.period", 8)
        //.add("test.sink.plugin.urls", getPluginUrlsAsString())
        .add("test.sink.test.class", TestSink.class.getName())
        .add("test.*.source.filter.exclude", "s0")
        .add("test.source.s1.metric.filter.exclude", "X*")
        .add("test.sink.sink1.metric.filter.exclude", "Y*")
        .add("test.sink.sink2.metric.filter.exclude", "Y*")
        .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    ms.start();
    ms.register("s0", "s0 desc", new TestSource("s0rec"));
    TestSource s1 = ms.register("s1", "s1 desc", new TestSource("s1rec"));
    s1.c1.incr();
    s1.xxx.incr();
    s1.g1.set(2);
    s1.yyy.incr(2);
    s1.s1.add(0);
    MetricsSink sink1 = mock(MetricsSink.class);
    MetricsSink sink2 = mock(MetricsSink.class);
    ms.registerSink("sink1", "sink1 desc", sink1);
    ms.registerSink("sink2", "sink2 desc", sink2);
    ms.publishMetricsNow(); // publish the metrics

    try {
      verify(sink1, timeout(200).times(2)).putMetrics(r1.capture());
      verify(sink2, timeout(200).times(2)).putMetrics(r2.capture());
    } finally {
      ms.stop();
      ms.shutdown();
    }
    //When we call stop, at most two sources will be consumed by each sink thread.
    List<MetricsRecord> mr1 = r1.getAllValues();
    List<MetricsRecord> mr2 = r2.getAllValues();
    checkMetricsRecords(mr1);
    assertEquals(mr1, mr2, "output");

  }
  
  @Test public void testMultiThreadedPublish() throws Exception {
    final int numThreads = 10;
    new ConfigBuilder().add("*.period", 80)
      .add("test.sink.collector." + MetricsConfig.QUEUE_CAPACITY_KEY,
              numThreads)
      .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    final MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    ms.start();

    final CollectingSink sink = new CollectingSink(numThreads);
    ms.registerSink("collector",
        "Collector of values from all threads.", sink);
    final TestSource[] sources = new TestSource[numThreads];
    final Thread[] threads = new Thread[numThreads];
    final String[] results = new String[numThreads];
    final CyclicBarrier barrier1 = new CyclicBarrier(numThreads),
        barrier2 = new CyclicBarrier(numThreads);
    for (int i = 0; i < numThreads; i++) {
      sources[i] = ms.register("threadSource" + i,
          "A source of my threaded goodness.",
          new TestSource("threadSourceRec" + i));
      threads[i] = new Thread(new Runnable() {
        private boolean safeAwait(int mySource, CyclicBarrier barrier) {
          try {
            barrier.await(2, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            results[mySource] = "Interrupted";
            return false;
          } catch (BrokenBarrierException e) {
            results[mySource] = "Broken Barrier";
            return false;
          } catch (TimeoutException e) {
            results[mySource] = "Timed out on barrier";
            return false;
          }
          return true;
        }
        
        @Override
        public void run() {
          int mySource = Integer.parseInt(Thread.currentThread().getName());
          if (sink.collected[mySource].get() != 0L) {
            results[mySource] = "Someone else collected my metric!";
            return;
          }
          // Wait for all the threads to come here so we can hammer
          // the system at the same time
          if (!safeAwait(mySource, barrier1)) return;
          sources[mySource].g1.set(230);
          ms.publishMetricsNow();
          // Since some other thread may have snatched my metric,
          // I need to wait for the threads to finish before checking.
          if (!safeAwait(mySource, barrier2)) return;
          if (sink.collected[mySource].get() != 230L) {
            results[mySource] = "Metric not collected!";
            return;
          }
          results[mySource] = "Passed";
        }
      }, "" + i);
    }
    for (Thread t : threads)
      t.start();
    for (Thread t : threads)
      t.join();
    assertEquals(0L, ms.droppedPubAll.value());
    assertTrue(Arrays.asList(results).stream().allMatch(
        input -> input.equalsIgnoreCase("Passed")),
        String.join("\n", Arrays.asList(results)));
    ms.stop();
    ms.shutdown();
  }

  private static class CollectingSink implements MetricsSink {
    private final AtomicLong[] collected;
    
    public CollectingSink(int capacity) {
      collected = new AtomicLong[capacity];
      for (int i = 0; i < capacity; i++) {
        collected[i] = new AtomicLong();
      }
    }
    
    @Override
    public void init(SubsetConfiguration conf) {
    }

    @Override
    public void putMetrics(MetricsRecord record) {
      final String prefix = "threadSourceRec";
      if (record.name().startsWith(prefix)) {
        final int recordNumber = Integer.parseInt(
            record.name().substring(prefix.length()));
        ArrayList<String> names = new ArrayList<String>();
        for (AbstractMetric m : record.metrics()) {
          if (m.name().equalsIgnoreCase("g1")) {
            collected[recordNumber].set(m.value().longValue());
            return;
          }
          names.add(m.name());
        }
      }
    }

    @Override
    public void flush() {
    }
  }

  @Test public void testHangingSink() {
    new ConfigBuilder().add("*.period", 8)
      .add("test.sink.test.class", TestSink.class.getName())
      .add("test.sink.hanging.retry.delay", "1")
      .add("test.sink.hanging.retry.backoff", "1.01")
      .add("test.sink.hanging.retry.count", "0")
      .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    ms.start();
    TestSource s = ms.register("s3", "s3 desc", new TestSource("s3rec"));
    s.c1.incr();
    HangingSink hanging = new HangingSink();
    ms.registerSink("hanging", "Hang the sink!", hanging);
    ms.publishMetricsNow();
    assertEquals(1L, ms.droppedPubAll.value());
    assertFalse(hanging.getInterrupted());
    ms.stop();
    ms.shutdown();
    assertTrue(hanging.getInterrupted());
    assertTrue(hanging.getGotCalledSecondTime(),
        "The sink didn't get called after its first hang for subsequent records.");
  }

  private static class HangingSink implements MetricsSink {
    private volatile boolean interrupted;
    private boolean gotCalledSecondTime;
    private boolean firstTime = true;

    public boolean getGotCalledSecondTime() {
      return gotCalledSecondTime;
    }

    public boolean getInterrupted() {
      return interrupted;
    }

    @Override
    public void init(SubsetConfiguration conf) {
    }

    @Override
    public void putMetrics(MetricsRecord record) {
      // No need to hang every time, just the first record.
      if (!firstTime) {
        gotCalledSecondTime = true;
        return;
      }
      firstTime = false;
      try {
        Thread.sleep(10 * 1000);
      } catch (InterruptedException ex) {
        interrupted = true;
      }
    }

    @Override
    public void flush() {
    }
  }

  @Test public void testRegisterDups() {
    MetricsSystem ms = new MetricsSystemImpl();
    TestSource ts1 = new TestSource("ts1");
    TestSource ts2 = new TestSource("ts2");
    ms.register("ts1", "", ts1);
    MetricsSource s1 = ms.getSource("ts1");
    assertNotNull(s1);
    // should work when metrics system is not started
    ms.register("ts1", "", ts2);
    MetricsSource s2 = ms.getSource("ts1");
    assertNotNull(s2);
    assertNotSame(s1, s2);
    ms.shutdown();
  }

  @Test
  public void testRegisterDupError() {
    assertThrows(MetricsException.class, () -> {
      MetricsSystem ms = new MetricsSystemImpl("test");
      TestSource ts = new TestSource("ts");
      ms.register(ts);
      ms.register(ts);
    });
  }

  @Test public void testStartStopStart() {
    DefaultMetricsSystem.shutdown(); // Clear pre-existing source names.
    MetricsSystemImpl ms = new MetricsSystemImpl("test");
    TestSource ts = new TestSource("ts");
    ms.start();
    ms.register("ts", "", ts);
    MetricsSourceAdapter sa = ms.getSourceAdapter("ts");
    assertNotNull(sa);
    assertNotNull(sa.getMBeanName());
    ms.stop();
    ms.shutdown();
    ms.start();
    sa = ms.getSourceAdapter("ts");
    assertNotNull(sa);
    assertNotNull(sa.getMBeanName());
    ms.stop();
    ms.shutdown();
  }

  @Test public void testUnregisterSource() {
    MetricsSystem ms = new MetricsSystemImpl();
    TestSource ts1 = new TestSource("ts1");
    TestSource ts2 = new TestSource("ts2");
    ms.register("ts1", "", ts1);
    ms.register("ts2", "", ts2);
    MetricsSource s1 = ms.getSource("ts1");
    assertNotNull(s1);
    // should work when metrics system is not started
    ms.unregisterSource("ts1");
    s1 = ms.getSource("ts1");
    assertNull(s1);
    MetricsSource s2 = ms.getSource("ts2");
    assertNotNull(s2);
    ms.shutdown();
  }

  @Test public void testRegisterSourceWithoutName() {
    MetricsSystem ms = new MetricsSystemImpl();
    TestSource ts = new TestSource("ts");
    TestSource2 ts2 = new TestSource2("ts2");
    ms.register(ts);
    ms.register(ts2);
    ms.init("TestMetricsSystem");
    // if metrics source is registered without name,
    // the class name will be used as the name
    MetricsSourceAdapter sa = ((MetricsSystemImpl) ms).getSourceAdapter("TestSource");
    assertNotNull(sa);
    MetricsSourceAdapter sa2 = ((MetricsSystemImpl) ms).getSourceAdapter("TestSource2");
    assertNotNull(sa2);
    ms.shutdown();
  }

  private void checkMetricsRecords(List<MetricsRecord> recs) {
    LOG.debug(recs.toString());
    MetricsRecord r = recs.get(0);
    assertEquals("s1rec", r.name(), "name");
    TestHelper.assertEquals("tags", new MetricsTag[] {
      tag(MsInfo.Context, "test"),
      tag(MsInfo.Hostname, hostname)}, r.tags());
    TestHelper.assertEquals("metrics", MetricsLists.builder("")
      .addCounter(info("C1", "C1 desc"), 1L)
      .addGauge(info("G1", "G1 desc"), 2L)
      .addCounter(info("S1NumOps", "Number of ops for s1"), 1L)
      .addGauge(info("S1AvgTime", "Average time for s1"), 0.0)
      .metrics(), r.metrics());

    r = recs.get(1);
    assertTrue(Iterables.contains(r.metrics(), new MetricGaugeInt(MsInfo.NumActiveSinks, 3)),
        "NumActiveSinks should be 3");
    assertTrue(Iterables.contains(r.metrics(), new MetricGaugeInt(MsInfo.NumAllSinks, 3)),
        "NumAllSinks should be 3");
  }

  @Test
  public void testQSize() throws Exception {
    new ConfigBuilder().add("*.period", 8)
        .add("*.queue.capacity", 2)
        .add("test.sink.test.class", TestSink.class.getName())
        .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    final CountDownLatch proceedSignal = new CountDownLatch(1);
    final CountDownLatch reachedPutMetricSignal = new CountDownLatch(1);
    ms.start();
    try {
      MetricsSink slowSink = mock(MetricsSink.class);
      MetricsSink dataSink = mock(MetricsSink.class);
      ms.registerSink("slowSink",
          "The sink that will wait on putMetric", slowSink);
      ms.registerSink("dataSink",
          "The sink I'll use to get info about slowSink", dataSink);
      doAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          reachedPutMetricSignal.countDown();
          proceedSignal.await();
          return null;
        }
      }).when(slowSink).putMetrics(any(MetricsRecord.class));

      // trigger metric collection first time
      ms.onTimerEvent();
      assertTrue(reachedPutMetricSignal.await(1, TimeUnit.SECONDS));
      // Now that the slow sink is still processing the first metric,
      // its queue length should be 1 for the second collection.
      ms.onTimerEvent();
      verify(dataSink, timeout(500).times(2)).putMetrics(r1.capture());
      List<MetricsRecord> mr = r1.getAllValues();
      Number qSize = StreamSupport.stream(mr.get(1).metrics().spliterator(),
          false).filter(
              input -> {
                assert input != null;
                return input.name().equals("Sink_slowSinkQsize");
              }).findFirst().get().value();
      assertEquals(1, qSize);
    } finally {
      proceedSignal.countDown();
      ms.stop();
    }
  }

  /**
   * Class to verify HADOOP-11932. Instead of reading from HTTP, going in loop
   * until closed.
   */
  private static class TestClosableSink implements MetricsSink, Closeable {

    boolean closed = false;
    CountDownLatch collectingLatch;

    public TestClosableSink(CountDownLatch collectingLatch) {
      this.collectingLatch = collectingLatch;
    }

    @Override
    public void init(SubsetConfiguration conf) {
    }

    @Override
    public void close() throws IOException {
      closed = true;
    }

    @Override
    public void putMetrics(MetricsRecord record) {
      while (!closed) {
        collectingLatch.countDown();
      }
    }

    @Override
    public void flush() {
    }
  }

  /**
   * HADOOP-11932
   */
  @Test
  @Timeout(value = 5)
  public void testHangOnSinkRead() throws Exception {
    new ConfigBuilder().add("*.period", 8)
        .add("test.sink.test.class", TestSink.class.getName())
        .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    ms.start();
    try {
      CountDownLatch collectingLatch = new CountDownLatch(1);
      MetricsSink sink = new TestClosableSink(collectingLatch);
      ms.registerSink("closeableSink",
          "The sink will be used to test closeability", sink);
      // trigger metric collection first time
      ms.onTimerEvent();
      // Make sure that sink is collecting metrics
      assertTrue(collectingLatch.await(1, TimeUnit.SECONDS));
    } finally {
      ms.stop();
    }
  }

  @Test
  public void testRegisterSourceJmxCacheTTL() {
    MetricsSystem ms = new MetricsSystemImpl();
    ms.init("TestMetricsSystem");
    TestSource ts = new TestSource("ts");
    ms.register(ts);
    MetricsSourceAdapter sa = ((MetricsSystemImpl) ms)
        .getSourceAdapter("TestSource");
    assertEquals(MetricsConfig.PERIOD_DEFAULT * 1000 + 1,
        sa.getJmxCacheTTL());
    ms.shutdown();
  }

  @Test
  public void testRegisterSinksMultiplePeriods() throws Exception {
    new ConfigBuilder().add("test.sink.test1.period", 100000)
        .add("test.sink.test1.class", TestSink.class.getName())
        .add("test.sink.test2.period", 200000)
        .add("test.sink.test2.class", TestSink.class.getName())
        .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    MetricsSystemImpl ms = new MetricsSystemImpl();
    try {
      ms.init("test");
      TestSink sink1 = (TestSink) ms.getSinkAdapter("test1").sink();
      TestSink sink2 = (TestSink) ms.getSinkAdapter("test2").sink();
      assertEquals(0, sink1.getMetricValues().size());
      assertEquals(0, sink2.getMetricValues().size());
      ms.onTimerEvent();
      // Give some time for the publish event to go through
      GenericTestUtils.waitFor(new BooleanSupplier() {
        @Override
        public boolean getAsBoolean() {
          return sink1.getMetricValues().size() > 0;
        }
      }, 10, 10000);
      assertEquals(1, sink1.getMetricValues().size());
      assertEquals(0, sink2.getMetricValues().size());
      ms.onTimerEvent();
      // Give some time for the publish event to go through
      GenericTestUtils.waitFor(new BooleanSupplier() {
        @Override
        public boolean getAsBoolean() {
          return sink1.getMetricValues().size() > 1 &&
              sink2.getMetricValues().size() > 0;
        }
      }, 10, 10000);
      assertEquals(2, sink1.getMetricValues().size());
      assertEquals(1, sink2.getMetricValues().size());
    } finally {
      ms.shutdown();
    }
  }

  @Metrics(context="test")
  private static class TestSource {
    @Metric("C1 desc") MutableCounterLong c1;
    @Metric("XXX desc") MutableCounterLong xxx;
    @Metric("G1 desc") MutableGaugeLong g1;
    @Metric("YYY desc") MutableGaugeLong yyy;
    @Metric MutableRate s1;
    final MetricsRegistry registry;

    TestSource(String recName) {
      registry = new MetricsRegistry(recName);
    }
  }

  @Metrics(context="test")
  private static class TestSource2 {
    @Metric("C1 desc") MutableCounterLong c1;
    @Metric("XXX desc") MutableCounterLong xxx;
    @Metric("G1 desc") MutableGaugeLong g1;
    @Metric("YYY desc") MutableGaugeLong yyy;
    @Metric MutableRate s1;
    final MetricsRegistry registry;

    TestSource2(String recName) {
      registry = new MetricsRegistry(recName);
    }
  }

  private static String getPluginUrlsAsString() {
    return "file:metrics2-test-plugin.jar";
  }

  @Test
  public void testMetricSystemRestart() {
    MetricsSystemImpl ms = new MetricsSystemImpl("msRestartTestSystem");
    TestSink ts = new TestSink();
    String sinkName = "restartTestSink";

    try {
      ms.start();
      ms.register(sinkName, "", ts);
      assertNotNull(ms.getSinkAdapter(sinkName),
          "no adapter exists for " + sinkName);
      ms.stop();

      ms.start();
      assertNotNull(ms.getSinkAdapter(sinkName),
          "no adapter exists for " + sinkName);
    } finally {
      ms.stop();
    }
  }
}
