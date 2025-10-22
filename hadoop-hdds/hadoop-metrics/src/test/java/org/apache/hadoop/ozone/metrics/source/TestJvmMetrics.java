/*
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

package org.apache.hadoop.ozone.metrics.source;

import org.apache.hadoop.ozone.metrics.MetricsCollector;
import org.apache.hadoop.ozone.metrics.MetricsRecordBuilder;
import org.apache.hadoop.ozone.metrics.impl.MetricsCollectorImpl;
import org.apache.hadoop.ozone.metrics.util.TestHelper;
import org.apache.hadoop.util.GcTimeMonitor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.ozone.metrics.impl.MsInfo.ProcessName;
import static org.apache.hadoop.ozone.metrics.impl.MsInfo.SessionId;
import static org.apache.hadoop.ozone.metrics.util.MetricsAsserts.getMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyFloat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.hadoop.ozone.metrics.source.JvmMetricsInfo.*;

@Timeout(30)
public class TestJvmMetrics {

  private JvmPauseMonitor pauseMonitor;
  private GcTimeMonitor gcTimeMonitor;

  /**
   * Robust shutdown of the monitors if they haven't been stopped already.
   */
  @AfterEach
  public void teardown() {
    ServiceOperations.stop(pauseMonitor);
    if (gcTimeMonitor != null) {
      gcTimeMonitor.shutdown();
    }
  }

  @Test
  public void testJvmPauseMonitorPresence() {
    pauseMonitor = new JvmPauseMonitor();
    pauseMonitor.init(new Configuration());
    pauseMonitor.start();
    JvmMetrics jvmMetrics = new JvmMetrics("test", "test", false);
    jvmMetrics.setPauseMonitor(pauseMonitor);
    MetricsRecordBuilder rb = getMetrics(jvmMetrics);
    MetricsCollector mc = rb.parent();

    verify(mc).addRecord(JvmMetrics);
    verify(rb).tag(ProcessName, "test");
    verify(rb).tag(SessionId, "test");
    for (JvmMetricsInfo info : JvmMetricsInfo.values()) {
      if (info.name().startsWith("Mem")) {
        verify(rb).addGauge(eq(info), anyFloat());
      } else if (info.name().startsWith("Threads")) {
        verify(rb).addGauge(eq(info), anyInt());
      }
    }
  }

  @Test
  public void testGcTimeMonitorPresence() {
    gcTimeMonitor = new GcTimeMonitor(60000, 1000, 70, null);
    gcTimeMonitor.start();
    JvmMetrics jvmMetrics = new JvmMetrics("test", "test", false);
    jvmMetrics.setGcTimeMonitor(gcTimeMonitor);
    MetricsRecordBuilder rb = getMetrics(jvmMetrics);
    MetricsCollector mc = rb.parent();

    verify(mc).addRecord(JvmMetrics);
    verify(rb).tag(ProcessName, "test");
    verify(rb).tag(SessionId, "test");
    for (JvmMetricsInfo info : JvmMetricsInfo.values()) {
      if (info.name().equals("GcTimePercentage")) {
        verify(rb).addGauge(eq(info), anyInt());
      }
    }
  }

  @Test
  public void testDoubleStop() throws Throwable {
    pauseMonitor = new JvmPauseMonitor();
    pauseMonitor.init(new Configuration());
    pauseMonitor.start();
    pauseMonitor.stop();
    pauseMonitor.stop();
  }

  @Test
  public void testDoubleStart() throws Throwable {
    pauseMonitor = new JvmPauseMonitor();
    pauseMonitor.init(new Configuration());
    pauseMonitor.start();
    pauseMonitor.start();
    pauseMonitor.stop();
  }

  @Test
  public void testStopBeforeStart() throws Throwable {
    pauseMonitor = new JvmPauseMonitor();
    try {
      pauseMonitor.init(new Configuration());
      pauseMonitor.stop();
      pauseMonitor.start();
      fail("Expected an exception, got " + pauseMonitor);
    } catch (ServiceStateException e) {
      TestHelper.assertExceptionContains("cannot enter state", e);
    }
  }

  @Test
  public void testStopBeforeInit() throws Throwable {
    pauseMonitor = new JvmPauseMonitor();
    try {
      pauseMonitor.stop();
      pauseMonitor.init(new Configuration());
      fail("Expected an exception, got " + pauseMonitor);
    } catch (ServiceStateException e) {
      TestHelper.assertExceptionContains("cannot enter state", e);
    }
  }

  @Test
  public void testGcTimeMonitor() {
    class Alerter implements GcTimeMonitor.GcTimeAlertHandler {
      private volatile int numAlerts;
      private volatile int maxGcTimePercentage;
      @Override
      public void alert(GcTimeMonitor.GcData gcData) {
        numAlerts++;
        if (gcData.getGcTimePercentage() > maxGcTimePercentage) {
          maxGcTimePercentage = gcData.getGcTimePercentage();
        }
      }
    }
    Alerter alerter = new Alerter();

    int alertGcPerc = 10;  // Alerter should be called if GC takes >= 10%
    gcTimeMonitor = new GcTimeMonitor(60*1000, 100, alertGcPerc, alerter);
    gcTimeMonitor.start();

    int maxGcTimePercentage = 0;
    long gcCount = 0;

    // Generate a lot of garbage for some time and verify that the monitor
    // reports at least some percentage of time in GC pauses, and that the
    // alerter is invoked at least once.

    List<String> garbageStrings = new ArrayList<>();

    long startTime = System.currentTimeMillis();
    // Run this for at least 1 sec for our monitor to collect enough data
    while (System.currentTimeMillis() - startTime < 1000) {
      for (int j = 0; j < 100000; j++) {
        garbageStrings.add(
            "Long string prefix just to fill memory with garbage " + j);
      }
      garbageStrings.clear();
      System.gc();

      GcTimeMonitor.GcData gcData = gcTimeMonitor.getLatestGcData();
      int gcTimePercentage = gcData.getGcTimePercentage();
      if (gcTimePercentage > maxGcTimePercentage) {
        maxGcTimePercentage = gcTimePercentage;
      }
      gcCount = gcData.getAccumulatedGcCount();
    }

    assertTrue(maxGcTimePercentage > 0);
    assertTrue(gcCount > 0);
    assertTrue(alerter.numAlerts > 0);
    assertTrue(alerter.maxGcTimePercentage >= alertGcPerc);
  }

  @Test
  public void testJvmMetricsSingletonWithSameProcessName() {
    org.apache.hadoop.metrics2.source.JvmMetrics jvmMetrics1 = org.apache.hadoop.metrics2.source.JvmMetrics
        .initSingleton("test", null);
    org.apache.hadoop.metrics2.source.JvmMetrics jvmMetrics2 = org.apache.hadoop.metrics2.source.JvmMetrics
        .initSingleton("test", null);
    assertEquals(jvmMetrics1, jvmMetrics2,
        "initSingleton should return the singleton instance");
  }

  @Test
  public void testJvmMetricsSingletonWithDifferentProcessNames() {
    final String process1Name = "process1";
    JvmMetrics jvmMetrics1 = org.apache.hadoop.ozone.metrics.source.JvmMetrics.initSingleton(process1Name, null);
    final String process2Name = "process2";
    JvmMetrics jvmMetrics2 = org.apache.hadoop.ozone.metrics.source.JvmMetrics.initSingleton(process2Name, null);
    assertEquals(jvmMetrics1, jvmMetrics2,
        "initSingleton should return the singleton instance");
    assertEquals(process1Name, jvmMetrics1.processName,
        "unexpected process name of the singleton instance");
    assertEquals(process1Name, jvmMetrics2.processName,
        "unexpected process name of the singleton instance");
  }

  /**
   * Performance test for JvmMetrics#getMetrics, comparing performance of
   * getting thread usage from ThreadMXBean with that from ThreadGroup.
   */
  @Test
  public void testGetMetricsPerf() {
    JvmMetrics jvmMetricsUseMXBean = new JvmMetrics("test", "test", true);
    JvmMetrics jvmMetrics = new JvmMetrics("test", "test", false);
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    // warm up
    jvmMetrics.getMetrics(collector, true);
    jvmMetricsUseMXBean.getMetrics(collector, true);
    // test cases with different numbers of threads
    int[] numThreadsCases = {100, 200, 500, 1000, 2000, 3000};
    List<TestThread> threads = new ArrayList();
    for (int numThreads : numThreadsCases) {
      updateThreadsAndWait(threads, numThreads);
      long startNs = System.nanoTime();
      jvmMetricsUseMXBean.getMetrics(collector, true);
      long processingNsFromMXBean = System.nanoTime() - startNs;
      startNs = System.nanoTime();
      jvmMetrics.getMetrics(collector, true);
      long processingNsFromGroup = System.nanoTime() - startNs;
      System.out.println(
          "#Threads=" + numThreads + ", ThreadMXBean=" + processingNsFromMXBean
              + " ns, ThreadGroup=" + processingNsFromGroup + " ns, ratio: " + (
              processingNsFromMXBean / processingNsFromGroup));
    }
    // cleanup
    updateThreadsAndWait(threads, 0);
  }

  private static void updateThreadsAndWait(List<TestThread> threads,
      int expectedNumThreads) {
    // add/remove threads according to expected number
    int addNum = expectedNumThreads - threads.size();
    if (addNum > 0) {
      for (int i = 0; i < addNum; i++) {
        TestThread testThread = new TestThread();
        testThread.start();
        threads.add(testThread);
      }
    } else if (addNum < 0) {
      for (int i = 0; i < Math.abs(addNum); i++) {
        threads.get(i).exit = true;
      }
    } else {
      return;
    }
    // wait for threads to reach the expected number
    while (true) {
      Iterator<TestThread> it = threads.iterator();
      while (it.hasNext()) {
        if (it.next().exited) {
          it.remove();
        }
      }
      if (threads.size() == expectedNumThreads) {
        break;
      } else {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          //ignore
        }
      }
    }
  }

  static class TestThread extends Thread {
    private volatile boolean exit = false;
    private boolean exited = false;
    @Override
    public void run() {
      while (!exit) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      exited = true;
    }
  }
}
