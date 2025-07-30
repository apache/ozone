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

import org.apache.hadoop.ozone.metrics.MetricsCollector;
import org.apache.hadoop.ozone.metrics.MetricsException;
import org.apache.hadoop.ozone.metrics.MetricsInfo;
import org.apache.hadoop.ozone.metrics.MetricsRecordBuilder;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.apache.hadoop.ozone.metrics.lib.Interns.info;
import static org.apache.ozone.test.MetricsAsserts.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

/**
 * Test the metric registry class
 */
public class TestMetricsRegistry {

  /**
   * Test various factory methods
   */
  @Test public void testNewMetrics() {
    final MetricsRegistry r = new MetricsRegistry("test");
    r.newCounter("c1", "c1 desc", 1);
    r.newCounter("c2", "c2 desc", 2L);
    r.newGauge("g1", "g1 desc", 3);
    r.newGauge("g2", "g2 desc", 4L);
    r.newGauge("g3", "g3 desc", 5f);
    r.newStat("s1", "s1 desc", "ops", "time");

    assertEquals(6, r.metrics().size(), "num metrics in registry");
    assertTrue(r.get("c1") instanceof MutableCounterInt, "c1 found");
    assertTrue(r.get("c2") instanceof MutableCounterLong, "c2 found");
    assertTrue(r.get("g1") instanceof MutableGaugeInt, "g1 found");
    assertTrue(r.get("g2") instanceof MutableGaugeLong, "g2 found");
    assertTrue(r.get("g3") instanceof MutableGaugeFloat, "g3 found");
    assertTrue(r.get("s1") instanceof MutableStat, "s1 found");

    expectMetricsException("Metric name c1 already exists", new Runnable() {
      @Override
      public void run() { r.newCounter("c1", "test dup", 0); }
    });
  }

  /**
   * Test adding metrics with whitespace in the name
   */
  @Test
  public void testMetricsRegistryIllegalMetricNames() {
    final MetricsRegistry r = new MetricsRegistry("test");
    // Fill up with some basics
    r.newCounter("c1", "c1 desc", 1);
    r.newGauge("g1", "g1 desc", 1);
    r.newQuantiles("q1", "q1 desc", "q1 name", "q1 val type", 1);
    // Add some illegal names
    expectMetricsException("Metric name 'badcount 2' contains "+
        "illegal whitespace character", new Runnable() {
      @Override
      public void run() { r.newCounter("badcount 2", "c2 desc", 2); }
    });
    expectMetricsException("Metric name 'badcount3  ' contains "+
        "illegal whitespace character", new Runnable() {
      @Override
      public void run() { r.newCounter("badcount3  ", "c3 desc", 3); }
    });
    expectMetricsException("Metric name '  badcount4' contains "+
        "illegal whitespace character", new Runnable() {
      @Override
      public void run() { r.newCounter("  badcount4", "c4 desc", 4); }
    });
    expectMetricsException("Metric name 'withtab5	' contains "+
        "illegal whitespace character", new Runnable() {
      @Override
      public void run() { r.newCounter("withtab5	", "c5 desc", 5); }
    });
    expectMetricsException("Metric name 'withnewline6\n' contains "+
        "illegal whitespace character", new Runnable() {
      @Override
      public void run() { r.newCounter("withnewline6\n", "c6 desc", 6); }
    });
    // Final validation
    assertEquals(3, r.metrics().size(), "num metrics in registry");
  }

  /**
   * Test the add by name method
   */
  @Test public void testAddByName() {
    MetricsRecordBuilder rb = mockMetricsRecordBuilder();
    final MetricsRegistry r = new MetricsRegistry("test");
    r.add("s1", 42);
    r.get("s1").snapshot(rb);
    verify(rb).addCounter(info("S1NumOps", "Number of ops for s1"), 1L);
    verify(rb).addGauge(info("S1AvgTime", "Average time for s1"), 42.0);

    r.newCounter("c1", "test add", 1);
    r.newGauge("g1", "test add", 1);

    expectMetricsException("Unsupported add", new Runnable() {
      @Override
      public void run() { r.add("c1", 42); }
    });

    expectMetricsException("Unsupported add", new Runnable() {
      @Override
      public void run() { r.add("g1", 42); }
    });
  }

  /**
   * Test adding illegal parameters
   */
  @Test
  public void testAddIllegalParameters() {
    final MetricsRegistry r = new MetricsRegistry("IllegalParamTest");

    expectMetricsException("Interval should be positive.  Value passed is: -20",
        new Runnable() {
      @Override
      public void run() {
          r.newQuantiles("q1", "New Quantile 1", "qq1", "qv1", (int)-20);
      }
    });
  }

  @Unhealthy
  private void expectMetricsException(String prefix, Runnable fun) {
    try {
      fun.run();
    }
    catch (MetricsException e) {
      assertTrue(e.getMessage().startsWith(prefix), "expected exception");
      return;
    }
    fail("should've thrown '"+ prefix +"...'");
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
    when(mc.addRecord((MetricsInfo) anyInfo())).thenReturn(rb);
    return rb;
  }
}
