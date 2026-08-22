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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.metrics2.util.SampleStat;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ConcurrentMutableStat}.
 */
public class TestConcurrentMutableStat {

  @Test
  public void testSingleThreadedCountAndMean() {
    ConcurrentMutableStat stat = newStat();
    for (int i = 1; i <= 10; i++) {
      stat.add(i);  // sum=55, count=10
    }
    SampleStat last = stat.lastStat();
    assertEquals(10, last.numSamples());
    assertEquals(5.5, last.mean(), 0.001);
  }

  @Test
  public void testSingleThreadedMinMax() {
    ConcurrentMutableStat stat = newStat();
    stat.add(5);
    stat.add(1);
    stat.add(10);
    stat.add(3);
    SampleStat last = stat.lastStat();
    assertEquals(4, last.numSamples());
    assertEquals(1.0, last.min(), 0.001);
    assertEquals(10.0, last.max(), 0.001);
  }

  @Test
  public void testSingleValueMinEqualsMax() {
    ConcurrentMutableStat stat = newStat();
    stat.add(42);
    SampleStat last = stat.lastStat();
    assertEquals(1, last.numSamples());
    assertEquals(42.0, last.min(), 0.001);
    assertEquals(42.0, last.max(), 0.001);
  }

  @Test
  public void testToStringContainsSampleCount() {
    ConcurrentMutableStat stat = newStat();
    for (int i = 0; i < 7; i++) {
      stat.add(100);
    }
    assertThat(stat.toString()).contains("Samples = 7");
  }

  @Test
  public void testConcurrentAddCount() throws InterruptedException {
    ConcurrentMutableStat stat = newStat();
    int threads = 14;
    int addsPerThread = 1000;

    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    ExecutorService pool = Executors.newFixedThreadPool(threads);

    for (int t = 0; t < threads; t++) {
      pool.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < addsPerThread; i++) {
            stat.add(1);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
    }
    start.countDown();
    assertTrue(done.await(30, TimeUnit.SECONDS));
    pool.shutdown();

    assertEquals((long) threads * addsPerThread, stat.lastStat().numSamples());
  }

  @Test
  public void testConcurrentAddMaxValue() throws InterruptedException {
    ConcurrentMutableStat stat = newStat();
    int threads = 14;
    CountDownLatch done = new CountDownLatch(threads);
    ExecutorService pool = Executors.newFixedThreadPool(threads);

    for (int t = 0; t < threads; t++) {
      long value = (t + 1) * 10L;  // 10, 20, ..., 140
      pool.submit(() -> {
        stat.add(value);
        done.countDown();
      });
    }
    assertTrue(done.await(10, TimeUnit.SECONDS));
    pool.shutdown();

    SampleStat last = stat.lastStat();
    assertEquals(threads, last.numSamples());
    assertEquals(140.0, last.max(), 0.001);
  }

  @Test
  public void testConcurrentAddMinValue() throws InterruptedException {
    ConcurrentMutableStat stat = newStat();
    int threads = 14;
    CountDownLatch done = new CountDownLatch(threads);
    ExecutorService pool = Executors.newFixedThreadPool(threads);

    for (int t = 0; t < threads; t++) {
      long value = (t + 1) * 10L;  // 10, 20, ..., 140
      pool.submit(() -> {
        stat.add(value);
        done.countDown();
      });
    }
    assertTrue(done.await(10, TimeUnit.SECONDS));
    pool.shutdown();

    assertEquals(10.0, stat.lastStat().min(), 0.001);
  }

  @Test
  public void testMultipleAddsAndDrains() {
    ConcurrentMutableStat stat = newStat();

    stat.add(1);
    stat.add(3);
    // first drain via toString; interval not reset by toString, samples accumulate
    assertThat(stat.toString()).contains("Samples = 2");

    stat.add(5);
    stat.add(7);
    // interval accumulates until snapshot(); all four samples visible
    assertThat(stat.toString()).contains("Samples = 4");
    assertEquals(4.0, stat.lastStat().mean(), 0.001);
    assertEquals(7.0, stat.lastStat().max(), 0.001);
  }

  private static ConcurrentMutableStat newStat() {
    return new ConcurrentMutableStat("test", "test stat", "Ops", "Time", true);
  }
}
