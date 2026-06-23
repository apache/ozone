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

package org.apache.hadoop.ozone.recon;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

/**
 * Tests for {@link TarExtractor}.
 */
public class TestTarExtractor {

  @Test
  public void testStartCreatesFixedThreadPoolWithConfiguredSize() {
    int poolSize = 8;
    String threadPrefix = "TestPrefix-";
    ExecutorService mockExecutor = mock(ExecutorService.class);

    // Construct outside mockStatic block so ThreadFactoryBuilder can
    // use the real Executors.defaultThreadFactory() internally.
    TarExtractor extractor = new TarExtractor(poolSize, threadPrefix);

    try (MockedStatic<Executors> executorsMock = mockStatic(Executors.class)) {
      executorsMock.when(() -> Executors.newFixedThreadPool(
          eq(poolSize), any(ThreadFactory.class))).thenReturn(mockExecutor);

      extractor.start();

      executorsMock.verify(() -> Executors.newFixedThreadPool(
          eq(poolSize), any(ThreadFactory.class)));
    }
  }

  @Test
  public void testThreadFactoryUsesConfiguredPrefix() {
    int poolSize = 4;
    String threadPrefix = "MyCustomPrefix-";
    ExecutorService mockExecutor = mock(ExecutorService.class);
    TarExtractor extractor = new TarExtractor(poolSize, threadPrefix);

    ArgumentCaptor<ThreadFactory> factoryCaptor =
        ArgumentCaptor.forClass(ThreadFactory.class);

    try (MockedStatic<Executors> executorsMock = mockStatic(Executors.class)) {
      executorsMock.when(() -> Executors.newFixedThreadPool(
          eq(poolSize), any(ThreadFactory.class))).thenReturn(mockExecutor);

      extractor.start();

      executorsMock.verify(() -> Executors.newFixedThreadPool(
          eq(poolSize), factoryCaptor.capture()));

      ThreadFactory capturedFactory = factoryCaptor.getValue();
      Thread thread = capturedFactory.newThread(() -> {
      });
      assertTrue(thread.getName().startsWith(threadPrefix),
          "Thread name should start with configured prefix, but was: "
              + thread.getName());
    }
  }

  @Test
  public void testStopShutsDownExecutor() throws InterruptedException {
    int poolSize = 4;
    String threadPrefix = "ShutdownTest-";
    ExecutorService mockExecutor = mock(ExecutorService.class);

    // Construct outside mockStatic block.
    TarExtractor extractor = new TarExtractor(poolSize, threadPrefix);

    try (MockedStatic<Executors> executorsMock = mockStatic(Executors.class)) {
      executorsMock.when(() -> Executors.newFixedThreadPool(
          eq(poolSize), any(ThreadFactory.class))).thenReturn(mockExecutor);
      when(mockExecutor.awaitTermination(60, TimeUnit.SECONDS))
          .thenReturn(true);

      extractor.start();
      extractor.stop();

      verify(mockExecutor).shutdown();
    }
  }

  @Test
  public void testStartIsIdempotent() {
    int poolSize = 4;
    String threadPrefix = "IdempotentTest-";
    ExecutorService mockExecutor = mock(ExecutorService.class);
    TarExtractor extractor = new TarExtractor(poolSize, threadPrefix);

    try (MockedStatic<Executors> executorsMock = mockStatic(Executors.class)) {
      executorsMock.when(() -> Executors.newFixedThreadPool(
          eq(poolSize), any(ThreadFactory.class))).thenReturn(mockExecutor);

      extractor.start();
      extractor.start(); // second call should be a no-op

      // newFixedThreadPool should only be called once
      executorsMock.verify(() -> Executors.newFixedThreadPool(
          eq(poolSize), any(ThreadFactory.class)));
    }
  }
}
