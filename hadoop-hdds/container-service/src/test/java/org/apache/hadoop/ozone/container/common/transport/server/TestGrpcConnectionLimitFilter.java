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

package org.apache.hadoop.ozone.container.common.transport.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.thirdparty.io.grpc.Attributes;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link GrpcConnectionLimitFilter}.
 */
class TestGrpcConnectionLimitFilter {

  private static final Attributes EMPTY_ATTRS = Attributes.EMPTY;

  @Test
  void testConnectionAcceptedUnderLimit() {
    GrpcConnectionLimitFilter filter = new GrpcConnectionLimitFilter(5);

    // Accept 5 connections (at limit)
    for (int i = 0; i < 5; i++) {
      Attributes result = filter.transportReady(EMPTY_ATTRS);
      assertNotNull(result);
    }

    assertEquals(5, filter.getActiveConnections());
    assertEquals(5, filter.getTotalAcceptedConnections());
    assertEquals(0, filter.getTotalRejectedConnections());
  }

  @Test
  void testConnectionRejectedOverLimit() {
    GrpcConnectionLimitFilter filter = new GrpcConnectionLimitFilter(3);

    for (int i = 0; i < 3; i++) {
      filter.transportReady(EMPTY_ATTRS);
    }

    StatusRuntimeException exception = assertThrows(
        StatusRuntimeException.class,
        () -> filter.transportReady(EMPTY_ATTRS)
    );

    assertEquals(Status.Code.RESOURCE_EXHAUSTED, exception.getStatus().getCode());
    assertEquals(3, filter.getActiveConnections());
    assertEquals(3, filter.getTotalAcceptedConnections());
    assertEquals(1, filter.getTotalRejectedConnections());
  }

  @Test
  void testConnectionTerminationDecrementsCounter() {
    GrpcConnectionLimitFilter filter = new GrpcConnectionLimitFilter(5);

    for (int i = 0; i < 3; i++) {
      filter.transportReady(EMPTY_ATTRS);
    }
    assertEquals(3, filter.getActiveConnections());

    filter.transportTerminated(EMPTY_ATTRS);
    filter.transportTerminated(EMPTY_ATTRS);

    assertEquals(1, filter.getActiveConnections());
    assertEquals(3, filter.getTotalAcceptedConnections());
  }

  @Test
  void testNewConnectionAllowedAfterTermination() {
    GrpcConnectionLimitFilter filter = new GrpcConnectionLimitFilter(2);

    filter.transportReady(EMPTY_ATTRS);
    filter.transportReady(EMPTY_ATTRS);
    assertEquals(2, filter.getActiveConnections());

    assertThrows(StatusRuntimeException.class,
        () -> filter.transportReady(EMPTY_ATTRS));
    assertEquals(1, filter.getTotalRejectedConnections());

    filter.transportTerminated(EMPTY_ATTRS);
    assertEquals(1, filter.getActiveConnections());

    filter.transportReady(EMPTY_ATTRS);
    assertEquals(2, filter.getActiveConnections());
    assertEquals(3, filter.getTotalAcceptedConnections());
  }

  @Test
  void testZeroLimitDisablesLimiting() {
    GrpcConnectionLimitFilter filter = new GrpcConnectionLimitFilter(0);

    for (int i = 0; i < 1000; i++) {
      Attributes result = filter.transportReady(EMPTY_ATTRS);
      assertNotNull(result);
    }

    assertEquals(0, filter.getActiveConnections());
    assertEquals(0, filter.getTotalAcceptedConnections());
    assertEquals(0, filter.getTotalRejectedConnections());
  }

  @Test
  void testNegativeLimitDisablesLimiting() {
    GrpcConnectionLimitFilter filter = new GrpcConnectionLimitFilter(-1);

    for (int i = 0; i < 100; i++) {
      Attributes result = filter.transportReady(EMPTY_ATTRS);
      assertNotNull(result);
    }

    assertEquals(0, filter.getActiveConnections());
  }

  @Test
  void testGetMaxConnections() {
    GrpcConnectionLimitFilter filter = new GrpcConnectionLimitFilter(5000);
    assertEquals(5000, filter.getMaxConnections());
  }

  @Test
  void testConcurrentConnections() throws InterruptedException {
    final int maxConnections = 100;
    final int numThreads = 200;
    GrpcConnectionLimitFilter filter = new GrpcConnectionLimitFilter(maxConnections);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);
    AtomicInteger acceptedCount = new AtomicInteger(0);
    AtomicInteger rejectedCount = new AtomicInteger(0);
    List<Boolean> connectionResults = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          filter.transportReady(EMPTY_ATTRS);
          acceptedCount.incrementAndGet();
          synchronized (connectionResults) {
            connectionResults.add(true);
          }
        } catch (StatusRuntimeException e) {
          rejectedCount.incrementAndGet();
          synchronized (connectionResults) {
            connectionResults.add(false);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    doneLatch.await(10, TimeUnit.SECONDS);
    executor.shutdown();
    
    assertEquals(maxConnections, acceptedCount.get(),
        "Should accept exactly " + maxConnections + " connections");
    assertEquals(numThreads - maxConnections, rejectedCount.get(),
        "Should reject " + (numThreads - maxConnections) + " connections");
    assertEquals(maxConnections, filter.getActiveConnections());
    assertEquals(maxConnections, filter.getTotalAcceptedConnections());
    assertEquals(numThreads - maxConnections, filter.getTotalRejectedConnections());
  }
}
