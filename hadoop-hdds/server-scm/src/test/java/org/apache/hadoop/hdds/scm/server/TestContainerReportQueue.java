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

package org.apache.hadoop.hdds.scm.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReport;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;
import org.junit.jupiter.api.Test;

/** Tests for {@link ContainerReportQueue}. */
class TestContainerReportQueue {

  private final ContainerReportQueue queue = new ContainerReportQueue(2);

  private static ContainerReport fcr(DatanodeDetails dn) {
    return new ContainerReportFromDatanode(dn, ContainerReportsProto.getDefaultInstance());
  }

  private static ContainerReport icr(DatanodeDetails dn) {
    return new IncrementalContainerReportFromDatanode(dn, IncrementalContainerReportProto.getDefaultInstance());
  }

  @Test
  void reportsInOrderAndNewFcrReplacesQueuedFcr() {
    final ContainerReportQueue largerQueue = new ContainerReportQueue(3);
    final DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dn2 = MockDatanodeDetails.randomDatanodeDetails();
    final ContainerReport newFcr = fcr(dn1);
    final ContainerReport icr = icr(dn2);
    assertTrue(largerQueue.add(fcr(dn1)));
    assertTrue(largerQueue.add(icr));
    assertTrue(largerQueue.add(newFcr));

    assertEquals(2, largerQueue.size());
    assertEquals(1, largerQueue.getAndResetDropCount(ContainerReportFromDatanode.class.getSimpleName()));
    // the new FCR takes the place of the one it replaced
    assertSame(newFcr, largerQueue.peek());
    assertSame(newFcr, largerQueue.poll());
    assertSame(icr, largerQueue.poll());
    assertTrue(largerQueue.isEmpty());
    assertNull(largerQueue.poll());
  }

  @Test
  void addFailsWhenFull() {
    assertTrue(queue.add(icr(MockDatanodeDetails.randomDatanodeDetails())));
    assertTrue(queue.add(icr(MockDatanodeDetails.randomDatanodeDetails())));

    assertEquals(0, queue.remainingCapacity());
    assertFalse(queue.offer(icr(MockDatanodeDetails.randomDatanodeDetails())));
    assertThrows(IllegalStateException.class, () -> queue.add(icr(MockDatanodeDetails.randomDatanodeDetails())));
  }

  @Test
  void pollTimesOutWhenEmpty() throws Exception {
    final long start = System.nanoTime();
    assertNull(queue.poll(100, TimeUnit.MILLISECONDS));
    assertThat(System.nanoTime() - start).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(100));
  }

  /** A report added while poll is waiting wakes it up, well before its timeout. */
  @Test
  void pollReturnsReportAddedWhileWaiting() throws Exception {
    final ContainerReport report = icr(MockDatanodeDetails.randomDatanodeDetails());
    final long start = System.nanoTime();
    final CompletableFuture<ContainerReport> polled = CompletableFuture.supplyAsync(() -> {
      try {
        return queue.poll(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    });
    Thread.sleep(100);
    queue.add(report);

    assertSame(report, polled.get(10, TimeUnit.SECONDS));
    assertThat(System.nanoTime() - start).isLessThan(TimeUnit.SECONDS.toNanos(10));
  }

  /** Even the longest timeout waits for a report. */
  @Test
  void pollWithLongestTimeoutWaits() throws Exception {
    final ContainerReport report = icr(MockDatanodeDetails.randomDatanodeDetails());
    final CompletableFuture<ContainerReport> polled = CompletableFuture.supplyAsync(() -> {
      try {
        return queue.poll(Long.MAX_VALUE, TimeUnit.DAYS);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    });
    Thread.sleep(100);
    assertFalse(polled.isDone());
    queue.add(report);

    assertSame(report, polled.get(10, TimeUnit.SECONDS));
  }

  @Test
  void takeWaitsForReport() throws Exception {
    final ContainerReport report = fcr(MockDatanodeDetails.randomDatanodeDetails());
    final CompletableFuture<ContainerReport> taken = CompletableFuture.supplyAsync(() -> {
      try {
        return queue.take();
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    });
    Thread.sleep(100);
    assertFalse(taken.isDone());
    queue.add(report);

    assertSame(report, taken.get(10, TimeUnit.SECONDS));
  }

  @Test
  void takeIsInterruptible() throws Exception {
    final CompletableFuture<Throwable> failure = new CompletableFuture<>();
    final Thread taker = new Thread(() -> {
      try {
        queue.take();
        failure.complete(null);
      } catch (Throwable t) {
        failure.complete(t);
      }
    });
    taker.start();
    Thread.sleep(100);
    taker.interrupt();

    assertThat(failure.get(10, TimeUnit.SECONDS)).isInstanceOf(InterruptedException.class);
    taker.join();
  }
}
