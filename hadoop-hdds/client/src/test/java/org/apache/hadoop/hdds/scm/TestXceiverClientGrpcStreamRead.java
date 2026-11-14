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

package org.apache.hadoop.hdds.scm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for streaming read admission control in XceiverClientGrpc: long-lived block streams are
 * bounded by a dedicated semaphore, separate from the request semaphore.
 */
class TestXceiverClientGrpcStreamRead {

  private static final String MAX_STREAMS_KEY = "ozone.client.stream.read.max-concurrent-streams";

  private static OzoneConfiguration newConf() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(MAX_STREAMS_KEY, 1);
    // Keep the admission wait short so the exhaustion tests fail fast.
    conf.set("ozone.client.stream.read.timeout", "100ms");
    return conf;
  }

  /** Single slot, but a long admission wait, for tests that need a waiter parked in acquireStreamPermit. */
  private static OzoneConfiguration newConfWithLongWait() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(MAX_STREAMS_KEY, 1);
    conf.set("ozone.client.stream.read.timeout", "60s");
    return conf;
  }

  private static Pipeline newPipeline() {
    // Loopback with a free port nothing listens on: a started call is refused locally instead of reaching the network.
    return MockPipeline.createPipeline(
        Collections.singletonList(MockDatanodeDetails.randomLocalDatanodeDetails()));
  }

  /** Starts a thread that waits for a permit and returns it once it is parked in acquireStreamPermit. */
  private static Thread startParkedWaiter(XceiverClientGrpc client, BlockID blockID,
      AtomicReference<IOException> failure, AtomicBoolean interrupted) throws Exception {
    Thread waiter = new Thread(() -> {
      try {
        client.acquireStreamPermit(blockID);
      } catch (IOException e) {
        failure.set(e);
      }
      interrupted.set(Thread.currentThread().isInterrupted());
    }, "stream-permit-waiter");
    waiter.start();
    // Semaphore.tryAcquire(timeout) parks the thread, so this handshake proves it reached the wait.
    GenericTestUtils.waitFor(() -> waiter.getState() == Thread.State.TIMED_WAITING, 10, 10_000);
    return waiter;
  }

  @Test
  void streamAdmissionIsBoundedAndReleasedOnCompletion() throws Exception {
    try (XceiverClientGrpc client = new XceiverClientGrpc(newPipeline(), newConf())) {
      BlockID blockID = new BlockID(1L, 1L);
      client.acquireStreamPermit(blockID);
      assertEquals(0, client.availableStreamPermits());

      // TimeoutIOException: the block input stream retries it under the read retry policy.
      IOException e = assertThrows(TimeoutIOException.class, () -> client.acquireStreamPermit(blockID),
          "opening a stream beyond the limit should time out instead of blocking indefinitely");
      assertThat(e.getMessage()).contains(MAX_STREAMS_KEY);
      assertEquals(0, client.availableStreamPermits(), "a failed wait must not touch the held slot");

      client.completeStreamRead();
      assertEquals(1, client.availableStreamPermits());
      client.acquireStreamPermit(blockID);
      assertEquals(0, client.availableStreamPermits());
      client.completeStreamRead();
    }
  }

  /**
   * A waiter that reaches the slot before the admission timeout gets it: the wait is bounded, not a fail-fast.
   */
  @Test
  void waiterAcquiresSlotReleasedBeforeTimeout() throws Exception {
    try (XceiverClientGrpc client = new XceiverClientGrpc(newPipeline(), newConfWithLongWait())) {
      BlockID blockID = new BlockID(1L, 1L);
      client.acquireStreamPermit(blockID);
      AtomicReference<IOException> failure = new AtomicReference<>();
      AtomicBoolean interrupted = new AtomicBoolean();
      Thread waiter = startParkedWaiter(client, blockID, failure, interrupted);
      assertTrue(waiter.isAlive(), "a second stream must wait while the only slot is held");

      client.completeStreamRead();
      waiter.join(TimeUnit.SECONDS.toMillis(10));
      assertThat(waiter.isAlive()).as("the waiter should have taken the released slot").isFalse();
      assertNull(failure.get());
      assertEquals(0, client.availableStreamPermits(), "the waiter should hold the released slot");
      client.completeStreamRead();
      assertEquals(1, client.availableStreamPermits());
    }
  }

  /**
   * The interrupt exit of acquireStreamPermit holds no permit, so it must not release one either.
   */
  @Test
  void interruptedWaiterFailsWithoutReleasingPermit() throws Exception {
    try (XceiverClientGrpc client = new XceiverClientGrpc(newPipeline(), newConfWithLongWait())) {
      BlockID blockID = new BlockID(1L, 1L);
      client.acquireStreamPermit(blockID);
      AtomicReference<IOException> failure = new AtomicReference<>();
      AtomicBoolean interrupted = new AtomicBoolean();
      Thread waiter = startParkedWaiter(client, blockID, failure, interrupted);

      waiter.interrupt();
      waiter.join(TimeUnit.SECONDS.toMillis(10));
      assertThat(waiter.isAlive()).as("an interrupted waiter should fail promptly").isFalse();
      assertThat(failure.get()).isInstanceOf(InterruptedIOException.class);
      assertThat(failure.get().getCause()).isInstanceOf(InterruptedException.class);
      assertTrue(interrupted.get(), "the interrupt flag must be restored");
      assertEquals(0, client.availableStreamPermits(), "an interrupted wait must not release the held slot");
      client.completeStreamRead();
    }
  }

  /**
   * A started stream keeps its permit until completeStreamRead, and never touches the request semaphore.
   */
  @Test
  void startedStreamHoldsPermitUntilCompleted() throws Exception {
    OzoneConfiguration conf = newConf();
    final int requestPermits = HddsClientUtils.getMaxOutstandingRequests(conf);
    try (XceiverClientGrpc client = new XceiverClientGrpc(newPipeline(), conf)) {
      StreamingReaderSpi reader = mock(StreamingReaderSpi.class);
      // gRPC starts the call lazily, so no datanode needs to be listening for the stream to be started.
      client.initStreamRead(new BlockID(1L, 3L), reader, Collections.emptySet());
      verify(reader).setStreamingReadResponse(any());
      assertEquals(0, client.availableStreamPermits(), "a started stream must keep its permit");
      assertEquals(requestPermits, client.availableRequestPermits(),
          "an open stream must not consume a request permit");

      client.completeStreamRead();
      assertEquals(1, client.availableStreamPermits(), "completeStreamRead must release exactly one permit");
      assertEquals(requestPermits, client.availableRequestPermits());
    }
  }

  /**
   * The pread-after-sequential-read case: a reader whose open stream holds the last slot on this pipeline client
   * and opens another one must fail with an actionable message rather than wait for a permit only it can release.
   */
  @Test
  void secondStreamOnFullClientFailsInsteadOfHanging() throws Exception {
    try (XceiverClientGrpc client = new XceiverClientGrpc(newPipeline(), newConf())) {
      client.initStreamRead(new BlockID(1L, 4L), mock(StreamingReaderSpi.class), Collections.emptySet());
      assertEquals(0, client.availableStreamPermits());

      IOException e = assertThrows(TimeoutIOException.class,
          () -> client.initStreamRead(new BlockID(1L, 5L), mock(StreamingReaderSpi.class), Collections.emptySet()));
      assertThat(e.getMessage()).contains(MAX_STREAMS_KEY).contains("1 block streams are already open");
      assertEquals(0, client.availableStreamPermits(), "the started stream must keep its permit");

      client.completeStreamRead();
      assertEquals(1, client.availableStreamPermits());
    }
  }

  /**
   * With ozone.client.stream.read.max-concurrent-streams unset or non-positive, the stream limit must follow
   * hdds.ratis.raft.client.async.outstanding-requests.max so existing tuning is preserved.
   */
  @Test
  void streamLimitInheritsRequestLimitWhenUnset() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt("hdds.ratis.raft.client.async.outstanding-requests.max", 96);
    try (XceiverClientGrpc client = new XceiverClientGrpc(newPipeline(), conf)) {
      assertEquals(96, client.availableStreamPermits());
    }

    for (int nonPositive : new int[] {0, -2}) {
      conf.setInt(MAX_STREAMS_KEY, nonPositive);
      try (XceiverClientGrpc client = new XceiverClientGrpc(newPipeline(), conf)) {
        assertEquals(96, client.availableStreamPermits(), "explicit " + nonPositive + " should inherit");
      }
    }

    // An explicit stream limit takes precedence over the inherited request limit.
    conf.setInt(MAX_STREAMS_KEY, 7);
    try (XceiverClientGrpc client = new XceiverClientGrpc(newPipeline(), conf)) {
      assertEquals(7, client.availableStreamPermits());
    }
  }

  @Test
  void failedInitStreamReadReleasesPermit() throws Exception {
    XceiverClientGrpc client = new XceiverClientGrpc(newPipeline(), newConf());
    client.close();
    assertThrows(IOException.class,
        () -> client.initStreamRead(new BlockID(1L, 2L), mock(StreamingReaderSpi.class), Collections.emptySet()),
        "initStreamRead on a closed client should fail");
    assertEquals(1, client.availableStreamPermits(),
        "a failed initStreamRead must release the stream permit it acquired");
  }

  @Test
  void allDatanodesExcludedReleasesPermit() throws Exception {
    Pipeline pipeline = newPipeline();
    try (XceiverClientGrpc client = new XceiverClientGrpc(pipeline, newConf())) {
      IOException e = assertThrows(IOException.class,
          () -> client.initStreamRead(new BlockID(1L, 6L), mock(StreamingReaderSpi.class),
              Collections.singleton(pipeline.getFirstNode().getID())),
          "initStreamRead with every datanode excluded should fail");
      assertEquals("Failed to start streaming read to any available DataNodes", e.getMessage());
      assertEquals(1, client.availableStreamPermits(),
          "initStreamRead must release the stream permit when no datanode is tried");
    }
  }

  /**
   * An open that no datanode can serve must fail right away, without queueing for a slot on a full client.
   */
  @Test
  @Timeout(10)
  void allDatanodesExcludedFailsWithoutWaitingForPermit() throws Exception {
    Pipeline pipeline = newPipeline();
    try (XceiverClientGrpc client = new XceiverClientGrpc(pipeline, newConfWithLongWait())) {
      client.acquireStreamPermit(new BlockID(1L, 7L));
      IOException e = assertThrows(IOException.class,
          () -> client.initStreamRead(new BlockID(1L, 8L), mock(StreamingReaderSpi.class),
              Collections.singleton(pipeline.getFirstNode().getID())));
      assertEquals("Failed to start streaming read to any available DataNodes", e.getMessage());
      assertEquals(0, client.availableStreamPermits(), "the held slot must not be touched");
      client.completeStreamRead();
    }
  }
}
