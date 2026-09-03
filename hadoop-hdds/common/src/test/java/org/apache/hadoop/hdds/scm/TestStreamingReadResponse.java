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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCallStreamObserver;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StreamingReadResponse#awaitReady(long)}.
 */
class TestStreamingReadResponse {

  private static final class FakeRequestObserver extends ClientCallStreamObserver<ContainerCommandRequestProto> {
    private final AtomicBoolean ready = new AtomicBoolean();

    @Override
    public boolean isReady() {
      return ready.get();
    }

    @Override
    public void setOnReadyHandler(Runnable onReadyHandler) {
    }

    @Override
    public void disableAutoInboundFlowControl() {
    }

    @Override
    public void request(int count) {
    }

    @Override
    public void setMessageCompression(boolean enable) {
    }

    @Override
    public void cancel(String message, Throwable cause) {
    }

    @Override
    public void onNext(ContainerCommandRequestProto value) {
    }

    @Override
    public void onError(Throwable t) {
    }

    @Override
    public void onCompleted() {
    }
  }

  private static StreamingReadResponse newResponse(FakeRequestObserver observer) {
    return new StreamingReadResponse(MockDatanodeDetails.randomDatanodeDetails(), observer);
  }

  @Test
  void returnsImmediatelyWhenAlreadyReady() throws Exception {
    final FakeRequestObserver observer = new FakeRequestObserver();
    observer.ready.set(true);
    assertTrue(newResponse(observer).awaitReady(TimeUnit.SECONDS.toNanos(10)));
  }

  @Test
  void timesOutWithoutASignal() throws Exception {
    final FakeRequestObserver observer = new FakeRequestObserver();
    final long start = System.nanoTime();
    assertFalse(newResponse(observer).awaitReady(TimeUnit.MILLISECONDS.toNanos(50)));
    assertTrue(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(50), "must wait out the timeout");
  }

  @Test
  void wakesOnSignalReady() throws Exception {
    final FakeRequestObserver observer = new FakeRequestObserver();
    final StreamingReadResponse response = newResponse(observer);
    final CountDownLatch waiting = new CountDownLatch(1);
    final AtomicBoolean result = new AtomicBoolean();
    final Thread waiter = new Thread(() -> {
      try {
        waiting.countDown();
        result.set(response.awaitReady(TimeUnit.SECONDS.toNanos(30)));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
    waiter.start();
    assertTrue(waiting.await(10, TimeUnit.SECONDS));

    observer.ready.set(true);
    response.signalReady();
    waiter.join(TimeUnit.SECONDS.toMillis(10));
    assertFalse(waiter.isAlive(), "waiter did not return");
    assertTrue(result.get());
  }

  @Test
  void failsImmediatelyWhenAlreadyTerminated() {
    final FakeRequestObserver observer = new FakeRequestObserver();
    final StreamingReadResponse response = newResponse(observer);
    final Throwable cause = new RuntimeException("boom");
    response.signalTerminated(cause);
    final long start = System.nanoTime();
    final IOException e = assertThrows(IOException.class, () -> response.awaitReady(TimeUnit.SECONDS.toNanos(30)));
    assertSame(cause, e.getCause());
    assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5), "must not wait out the timeout");
  }

  @Test
  void wakesOnSignalTerminated() throws Exception {
    final FakeRequestObserver observer = new FakeRequestObserver();
    final StreamingReadResponse response = newResponse(observer);
    final CountDownLatch waiting = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread waiter = new Thread(() -> {
      try {
        waiting.countDown();
        response.awaitReady(TimeUnit.SECONDS.toNanos(30));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (IOException e) {
        failure.set(e);
      }
    });
    waiter.start();
    assertTrue(waiting.await(10, TimeUnit.SECONDS));

    response.signalTerminated(null);
    waiter.join(TimeUnit.SECONDS.toMillis(10));
    assertFalse(waiter.isAlive(), "waiter did not return");
    assertTrue(failure.get() instanceof IOException, "waiter was not failed by signalTerminated");
  }
}
