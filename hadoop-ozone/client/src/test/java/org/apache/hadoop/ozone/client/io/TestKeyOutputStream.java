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

package org.apache.hadoop.ozone.client.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

/**
 * Tests KeyOutputStream.
 * This is a unit test meant to verify specific behaviors of KeyOutputStream.
 */
public class TestKeyOutputStream {

  @BeforeAll
  static void init() {
    GenericTestUtils.setLogLevel(KeyOutputStreamSemaphore.class, Level.DEBUG);
  }

  @Test
  void testConcurrentWriteLimitOne() throws Exception {
    // Verify the semaphore is working to limit the number of concurrent writes allowed.
    KeyOutputStreamSemaphore sema1 = new KeyOutputStreamSemaphore(1);
    KeyOutputStream keyOutputStream = spy(KeyOutputStream.class);
    when(keyOutputStream.getRequestSemaphore()).thenReturn(sema1);

    final AtomicInteger countWrite = new AtomicInteger(0);
    // mock write()
    doAnswer(invocation -> {
      countWrite.getAndIncrement();
      return invocation.callRealMethod();
    }).when(keyOutputStream).write(any(), anyInt(), anyInt());

    final ConcurrentHashMap<Long, CountDownLatch> mapNotifiers = new ConcurrentHashMap<>();

    final AtomicInteger countHandleWrite = new AtomicInteger(0);
    // mock handleWrite()
    doAnswer(invocation -> {
      final long tid = Thread.currentThread().getId();
      System.out.println("handleWrite() called from tid " + tid);
      final CountDownLatch latch = mapNotifiers.compute(tid, (k, v) ->
          v != null ? v : new CountDownLatch(1));
      countHandleWrite.getAndIncrement();
      // doing some "work"
      latch.await();
      return null;
    }).when(keyOutputStream).handleWrite(any(), anyInt(), anyLong(), anyBoolean());

    final Runnable writeRunnable = () -> {
      try {
        keyOutputStream.write(new byte[4], 0, 4);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };

    final Thread thread1 = new Thread(writeRunnable);
    thread1.start();

    final Thread thread2 = new Thread(writeRunnable);
    thread2.start();

    // Wait for both threads to enter write()
    GenericTestUtils.waitFor(() -> countWrite.get() == 2, 100, 3000);
    // One thread should enter handleWrite()
    GenericTestUtils.waitFor(() -> countHandleWrite.get() == 1, 100, 3000);
    // The other thread is waiting on the semaphore
    GenericTestUtils.waitFor(() -> sema1.getQueueLength() == 1, 100, 3000);

    // handleWrite is triggered only once because of the semaphore and the synchronized block
    verify(keyOutputStream, times(1)).handleWrite(any(), anyInt(), anyLong(), anyBoolean());

    // Now, allow the current thread to finish handleWrite
    // There is only one thread in handleWrite() so mapNotifiers should have only one entry.
    assertEquals(1, mapNotifiers.size());
    Entry<Long, CountDownLatch> entry = mapNotifiers.entrySet().stream().findFirst().get();
    mapNotifiers.remove(entry.getKey());
    entry.getValue().countDown();

    // Wait for the other thread to proceed
    GenericTestUtils.waitFor(() -> countHandleWrite.get() == 2, 100, 3000);
    verify(keyOutputStream, times(2)).handleWrite(any(), anyInt(), anyLong(), anyBoolean());

    // Allow the other thread to finish handleWrite
    entry = mapNotifiers.entrySet().stream().findFirst().get();
    mapNotifiers.remove(entry.getKey());
    entry.getValue().countDown();

    // Let threads finish
    thread2.join();
    thread1.join();
  }
}
