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

package org.apache.ozone.erasurecode.rawcoder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for raw coder utility methods.
 */
public class TestCoderUtil {

  private static final int INITIAL_LENGTH = 4096;
  private static final int SMALL_LENGTH = INITIAL_LENGTH + 1;
  private static final int LARGE_LENGTH = SMALL_LENGTH * 2;

  @BeforeEach
  public void resetEmptyChunk() throws Exception {
    Field emptyChunk = CoderUtil.class.getDeclaredField("emptyChunk");
    emptyChunk.setAccessible(true);
    synchronized (CoderUtil.class) {
      emptyChunk.set(null, new byte[INITIAL_LENGTH]);
    }
  }

  @Test
  // HDDS-15341: This can reproduce the race that can make getEmptyChunk()
  // return a buffer shorter than requested, which later causes
  // ArrayIndexOutOfBoundsException when resetBuffer() passes that buffer
  // to System.arraycopy().
  public void getEmptyChunkDoesNotShrinkWhenCacheGrowsConcurrently()
      throws Exception {
    AtomicReference<Thread> workerThread = new AtomicReference<>();
    ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
      Thread thread = new Thread(r, "get-empty-chunk-small");
      workerThread.set(thread);
      return thread;
    });

    try {
      Future<byte[]> smallChunk;
      synchronized (CoderUtil.class) {
        smallChunk = executor.submit(() -> CoderUtil.getEmptyChunk(
            SMALL_LENGTH));
        waitUntilBlocked(workerThread);
        assertThat(CoderUtil.getEmptyChunk(LARGE_LENGTH).length)
            .isGreaterThanOrEqualTo(LARGE_LENGTH);
      }

      assertThat(smallChunk.get(10, TimeUnit.SECONDS).length)
          .as("concurrent caller should return the larger chunk already cached")
          .isGreaterThanOrEqualTo(LARGE_LENGTH);
      assertThat(CoderUtil.getEmptyChunk(LARGE_LENGTH).length)
          .as("empty chunk cache should not shrink")
          .isGreaterThanOrEqualTo(LARGE_LENGTH);
    } finally {
      executor.shutdownNow();
    }
  }

  private static void waitUntilBlocked(AtomicReference<Thread> threadRef)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      Thread thread = threadRef.get();
      if (thread != null && thread.getState() == Thread.State.BLOCKED) {
        return;
      }
      Thread.sleep(10);
    }

    Thread thread = threadRef.get();
    fail("small getEmptyChunk caller did not block on CoderUtil.class; state="
        + (thread == null ? "not started" : thread.getState()));
  }
}
