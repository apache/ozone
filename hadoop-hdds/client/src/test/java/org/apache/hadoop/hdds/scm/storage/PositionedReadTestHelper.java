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

package org.apache.hadoop.hdds.scm.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Shared helpers for concurrent positioned-read unit tests.
 */
public final class PositionedReadTestHelper {

  public static final int THREAD_COUNT = 8;
  public static final int ITERATIONS = 100;
  public static final int BUFFER_SIZE = 4096;
  public static final int SOURCE_SIZE = 512 * 1024;

  /**
   * Performs a positioned read at the given key-relative offset into {@code buf}.
   */
  @FunctionalInterface
  public interface PositionedReadAction {
    void readAtOffset(int offset, ByteBuffer buf) throws Exception;
  }

  private PositionedReadTestHelper() {
  }

  public static void runConcurrentPositionedReads(byte[] source,
      PositionedReadAction action) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < THREAD_COUNT; t++) {
        final int threadId = t;
        futures.add(pool.submit((Callable<Void>) () -> {
          for (int i = 0; i < ITERATIONS; i++) {
            int offset = (threadId * 1000 + i * 17) % (source.length - BUFFER_SIZE);
            ByteBuffer buf = ByteBuffer.allocate(BUFFER_SIZE);
            action.readAtOffset(offset, buf);
            buf.flip();
            byte[] expected = Arrays.copyOfRange(source, offset, offset + BUFFER_SIZE);
            byte[] actual = new byte[BUFFER_SIZE];
            buf.get(actual);
            assertArrayEquals(expected, actual,
                "thread " + threadId + " offset " + offset);
          }
          return null;
        }));
      }
      for (Future<?> future : futures) {
        future.get(1, TimeUnit.MINUTES);
      }
    } finally {
      pool.shutdownNow();
    }
  }

  public static Throwable unwrapExecutionException(
      ExecutionException executionException) {
    Throwable cause = executionException.getCause();
    while (cause instanceof RuntimeException && cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }
}
