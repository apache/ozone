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

package org.apache.hadoop.hdds.utils.db;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.util.function.CheckedRunnable;

/**
 * ThreadPoolExecutor which throttles on the request before submitting a runnable to a ThreadPoolExecutor.
 */
public class ThrottledThreadpoolExecutor implements Closeable {
  private final ThreadPoolExecutor pool;
  private final AtomicInteger availableTaskSlots;
  private final int maxNumberOfThreads;
  private final Object lock;

  public ThrottledThreadpoolExecutor(int maxNumberOfThreads) {
    pool = new ThreadPoolExecutor(maxNumberOfThreads, maxNumberOfThreads, 5, TimeUnit.MINUTES,
        new LinkedBlockingQueue<>());
    pool.allowCoreThreadTimeOut(true);
    int maxNumberOfTasks = 2 * maxNumberOfThreads;
    availableTaskSlots = new AtomicInteger(maxNumberOfTasks);
    lock = new Object();
    this.maxNumberOfThreads = maxNumberOfThreads;
  }

  public CompletableFuture<Void> submit(CheckedRunnable<?> task) throws InterruptedException {
    waitForQueue();
    CompletableFuture<Void> future = new CompletableFuture<>();
    pool.submit(() -> {
      try {
        task.run();
        future.complete(null);
      } catch (Throwable e) {
        future.completeExceptionally(e);
      } finally {
        availableTaskSlots.incrementAndGet();
        synchronized (lock) {
          lock.notify();
        }
      }
    });
    return future;
  }

  public void waitForQueue() throws InterruptedException {
    synchronized (lock) {
      while (availableTaskSlots.get() <= 0) {
        lock.wait(10000);
      }
      availableTaskSlots.decrementAndGet();
    }
  }

  @Override
  public void close() throws IOException {
    this.pool.shutdown();
  }

  public long getTaskCount() {
    return pool.getTaskCount();
  }

  public int getMaxNumberOfThreads() {
    return maxNumberOfThreads;
  }
}
