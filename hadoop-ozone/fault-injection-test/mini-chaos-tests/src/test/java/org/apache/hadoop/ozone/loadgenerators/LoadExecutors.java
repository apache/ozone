/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.loadgenerators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Load executors for Ozone, this class provides a pluggable
 * executor for different load generators.
 */
public class LoadExecutors {
  private static final Logger LOG =
      LoggerFactory.getLogger(LoadExecutors.class);

  private final LoadGenerator generator;
  private final int numThreads;
  private final ThreadPoolExecutor executor;
  private final List<CompletableFuture<Void>> futures = new ArrayList<>();

  public LoadExecutors(int numThreads, LoadGenerator generator) {
    this.numThreads = numThreads;
    this.generator = generator;
    this.executor = new ThreadPoolExecutor(numThreads, numThreads,
        100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1024),
        new ThreadPoolExecutor.CallerRunsPolicy());
    executor.prestartAllCoreThreads();
  }

  public void startLoad(long time) {
    LOG.info("Starting {} threads for {}", numThreads, generator.name());
    generator.initialize();
    for (int i = 0; i < numThreads; i++) {
      futures.add(CompletableFuture.runAsync(
          () ->generator.startLoad(time), executor));
    }
  }

  public void waitForCompletion() {
    // Wait for IO to complete
    for (CompletableFuture<Void> f : futures) {
      try {
        f.get();
      } catch (Throwable t) {
        LOG.error("startIO failed with exception", t);
      }
    }
  }

  public void shutdown() {
    try {
      executor.shutdown();
      executor.awaitTermination(1, TimeUnit.DAYS);
    } catch (Exception e) {
      LOG.error("error while closing ", e);
    }
  }
}
