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

import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Load executors for Ozone, this class provides a plugable
 * executor for different load generators.
 */
public class LoadExecutors {
  private static final Logger LOG =
      LoggerFactory.getLogger(LoadExecutors.class);

  private final LoadGenerator generator;
  private final int numThreads;
  private final ExecutorService executor;
  private final List<CompletableFuture<Void>> futures = new ArrayList<>();

  public LoadExecutors(int numThreads, LoadGenerator generator) {
    this.numThreads = numThreads;
    this.generator = generator;
    this.executor = Executors.newFixedThreadPool(numThreads);
  }

  private void load(long runTimeMillis) {
    long threadID = Thread.currentThread().getId();
    LOG.info("{} LOADGEN: Started {} IO Thread:{}.",
        generator, threadID);
    long startTime = Time.monotonicNow();

    while (Time.monotonicNow() - startTime < runTimeMillis) {

      try {
        generator.generateLoad();
      } catch (Throwable t) {
        LOG.error("{} LOADGEN: Exiting due to exception",
            generator, t);
        ExitUtil.terminate(new ExitUtil.ExitException(1, t));
        break;
      }
    }
  }


  public void startLoad(long time) {
    LOG.info("Starting {} threads for {}", numThreads, generator);
    try {
      generator.initialize();
      for (int i = 0; i < numThreads; i++) {
        futures.add(CompletableFuture.runAsync(
            () -> load(time), executor));
      }
    } catch (Throwable t) {
      LOG.error("Failed to initialize loadgen:{}", generator, t);
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
