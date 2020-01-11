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
package org.apache.hadoop.ozone;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.ozone.utils.LoadBucket;
import org.apache.hadoop.ozone.utils.TestProbability;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Simple Load generator for testing.
 */
public class MiniOzoneLoadGenerator {

  private static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneLoadGenerator.class);

  private static String keyNameDelimiter = "_";

  private ThreadPoolExecutor writeExecutor;
  private int numThreads;
  // number of buffer to be allocated, each is allocated with length which
  // is multiple of 2, each buffer is populated with random data.
  private int numBuffers;
  private List<ByteBuffer> buffers;

  private AtomicBoolean isIOThreadRunning;

  private final List<LoadBucket> ozoneBuckets;

  private final AtomicInteger agedFileWrittenIndex;
  private final ExecutorService agedFileExecutor;
  private final LoadBucket agedLoadBucket;
  private final TestProbability agedWriteProbability;

  private final ThreadPoolExecutor fsExecutor;
  private final LoadBucket fsBucket;

  MiniOzoneLoadGenerator(List<LoadBucket> bucket,
                         LoadBucket agedLoadBucket, LoadBucket fsBucket,
                         int numThreads, int numBuffers) {
    this.ozoneBuckets = bucket;
    this.numThreads = numThreads;
    this.numBuffers = numBuffers;
    this.writeExecutor = createExecutor();

    this.agedFileWrittenIndex = new AtomicInteger(0);
    this.agedFileExecutor = Executors.newSingleThreadExecutor();
    this.agedLoadBucket = agedLoadBucket;
    this.agedWriteProbability = TestProbability.valueOf(10);

    this.fsExecutor = createExecutor();
    this.fsBucket = fsBucket;

    this.isIOThreadRunning = new AtomicBoolean(false);

    // allocate buffers and populate random data.
    buffers = new ArrayList<>();
    for (int i = 0; i < numBuffers; i++) {
      int size = (int) StorageUnit.KB.toBytes(1 << i);
      ByteBuffer buffer = ByteBuffer.allocate(size);
      buffer.put(RandomUtils.nextBytes(size));
      buffers.add(buffer);
    }
  }

  private ThreadPoolExecutor createExecutor() {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(numThreads, numThreads,
        100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1024),
        new ThreadPoolExecutor.CallerRunsPolicy());
    executor.prestartAllCoreThreads();
    return executor;

  }

  // Start IO load on an Ozone bucket.
  private void load(long runTimeMillis) {
    long threadID = Thread.currentThread().getId();
    LOG.info("Started Mixed IO Thread:{}.", threadID);
    String threadName = Thread.currentThread().getName();
    long startTime = Time.monotonicNow();

    while (isIOThreadRunning.get() &&
        (Time.monotonicNow() < startTime + runTimeMillis)) {
      LoadBucket bucket =
          ozoneBuckets.get((int) (Math.random() * ozoneBuckets.size()));
      try {
        int index = RandomUtils.nextInt();
        ByteBuffer buffer = getBuffer(index);
        String keyName = getKeyName(index, threadName);
        bucket.writeKey(buffer, keyName);

        bucket.readKey(buffer, keyName);

        bucket.deleteKey(keyName);
      } catch (Exception e) {
        LOG.error("LOADGEN: Exiting due to exception", e);
        break;
      }
    }
    // This will terminate other threads too.
    isIOThreadRunning.set(false);
    LOG.info("Terminating IO thread:{}.", threadID);
  }

  private Optional<Integer> randomKeyToRead() {
    int currentIndex = agedFileWrittenIndex.get();
    return currentIndex != 0
      ? Optional.of(RandomUtils.nextInt(0, currentIndex))
      : Optional.empty();
  }

  private void startAgedLoad(long runTimeMillis) {
    long threadID = Thread.currentThread().getId();
    LOG.info("AGED LOADGEN: Started Aged IO Thread:{}.", threadID);
    String threadName = Thread.currentThread().getName();
    long startTime = Time.monotonicNow();

    while (isIOThreadRunning.get() &&
        (Time.monotonicNow() < startTime + runTimeMillis)) {

      String keyName = null;
      try {
        if (agedWriteProbability.isTrue()) {
          int index = agedFileWrittenIndex.getAndIncrement();
          ByteBuffer buffer = getBuffer(index);
          keyName = getKeyName(index, threadName);

          agedLoadBucket.writeKey(buffer, keyName);
        } else {
          Optional<Integer> index = randomKeyToRead();
          if (index.isPresent()) {
            ByteBuffer buffer = getBuffer(index.get());
            keyName = getKeyName(index.get(), threadName);
            agedLoadBucket.readKey(buffer, keyName);
          }
        }
      } catch (Throwable t) {
        LOG.error("AGED LOADGEN: {} Exiting due to exception", keyName, t);
        break;
      }
    }
    // This will terminate other threads too.
    isIOThreadRunning.set(false);
    LOG.info("Terminating IO thread:{}.", threadID);
  }

  // Start IO load on an Ozone bucket.
  private void startFsLoad(long runTimeMillis) {
    long threadID = Thread.currentThread().getId();
    LOG.info("Started Filesystem IO Thread:{}.", threadID);
    String threadName = Thread.currentThread().getName();
    long startTime = Time.monotonicNow();

    while (isIOThreadRunning.get() &&
      (Time.monotonicNow() < startTime + runTimeMillis)) {
      try {
        int index = RandomUtils.nextInt();
        ByteBuffer buffer = getBuffer(index);
        String keyName = getKeyName(index, threadName);
        fsBucket.writeKey(true, buffer, keyName);

        fsBucket.readKey(true, buffer, keyName);

        fsBucket.deleteKey(true, keyName);
      } catch (Exception e) {
        LOG.error("LOADGEN: Exiting due to exception", e);
        break;
      }
    }
    // This will terminate other threads too.
    isIOThreadRunning.set(false);
    LOG.info("Terminating IO thread:{}.", threadID);
  }

  void startIO(long time, TimeUnit timeUnit) {
    List<CompletableFuture<Void>> writeFutures = new ArrayList<>();
    LOG.info("Starting MiniOzoneLoadGenerator for time {}:{} with {} buffers " +
            "and {} threads", time, timeUnit, numBuffers, numThreads);
    if (isIOThreadRunning.compareAndSet(false, true)) {
      // Start the IO thread
      for (int i = 0; i < numThreads; i++) {
        writeFutures.add(
            CompletableFuture.runAsync(() -> load(timeUnit.toMillis(time)),
                writeExecutor));
      }

      for (int i = 0; i < numThreads; i++) {
        writeFutures.add(
            CompletableFuture.runAsync(() -> startAgedLoad(
                timeUnit.toMillis(time)), agedFileExecutor));
      }

      for (int i = 0; i < numThreads; i++) {
        writeFutures.add(
            CompletableFuture.runAsync(() -> startFsLoad(
              timeUnit.toMillis(time)), fsExecutor));
      }

      // Wait for IO to complete
      for (CompletableFuture<Void> f : writeFutures) {
        try {
          f.get();
        } catch (Throwable t) {
          LOG.error("startIO failed with exception", t);
        }
      }
    }
  }

  public void shutdownLoadGenerator() {
    try {
      writeExecutor.shutdown();
      writeExecutor.awaitTermination(1, TimeUnit.DAYS);
    } catch (Exception e) {
      LOG.error("error while closing ", e);
    }
  }

  private ByteBuffer getBuffer(int keyIndex) {
    return buffers.get(keyIndex % numBuffers);
  }

  private String getKeyName(int keyIndex, String threadName) {
    return threadName + keyNameDelimiter + keyIndex;
  }
}
