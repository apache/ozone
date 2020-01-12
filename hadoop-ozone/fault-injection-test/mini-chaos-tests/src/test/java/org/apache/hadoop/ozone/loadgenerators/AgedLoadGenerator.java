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

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.ozone.MiniOzoneLoadGenerator;
import org.apache.hadoop.ozone.utils.LoadBucket;
import org.apache.hadoop.ozone.utils.TestProbability;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Aged Load Generator for Ozone.
 *
 * This Load Generator reads and write key to an Ozone bucket.
 *
 * The defautl writes to read ratio is 10:90.
 */
public class AgedLoadGenerator implements LoadGenerator {

  private static final Logger LOG =
      LoggerFactory.getLogger(AgedLoadGenerator.class);
  private static String fileSuffex = "aged";

  private final AtomicInteger agedFileWrittenIndex;
  private final AtomicInteger agedFileAllocationIndex;
  private final LoadBucket agedLoadBucket;
  private final TestProbability agedWriteProbability;
  private final DataBuffer dataBuffer;

  public AgedLoadGenerator(DataBuffer data, LoadBucket agedLoadBucket) {
    this.dataBuffer = data;
    this.agedFileWrittenIndex = new AtomicInteger(0);
    this.agedFileAllocationIndex = new AtomicInteger(0);
    this.agedLoadBucket = agedLoadBucket;
    this.agedWriteProbability = TestProbability.valueOf(10);
  }

  public void startLoad(long runTimeMillis) {
    long threadID = Thread.currentThread().getId();
    LOG.info("AGED LOADGEN: Started Aged IO Thread:{}.", threadID);
    long startTime = Time.monotonicNow();

    while (Time.monotonicNow() < startTime + runTimeMillis) {

      String keyName = null;
      try {
        if (agedWriteProbability.isTrue()) {
          synchronized (agedFileAllocationIndex) {
            int index = agedFileAllocationIndex.getAndIncrement();
            ByteBuffer buffer = dataBuffer.getBuffer(index);
            keyName = MiniOzoneLoadGenerator.getKeyName(index, fileSuffex);
            agedLoadBucket.writeKey(buffer, keyName);
            agedFileWrittenIndex.getAndIncrement();
          }
        } else {
          Optional<Integer> index = randomKeyToRead();
          if (index.isPresent()) {
            ByteBuffer buffer = dataBuffer.getBuffer(index.get());
            keyName =
                MiniOzoneLoadGenerator.getKeyName(index.get(), fileSuffex);
            agedLoadBucket.readKey(buffer, keyName);
          }
        }
      } catch (Throwable t) {
        LOG.error("AGED LOADGEN: {} Exiting due to exception", keyName, t);
        ExitUtil.terminate(new ExitUtil.ExitException(1, t));
        break;
      }
    }
  }

  private Optional<Integer> randomKeyToRead() {
    int currentIndex = agedFileWrittenIndex.get();
    return currentIndex != 0
        ? Optional.of(RandomUtils.nextInt(0, currentIndex))
        : Optional.empty();
  }

  public void initialize() {
    // Nothing to do here
  }

  @Override
  public String name() {
    return "Aged Load";
  }
}
