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
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Random load generator which writes, read and deletes keys from
 * the bucket.
 */
public class RandomLoadGenerator implements LoadGenerator {
  private static final Logger LOG =
      LoggerFactory.getLogger(RandomLoadGenerator.class);

  private final List<LoadBucket> ozoneBuckets;
  private final DataBuffer dataBuffer;

  public RandomLoadGenerator(DataBuffer dataBuffer, List<LoadBucket> buckets) {
    this.ozoneBuckets = buckets;
    this.dataBuffer = dataBuffer;
  }

  // Start IO load on an Ozone bucket.
  public void startLoad(long runTimeMillis) {
    long threadID = Thread.currentThread().getId();
    LOG.info("Started Mixed IO Thread:{}.", threadID);
    String threadName = Thread.currentThread().getName();
    long startTime = Time.monotonicNow();

    while (Time.monotonicNow() < startTime + runTimeMillis) {
      LoadBucket bucket =
          ozoneBuckets.get((int) (Math.random() * ozoneBuckets.size()));
      try {
        int index = RandomUtils.nextInt();
        ByteBuffer buffer = dataBuffer.getBuffer(index);
        String keyName = MiniOzoneLoadGenerator.getKeyName(index, threadName);
        bucket.writeKey(buffer, keyName);

        bucket.readKey(buffer, keyName);

        bucket.deleteKey(keyName);
      } catch (Throwable t) {
        LOG.error("LOADGEN: Exiting due to exception", t);
        ExitUtil.terminate(new ExitUtil.ExitException(3, t));
      }
    }
  }

  public void initialize() {
    // Nothing to do here
  }

  @Override
  public String name() {
    return "Random";
  }
}
