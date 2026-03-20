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

package org.apache.hadoop.hdds.tracing;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Generic span sampler that samples every Nth span.
 * Uses a counter to ensure accurate 1-in-N sampling without dropping
 */
public final class LoopSampler {
  private final long sampleInterval;
  private final AtomicLong counter = new AtomicLong(0);

  /**
   * @param sampleInterval sample every Nth span (e.g. 1000 = 1 in 1000)
   */
  public LoopSampler(long sampleInterval) {
    if (sampleInterval <= 0) {
      throw new IllegalArgumentException("sampleInterval must be positive: " + sampleInterval);
    }
    this.sampleInterval = sampleInterval;
  }

  /**
   * Returns true to sample this span, false to drop.
   * Thread-safe; provides deterministic 1-in-N sampling.
   */
  public boolean shouldSample() {
    long count = counter.incrementAndGet();
    return (count % sampleInterval) == 0;
  }
}
