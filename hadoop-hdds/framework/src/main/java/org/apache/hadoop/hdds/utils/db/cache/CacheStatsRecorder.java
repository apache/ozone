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

package org.apache.hadoop.hdds.utils.db.cache;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Records cache stats.
 */
class CacheStatsRecorder {
  private final AtomicLong cacheHits = new AtomicLong(0);
  private final AtomicLong cacheMisses = new AtomicLong(0);
  private final AtomicLong iterationTimes = new AtomicLong(0);

  public void recordHit() {
    cacheHits.incrementAndGet();
  }

  public void recordMiss() {
    cacheMisses.incrementAndGet();
  }

  public void recordValue(CacheValue<?> value) {
    if (value == null) {
      recordMiss();
    } else {
      recordHit();
    }
  }

  public void recordIteration() {
    iterationTimes.incrementAndGet();
  }

  public CacheStats snapshot() {
    return new CacheStats(
        cacheHits.get(),
        cacheMisses.get(),
        iterationTimes.get()
    );
  }

}
