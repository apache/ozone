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

package org.apache.hadoop.ozone.om.execution.flowcontrol;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.server.protocol.TermIndex;

/**
 * Context required for execution of a request.
 *
 * <p>Each context carries two epoch values:
 * <ul>
 *   <li>{@link #getIndex()} — the raft log index, scoped per raft group.
 *       Used for raft-specific bookkeeping (lastAppliedIndex, etc.).</li>
 *   <li>{@link #getCacheEpoch()} — a globally unique epoch assigned from a
 *       process-wide counter. Used as the version tag for table cache entries
 *       and their cleanup. A global epoch is required because multiple raft
 *       groups share the same table cache; per-group raft indices overlap and
 *       cause one group's double-buffer flush to prematurely evict another
 *       group's cache entries.</li>
 * </ul>
 */
public final class ExecutionContext {

  /**
   * Process-wide counter that guarantees every transaction across all raft
   * groups receives a unique cache epoch. This prevents epoch collisions
   * in the shared {@code TypedTable} cache when multiple
   * {@code BucketStateMachine} instances flush their double buffers
   * independently.
   */
  private static final AtomicLong GLOBAL_CACHE_EPOCH = new AtomicLong(0);

  private final long index;
  private final long cacheEpoch;
  private final TermIndex termIndex;

  private ExecutionContext(long index, TermIndex termIndex) {
    this.index = index;
    this.cacheEpoch = GLOBAL_CACHE_EPOCH.incrementAndGet();
    if (null == termIndex) {
      termIndex = TermIndex.valueOf(-1, index);
    }
    this.termIndex = termIndex;
  }

  public static ExecutionContext of(long index, TermIndex termIndex) {
    return new ExecutionContext(index, termIndex);
  }

  /**
   * @return the raft log index (per raft group).
   */
  public long getIndex() {
    return index;
  }

  /**
   * @return a globally unique epoch for tagging table cache entries.
   *         Must be used (instead of {@link #getIndex()}) in every
   *         {@code addCacheEntry} / {@code CacheValue.get} call so that
   *         double-buffer cleanup from one raft group never evicts entries
   *         belonging to another group.
   */
  public long getCacheEpoch() {
    return cacheEpoch;
  }

  public TermIndex getTermIndex() {
    return termIndex;
  }
}
