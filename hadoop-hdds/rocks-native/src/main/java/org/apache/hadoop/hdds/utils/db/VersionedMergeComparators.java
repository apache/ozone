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

/**
 * Comparison rules used by {@link VersionedKWayMergeIterator}.
 * <p>
 * Implementations supply user-key ordering for grouping and heap-head ordering
 * for k-way merge. Production {@code MinHeapMergeIterator} can keep its own
 * comparator when adapted to this iterator.
 */
public interface VersionedMergeComparators {

  /**
   * Lexicographic order for user keys.
   */
  int compareUserKeys(byte[] left, byte[] right);

  /**
   * Orders entries in the merge heap.
   */
  int compareHeapOrder(VersionedMergeEntry left, VersionedMergeEntry right);

  /**
   * Heap order with stable tie-breaking by source iterator index.
   */
  int compareHeapOrder(VersionedMergeEntry left, int leftIndex,
      VersionedMergeEntry right, int rightIndex);

  default boolean sameUserKey(VersionedMergeEntry left, VersionedMergeEntry right) {
    return compareUserKeys(left.getUserKey(), right.getUserKey()) == 0;
  }
}
