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
 * Default RocksDB comparison rules for snapshot-diff latest-version merge:
 * user key ascending, sequence descending (newest first), tombstone before value
 * at the same sequence.
 */
public enum LatestVersionMergeComparators implements VersionedMergeComparators {
  INSTANCE;

  @Override
  public int compareUserKeys(byte[] left, byte[] right) {
    if (left == right) {
      return 0;
    }
    if (left == null) {
      return -1;
    }
    if (right == null) {
      return 1;
    }
    int minLength = Math.min(left.length, right.length);
    for (int i = 0; i < minLength; i++) {
      int l = left[i] & 0xff;
      int r = right[i] & 0xff;
      if (l != r) {
        return Integer.compare(l, r);
      }
    }
    return Integer.compare(left.length, right.length);
  }

  @Override
  public int compareHeapOrder(VersionedMergeEntry left, VersionedMergeEntry right) {
    int keyCompare = compareUserKeys(left.getUserKey(), right.getUserKey());
    if (keyCompare != 0) {
      return keyCompare;
    }
    int seqCompare = Long.compare(right.getSequence(), left.getSequence());
    if (seqCompare != 0) {
      return seqCompare;
    }
    return Boolean.compare(right.isTombstone(), left.isTombstone());
  }

  @Override
  public int compareHeapOrder(VersionedMergeEntry left, int leftIndex,
      VersionedMergeEntry right, int rightIndex) {
    int result = compareHeapOrder(left, right);
    if (result == 0) {
      return Integer.compare(leftIndex, rightIndex);
    }
    return result;
  }
}
