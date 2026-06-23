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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Computes the expected output of {@link LatestVersionedKWayMergeIterator}.
 * <p>
 * Test-only reference implementation: groups all source records by user key, picks the
 * highest-sequence value and tombstone per key, then applies snapshot-diff emit rules.
 */
final class ExpectedLatestVersionMergeOutput {

  private ExpectedLatestVersionMergeOutput() {
  }

  static List<SourceRecord> fromSourceRecords(List<List<SourceRecord>> perSourceRecords) {
    Map<byte[], List<SourceRecord>> grouped = new TreeMap<>(ExpectedLatestVersionMergeOutput::compareUserKeys);
    for (List<SourceRecord> source : perSourceRecords) {
      for (SourceRecord record : source) {
        grouped.computeIfAbsent(record.userKey, key -> new ArrayList<>()).add(record);
      }
    }

    List<SourceRecord> expected = new ArrayList<>();
    for (List<SourceRecord> versions : grouped.values()) {
      SourceRecord latestValue = null;
      long latestValueSeq = -1L;
      SourceRecord latestTombstone = null;
      long latestTombstoneSeq = -1L;

      for (SourceRecord record : versions) {
        if (record.isTombstone()) {
          if (record.sequence > latestTombstoneSeq) {
            latestTombstone = record;
            latestTombstoneSeq = record.sequence;
          }
        } else if (record.sequence > latestValueSeq) {
          latestValue = record;
          latestValueSeq = record.sequence;
        }
      }

      if (latestValue != null && latestTombstone != null) {
        if (latestValueSeq > latestTombstoneSeq) {
          expected.add(latestTombstone);
          expected.add(latestValue);
        } else {
          expected.add(latestTombstone);
        }
      } else if (latestValue != null) {
        expected.add(latestValue);
      } else if (latestTombstone != null) {
        expected.add(latestTombstone);
      }
    }
    return expected;
  }

  private static int compareUserKeys(byte[] left, byte[] right) {
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

  static final class SourceRecord {
    private final byte[] userKey;
    private final long sequence;
    private final int type;
    private final byte[] value;

    SourceRecord(byte[] userKey, long sequence, int type, byte[] value) {
      this.userKey = userKey;
      this.sequence = sequence;
      this.type = type;
      this.value = value;
    }

    boolean isTombstone() {
      return type != LatestVersionedKWayMergeIterator.ROCKS_TYPE_VALUE;
    }

    byte[] getUserKey() {
      return userKey;
    }

    long getSequence() {
      return sequence;
    }

    int getType() {
      return type;
    }

    byte[] getValue() {
      return value;
    }

    @Override
    public String toString() {
      return "SourceRecord{key=" + Arrays.toString(userKey)
          + ", seq=" + sequence
          + ", type=" + type
          + ", value=" + (value == null ? null : Arrays.toString(value))
          + '}';
    }
  }
}
