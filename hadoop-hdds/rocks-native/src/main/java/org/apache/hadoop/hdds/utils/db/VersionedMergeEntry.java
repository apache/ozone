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
 * One RocksDB internal-key version exposed to {@link VersionedKWayMergeIterator}.
 * Implementations may defer copying the value bytes until the version wins the merge.
 */
public interface VersionedMergeEntry {

  /** RocksDB {@code ValueType::kTypeValue}. */
  int ROCKS_TYPE_VALUE = 1;

  /** RocksDB user key bytes in bytewise comparator order. */
  byte[] getUserKey();

  /** RocksDB internal sequence number for this version. */
  long getSequence();

  /** RocksDB {@code ValueType} (e.g. {@code kTypeValue == 1}). */
  int getValueType();

  default boolean isTombstone() {
    return getValueType() != ROCKS_TYPE_VALUE;
  }
}
