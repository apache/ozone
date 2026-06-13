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

import java.util.List;
import org.apache.hadoop.hdds.utils.db.VersionedKWayMergeIterator.MergedKeyValue;

/**
 * Decides which versions to emit after all versions of a user key are drained.
 */
@FunctionalInterface
public interface VersionedMergeEmitter {

  /**
   * Append zero or more output records for one user key.
   *
   * @param latestValue highest-sequence non-tombstone for the key, or {@code null}
   * @param latestTombstone highest-sequence tombstone for the key, or {@code null}
   * @param emitTo collector for output records
   */
  void emit(VersionedMergeEntry latestValue, VersionedMergeEntry latestTombstone,
      List<MergedKeyValue> emitTo);
}
