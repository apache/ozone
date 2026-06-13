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
 * Snapshot-diff emission rules: emit latest tombstone and/or latest value based
 * on which sequence is newer (captures delete-recreate patterns).
 */
public enum LatestVersionMergeEmitter implements VersionedMergeEmitter {
  INSTANCE;

  @Override
  public void emit(VersionedMergeEntry latestValue, VersionedMergeEntry latestTombstone,
      List<MergedKeyValue> emitTo) {
    if (latestValue != null && latestTombstone != null) {
      if (latestValue.getSequence() > latestTombstone.getSequence()) {
        emitTo.add(VersionedKWayMergeIterator.toMergedKeyValue(latestTombstone));
        emitTo.add(VersionedKWayMergeIterator.toMergedKeyValue(latestValue));
      } else {
        emitTo.add(VersionedKWayMergeIterator.toMergedKeyValue(latestTombstone));
      }
    } else if (latestValue != null) {
      emitTo.add(VersionedKWayMergeIterator.toMergedKeyValue(latestValue));
    } else if (latestTombstone != null) {
      emitTo.add(VersionedKWayMergeIterator.toMergedKeyValue(latestTombstone));
    }
  }
}
