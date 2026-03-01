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

package org.apache.ozone.rocksdiff;

import java.util.Objects;
import org.apache.ozone.rocksdb.util.SstFileInfo;

/**
 * Node representing a single L0 SST file created by flush operation.
 * This is a simpler version of CompactionNode, used in FlushLinkedList
 * for tracking flush operations instead of compaction DAG.
 */
public class FlushNode extends SstFileInfo {
  private final long snapshotGeneration;
  private final long flushTime;

  /**
   * FlushNode constructor.
   *
   * @param fileName           SST file name (without extension)
   * @param snapshotGeneration Snapshot generation (DB sequence number)
   * @param flushTime          Timestamp when flush completed (milliseconds)
   * @param startKey           Smallest key in the SST file
   * @param endKey             Largest key in the SST file
   * @param columnFamily       Column family name
   */
  public FlushNode(String fileName,
                   long snapshotGeneration,
                   long flushTime,
                   String startKey,
                   String endKey,
                   String columnFamily) {
    super(fileName, startKey, endKey, columnFamily);
    this.snapshotGeneration = snapshotGeneration;
    this.flushTime = flushTime;
  }

  public long getSnapshotGeneration() {
    return snapshotGeneration;
  }

  public long getFlushTime() {
    return flushTime;
  }

  @Override
  public String toString() {
    return String.format("FlushNode{file=%s, generation=%d, time=%d, cf=%s}",
        getFileName(), snapshotGeneration, flushTime, getColumnFamily());
  }

  // Use identity-based equality like CompactionNode
  @Override
  public final boolean equals(Object o) {
    return this == o;
  }

  // Hash based on file name, consistent with CompactionNode
  @Override
  public int hashCode() {
    return Objects.hash(getFileName());
  }
}
