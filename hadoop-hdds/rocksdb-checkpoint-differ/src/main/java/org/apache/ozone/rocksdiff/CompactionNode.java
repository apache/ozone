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
import org.apache.ozone.compaction.log.CompactionFileInfo;
import org.apache.ozone.rocksdb.util.SstFileInfo;

/**
 * Node in the compaction DAG that represents an SST file.
 */
public class CompactionNode extends SstFileInfo {
  private final long snapshotGeneration;
  private final long totalNumberOfKeys;
  private long cumulativeKeysReverseTraversal;

  /**
   * CompactionNode constructor.
   * @param file SST file (filename without extension)
   * @param seqNum Snapshot generation (sequence number)
   */
  public CompactionNode(String file, long seqNum, String startKey, String endKey, String columnFamily) {
    super(file, startKey, endKey, columnFamily);
    totalNumberOfKeys = 0L;
    snapshotGeneration = seqNum;
    cumulativeKeysReverseTraversal = 0L;
  }

  public CompactionNode(CompactionFileInfo compactionFileInfo) {
    this(compactionFileInfo.getFileName(), -1, compactionFileInfo.getStartKey(),
        compactionFileInfo.getEndKey(), compactionFileInfo.getColumnFamily());
  }

  @Override
  public String toString() {
    return String.format("Node{%s}", getFileName());
  }

  public long getSnapshotGeneration() {
    return snapshotGeneration;
  }

  public long getTotalNumberOfKeys() {
    return totalNumberOfKeys;
  }

  public long getCumulativeKeysReverseTraversal() {
    return cumulativeKeysReverseTraversal;
  }

  public void setCumulativeKeysReverseTraversal(
      long cumulativeKeysReverseTraversal) {
    this.cumulativeKeysReverseTraversal = cumulativeKeysReverseTraversal;
  }

  public void addCumulativeKeysReverseTraversal(long diff) {
    this.cumulativeKeysReverseTraversal += diff;
  }

  // Not changing previous behaviour.
  @Override
  public final boolean equals(Object o) {
    return this == o;
  }

  // Having hashcode only on the basis of the filename.
  @Override
  public int hashCode() {
    return Objects.hash(getFileName());
  }
}
