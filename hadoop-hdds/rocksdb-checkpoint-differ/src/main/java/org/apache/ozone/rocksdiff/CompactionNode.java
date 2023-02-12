/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ozone.rocksdiff;

/**
 * Node in the compaction DAG that represents an SST file.
 */
public class CompactionNode {
  // Name of the SST file
  private final String fileName;
  // The last snapshot created before this node came into existence
  private final String snapshotId;
  private final long snapshotGeneration;
  private final long totalNumberOfKeys;
  private long cumulativeKeysReverseTraversal;

  /**
   * CompactionNode constructor.
   * @param file SST file (filename without extension)
   * @param ssId snapshotId field. Added here for improved debuggability only
   * @param numKeys Number of keys in the SST
   * @param seqNum Snapshot generation (sequence number)
   */
  public CompactionNode(String file, String ssId, long numKeys, long seqNum) {
    fileName = file;
    snapshotId = ssId;
    totalNumberOfKeys = numKeys;
    snapshotGeneration = seqNum;
    cumulativeKeysReverseTraversal = 0L;
  }

  @Override
  public String toString() {
    return String.format("Node{%s}", fileName);
  }

  public String getFileName() {
    return fileName;
  }

  public String getSnapshotId() {
    return snapshotId;
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
}
