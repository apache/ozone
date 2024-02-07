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
  private final long snapshotGeneration;
  private final long totalNumberOfKeys;
  private long cumulativeKeysReverseTraversal;
  private final String startKey;
  private final String endKey;
  private final String columnFamily;

  /**
   * CompactionNode constructor.
   * @param file SST file (filename without extension)
   * @param numKeys Number of keys in the SST
   * @param seqNum Snapshot generation (sequence number)
   */

  public CompactionNode(String file, long numKeys, long seqNum,
                        String startKey, String endKey, String columnFamily) {
    fileName = file;
    totalNumberOfKeys = numKeys;
    snapshotGeneration = seqNum;
    cumulativeKeysReverseTraversal = 0L;
    this.startKey = startKey;
    this.endKey = endKey;
    this.columnFamily = columnFamily;
  }

  @Override
  public String toString() {
    return String.format("Node{%s}", fileName);
  }

  public String getFileName() {
    return fileName;
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

  public String getStartKey() {
    return startKey;
  }

  public String getEndKey() {
    return endKey;
  }

  public String getColumnFamily() {
    return columnFamily;
  }

  public void setCumulativeKeysReverseTraversal(
      long cumulativeKeysReverseTraversal) {
    this.cumulativeKeysReverseTraversal = cumulativeKeysReverseTraversal;
  }

  public void addCumulativeKeysReverseTraversal(long diff) {
    this.cumulativeKeysReverseTraversal += diff;
  }
}
