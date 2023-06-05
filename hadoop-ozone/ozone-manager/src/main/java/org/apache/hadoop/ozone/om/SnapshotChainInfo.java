/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import java.util.Objects;

/**
 * SnapshotChain supporting SnapshotInfo class.
 *
 * Defines and provides the interface to the SnapshotChainInfo
 * entry that comprises a SnapshotChain.
 * Getters / setters for current, prev, next element in snapshot chain.
 */
public class SnapshotChainInfo {
  private String snapshotID;
  private String previousSnapshotID;
  private String nextSnapshotID;

  public SnapshotChainInfo(String snapshotID, String prev, String next) {
    this.snapshotID = snapshotID;
    previousSnapshotID = prev;
    nextSnapshotID = next;
  }

  public void setNextSnapshotID(String snapsID) {
    nextSnapshotID = snapsID;
  }

  public void setPreviousSnapshotID(String snapsID) {
    previousSnapshotID = snapsID;
  }

  public String getSnapshotID() {
    return snapshotID;
  }

  public String getNextSnapshotID() {
    return nextSnapshotID;
  }

  public String getPreviousSnapshotID() {
    return previousSnapshotID;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SnapshotChainInfo that = (SnapshotChainInfo) o;
    return Objects.equals(snapshotID, that.snapshotID) &&
        Objects.equals(previousSnapshotID, that.previousSnapshotID) &&
        Objects.equals(nextSnapshotID, that.nextSnapshotID);
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshotID, previousSnapshotID, nextSnapshotID);
  }
}
