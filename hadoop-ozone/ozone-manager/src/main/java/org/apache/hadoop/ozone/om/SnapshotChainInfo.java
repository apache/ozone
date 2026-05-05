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

package org.apache.hadoop.ozone.om;

import java.util.Objects;
import java.util.UUID;

/**
 * SnapshotChain supporting SnapshotInfo class.
 * <p>
 * Defines and provides the interface to the SnapshotChainInfo
 * entry that comprises a SnapshotChain.
 * Getters / setters for current, prev, next element in snapshot chain.
 */
public class SnapshotChainInfo {
  private final UUID snapshotId;
  private UUID previousSnapshotId;
  private UUID nextSnapshotId;

  public SnapshotChainInfo(UUID snapshotID, UUID prev, UUID next) {
    this.snapshotId = snapshotID;
    this.previousSnapshotId = prev;
    this.nextSnapshotId = next;
  }

  public void setNextSnapshotId(UUID snapsID) {
    nextSnapshotId = snapsID;
  }

  public void setPreviousSnapshotId(UUID snapsID) {
    previousSnapshotId = snapsID;
  }

  public UUID getSnapshotId() {
    return snapshotId;
  }

  public UUID getNextSnapshotId() {
    return nextSnapshotId;
  }

  public boolean hasNextSnapshotId() {
    return Objects.nonNull(getNextSnapshotId());
  }

  public boolean hasPreviousSnapshotId() {
    return Objects.nonNull(getPreviousSnapshotId());
  }

  public UUID getPreviousSnapshotId() {
    return previousSnapshotId;
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
    return Objects.equals(snapshotId, that.snapshotId) &&
        Objects.equals(previousSnapshotId, that.previousSnapshotId) &&
        Objects.equals(nextSnapshotId, that.nextSnapshotId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshotId, previousSnapshotId, nextSnapshotId);
  }
}
