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

package org.apache.hadoop.ozone.repair.datanode.schemaupgrade;

import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.util.Time;

/**
 * This class represents upgrade v2 to v3 container result.
 */
class ContainerUpgradeResult {
  private final ContainerData originContainerData;
  private ContainerData newContainerData;
  private long totalRow = 0L;
  private final long startTimeMs = Time.monotonicNow();
  private long endTimeMs = 0L;
  private Status status = Status.FAIL;

  private String backupContainerFilePath;
  private String newContainerFilePath;

  ContainerUpgradeResult(ContainerData originContainerData) {
    this.originContainerData = originContainerData;
  }

  public long getTotalRow() {
    return totalRow;
  }

  public Status getStatus() {
    return status;
  }

  public void setNewContainerData(
      ContainerData newContainerData) {
    this.newContainerData = newContainerData;
  }

  ContainerData getNewContainerData() {
    return newContainerData;
  }

  public long getCostMs() {
    return endTimeMs - startTimeMs;
  }

  public ContainerData getOriginContainerData() {
    return originContainerData;
  }

  public void setBackupContainerFilePath(String backupContainerFilePath) {
    this.backupContainerFilePath = backupContainerFilePath;
  }

  String getBackupContainerFilePath() {
    return backupContainerFilePath;
  }

  public void setNewContainerFilePath(String newContainerFilePath) {
    this.newContainerFilePath = newContainerFilePath;
  }

  String getNewContainerFilePath() {
    return newContainerFilePath;
  }

  public void success(long rowCount) {
    this.totalRow = rowCount;
    this.endTimeMs = Time.monotonicNow();
    this.status = Status.SUCCESS;
  }

  @Override
  public String toString() {
    final StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("Result:{");
    stringBuilder.append("containerID=");
    stringBuilder.append(originContainerData.getContainerID());
    stringBuilder.append(", originContainerSchemaVersion=");
    stringBuilder.append(
        ((KeyValueContainerData) originContainerData).getSchemaVersion());

    if (newContainerData != null) {
      stringBuilder.append(", schemaV2ContainerFileBackupPath=");
      stringBuilder.append(backupContainerFilePath);

      stringBuilder.append(", newContainerSchemaVersion=");
      stringBuilder.append(
          ((KeyValueContainerData) newContainerData).getSchemaVersion());

      stringBuilder.append(", schemaV3ContainerFilePath=");
      stringBuilder.append(newContainerFilePath);
    }
    stringBuilder.append(", totalRow=");
    stringBuilder.append(totalRow);
    stringBuilder.append(", costMs=");
    stringBuilder.append(getCostMs());
    stringBuilder.append(", status=");
    stringBuilder.append(status);
    stringBuilder.append('}');
    return stringBuilder.toString();
  }

  enum Status {
    SUCCESS,
    FAIL
  }
}
