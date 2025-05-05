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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.apache.hadoop.util.Time;

/**
 * This class contains v2 to v3 container upgrade result.
 */
class VolumeUpgradeResult {
  private Map<Long, ContainerUpgradeResult> resultMap;
  private final HddsVolume hddsVolume;
  private final long startTimeMs = Time.monotonicNow();
  private long endTimeMs = 0L;
  private Exception e = null;
  private Status status = Status.FAIL;
  private DatanodeStoreSchemaThreeImpl store;

  VolumeUpgradeResult(HddsVolume hddsVolume) {
    this.hddsVolume = hddsVolume;
  }

  public HddsVolume getHddsVolume() {
    return hddsVolume;
  }

  public long getCost() {
    return endTimeMs - startTimeMs;
  }

  DatanodeStoreSchemaThreeImpl getStore() {
    return store;
  }

  void setStore(DatanodeStoreSchemaThreeImpl store) {
    this.store = store;
  }

  public void setResultList(
      List<ContainerUpgradeResult> resultList) {
    resultMap = new HashMap<>();
    resultList.forEach(res -> resultMap
        .put(res.getOriginContainerData().getContainerID(), res));
  }

  public Map<Long, ContainerUpgradeResult> getResultMap() {
    return resultMap;
  }

  public boolean isSuccess() {
    return this.status == Status.SUCCESS;
  }

  public void success() {
    this.endTimeMs = Time.monotonicNow();
    this.status = Status.SUCCESS;
  }

  public void fail(Exception exception) {
    this.endTimeMs = Time.monotonicNow();
    this.status = Status.FAIL;
    this.e = exception;
  }

  @Override
  public String toString() {
    final StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("Result:{");
    stringBuilder.append("hddsRootDir=");
    stringBuilder.append(getHddsVolume().getHddsRootDir());
    stringBuilder.append(", resultList=");
    AtomicLong total = new AtomicLong(0L);
    if (resultMap != null) {
      resultMap.forEach((k, r) -> {
        stringBuilder.append(r.toString());
        stringBuilder.append('\n');
        total.addAndGet(r.getTotalRow());
      });
    }
    stringBuilder.append(", totalRow=");
    stringBuilder.append(total.get());
    stringBuilder.append(", costMs=");
    stringBuilder.append(getCost());
    stringBuilder.append(", status=");
    stringBuilder.append(status);
    if (e != null) {
      stringBuilder.append(", Exception=");
      stringBuilder.append(e);
    }
    stringBuilder.append('}');
    return stringBuilder.toString();
  }

  enum Status {
    SUCCESS,
    FAIL
  }
}
