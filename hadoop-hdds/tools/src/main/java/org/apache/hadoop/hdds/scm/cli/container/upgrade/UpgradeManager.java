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
package org.apache.hadoop.hdds.scm.cli.container.upgrade;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;


import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * This class manages v2 to v3 container upgrade.
 */
public class UpgradeManager {

  public static final Logger LOG =
      LoggerFactory.getLogger(UpgradeManager.class);

  private final Map<String, DatanodeStoreSchemaThreeImpl>
      volumeStoreMap = new ConcurrentHashMap<>();

  public List<Result> run(OzoneConfiguration configuration,
      List<HddsVolume> volumes) throws IOException {
    List<Result> results = new ArrayList<>();
    Map<HddsVolume, CompletableFuture<Result>> volumeFutures = new HashMap<>();
    long startTime = System.currentTimeMillis();

    LOG.info("Start to upgrade {} volume(s)", volumes.size());
    for (StorageVolume volume : volumes) {
      final HddsVolume hddsVolume = (HddsVolume) volume;
      final UpgradeTask task =
          new UpgradeTask(configuration, hddsVolume, volumeStoreMap);
      final CompletableFuture<Result> future = task.getUpgradeFuture();
      volumeFutures.put(hddsVolume, future);
    }

    for (Map.Entry<HddsVolume, CompletableFuture<Result>> entry :
        volumeFutures.entrySet()) {
      final HddsVolume hddsVolume = entry.getKey();
      final CompletableFuture<Result> volumeFuture = entry.getValue();

      try {
        final Result result = volumeFuture.get();
        results.add(result);
        LOG.info("Finish upgrading containers on volume {}, {}",
            hddsVolume.getVolumeRootDir(), result.toString());
      } catch (Exception e) {
        LOG.error("Failed to upgrade containers on volume {}",
            hddsVolume.getVolumeRootDir(), e);
      }
    }

    LOG.info("It took {}ms to finish all volume upgrade.",
        (System.currentTimeMillis() - startTime));
    return results;
  }

  @VisibleForTesting
  public DatanodeStoreSchemaThreeImpl getDBStore(HddsVolume volume) {
    return volumeStoreMap.get(volume.getStorageDir().getAbsolutePath());
  }

  /**
   * This class contains v2 to v3 container upgrade result.
   */
  public static class Result {
    private Map<Long, UpgradeTask.UpgradeContainerResult> resultMap;
    private final HddsVolume hddsVolume;
    private final long startTimeMs = System.currentTimeMillis();
    private long endTimeMs = 0L;
    private Exception e = null;
    private Status status = Status.FAIL;

    public Result(HddsVolume hddsVolume) {
      this.hddsVolume = hddsVolume;
    }

    public HddsVolume getHddsVolume() {
      return hddsVolume;
    }

    public long getCost() {
      return endTimeMs - startTimeMs;
    }

    public void setResultList(
        List<UpgradeTask.UpgradeContainerResult> resultList) {
      resultMap = new HashMap<>();
      resultList.forEach(res -> resultMap
          .put(res.getOriginContainerData().getContainerID(), res));
    }

    public Map<Long, UpgradeTask.UpgradeContainerResult> getResultMap() {
      return resultMap;
    }

    public boolean isSuccess() {
      return this.status == Status.SUCCESS;
    }

    public void success() {
      this.endTimeMs = System.currentTimeMillis();
      this.status = Status.SUCCESS;
    }

    public void fail(Exception exception) {
      this.endTimeMs = System.currentTimeMillis();
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
          stringBuilder.append("\n");
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

}
