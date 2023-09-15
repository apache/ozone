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
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;

import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hdds.scm.cli.container.upgrade.UpgradeUtils.getContainerDBPath;

/**
 * This class manage upgrade v2 to v3 container.
 */
public class UpgradeManager {

  public static final Logger LOG =
      LoggerFactory.getLogger(UpgradeManager.class);

  private final Map<String, DatanodeStoreSchemaThreeImpl>
      volumeStoreMap = new HashMap<>();

  public void run(OzoneConfiguration configuration) throws IOException {
    final DatanodeDetails detail =
        UpgradeUtils.getDatanodeDetails(configuration);

    final MutableVolumeSet dataVolumeSet = UpgradeUtils
        .getHddsVolumes(configuration, StorageVolume.VolumeType.DATA_VOLUME,
            detail.getUuidString());

    for (StorageVolume storageVolume : dataVolumeSet.getVolumesList()) {
      final HddsVolume volume = (HddsVolume) storageVolume;
      volume.loadDbStore(false);
    }
    initVolumeStoreMap(dataVolumeSet, configuration);
    upgradeAll(dataVolumeSet, configuration);
  }

  public void initVolumeStoreMap(MutableVolumeSet dataVolumeSet,
                                 OzoneConfiguration configuration)
      throws IOException {
    for (StorageVolume storageVolume : dataVolumeSet.getVolumesList()) {
      final HddsVolume volume = (HddsVolume) storageVolume;
      final File containerDBPath = getContainerDBPath(volume);
      final DatanodeStoreSchemaThreeImpl datanodeStoreSchemaThree =
          new DatanodeStoreSchemaThreeImpl(configuration,
              containerDBPath.getAbsolutePath(), false);
      volumeStoreMap.put(volume.getStorageDir().getAbsolutePath(),
          datanodeStoreSchemaThree);
    }
  }

  public List<Result> upgradeAll(MutableVolumeSet volumeSet,
                                 OzoneConfiguration configuration) {
    List<Result> results = new ArrayList<>();
    final List<StorageVolume> volumesList = volumeSet.getVolumesList();
    List<CompletableFuture<Result>> volumeFutures =
        new ArrayList<>(volumesList.size());
    long startTime = System.currentTimeMillis();

    LOG.info("Start upgrade {} volumes container LayoutVersion",
        volumesList.size());

    for (StorageVolume volume : volumesList) {
      final HddsVolume hddsVolume = (HddsVolume) volume;
      final UpgradeTask upgradeVolume =
          new UpgradeTask(configuration, hddsVolume,
              volumeStoreMap.get(hddsVolume.getStorageDir()
                  .getAbsolutePath()));

      final CompletableFuture<Result> future =
          upgradeVolume.getUpgradeFutureByVolume();
      volumeFutures.add(future);
    }

    for (CompletableFuture<Result> volumeFuture : volumeFutures) {
      final Result result = volumeFuture.join();
      results.add(result);
      LOG.info("Finish upgrade containers on volume {}, result {}",
          result.getHddsVolumeRootDir(), result);
    }

    LOG.info("Upgrade all volume container LayoutVersion costs {}s",
        (System.currentTimeMillis() - startTime) / 1000);
    return results;
  }

  @VisibleForTesting
  public DatanodeStoreSchemaThreeImpl getDBStore(HddsVolume volume) {
    return volumeStoreMap.get(volume.getStorageDir().getAbsolutePath());
  }

  /**
   * This class response upgrade v2 to v3 container result.
   */
  public static class Result {
    private Map<Long, UpgradeTask.Result> resultMap;
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

    public String getHddsVolumeRootDir() {
      return hddsVolume.getVolumeRootDir();
    }

    public long getCost() {
      return endTimeMs - startTimeMs;
    }

    public void setResultList(List<UpgradeTask.Result> resultList) {
      resultMap = new HashMap<>();
      resultList.forEach(res -> resultMap
          .put(res.getOriginContainerData().getContainerID(), res));
    }

    public Map<Long, UpgradeTask.Result> getResultMap() {
      return resultMap;
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
      stringBuilder.append("Result{");
      stringBuilder.append("volumeDir=");
      stringBuilder.append(hddsVolume.getHddsRootDir());
      stringBuilder.append(", resultList=");
      AtomicLong total = new AtomicLong(0L);
      resultMap.forEach((k, r) -> {
        stringBuilder.append(r.toString());
        stringBuilder.append("\n");
        total.addAndGet(r.getTotalRow());
      });
      stringBuilder.append(", totalRow=");
      stringBuilder.append(total.get());
      stringBuilder.append(", costMs=");
      stringBuilder.append(getCost());
      stringBuilder.append("ms, status=");
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
