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

package org.apache.hadoop.ozone.container.common.volume;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.ratis.util.function.CheckedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * VolumeSet to manage volumes in a DataNode.
 */
public class MutableVolumeSet implements VolumeSet {

  private static final Logger LOG =
      LoggerFactory.getLogger(MutableVolumeSet.class);

  private ConfigurationSource conf;

  /**
   * Maintains a map of all active volumes in the DataNode.
   * Each volume has one-to-one mapping with a volumeInfo object.
   */
  private Map<String, StorageVolume> volumeMap;
  /**
   * Maintains a map of volumes which have failed. The keys in this map and
   * {@link #volumeMap} are mutually exclusive.
   */
  private Map<String, StorageVolume> failedVolumeMap;

  /**
   * A Reentrant Read Write Lock to synchronize volume operations in VolumeSet.
   * Any update to {@link #volumeMap} or {@link #failedVolumeMap} should be done after acquiring the write lock.
   */
  private final ReentrantReadWriteLock volumeSetRWLock;

  private final String datanodeUuid;

  private final StorageVolumeChecker volumeChecker;
  private CheckedRunnable<IOException> failedVolumeListener;
  private StateContext context;
  private final StorageVolumeFactory volumeFactory;
  private final StorageVolume.VolumeType volumeType;
  private int maxVolumeFailuresTolerated;
  private final VolumeHealthMetrics volumeHealthMetrics;

  public MutableVolumeSet(String dnUuid, ConfigurationSource conf,
      StateContext context, StorageVolume.VolumeType volumeType,
      StorageVolumeChecker volumeChecker) throws IOException {
    this(dnUuid, null, conf, context, volumeType, volumeChecker);
  }

  public MutableVolumeSet(String dnUuid, String clusterID,
      ConfigurationSource conf, StateContext context,
      StorageVolume.VolumeType volumeType, StorageVolumeChecker volumeChecker
  ) throws IOException {
    this.context = context;
    this.datanodeUuid = dnUuid;
    this.conf = conf;
    this.volumeSetRWLock = new ReentrantReadWriteLock();
    this.volumeChecker = volumeChecker;
    if (this.volumeChecker != null) {
      this.volumeChecker.registerVolumeSet(this);
    }
    this.volumeType = volumeType;

    SpaceUsageCheckFactory usageCheckFactory =
        SpaceUsageCheckFactory.create(conf);
    DatanodeConfiguration dnConf =
        conf.getObject(DatanodeConfiguration.class);
    if (volumeType == StorageVolume.VolumeType.META_VOLUME) {
      this.volumeFactory = new MetadataVolumeFactory(conf, usageCheckFactory,
          this);
      maxVolumeFailuresTolerated = dnConf.getFailedMetadataVolumesTolerated();
    } else if (volumeType == StorageVolume.VolumeType.DB_VOLUME) {
      this.volumeFactory = new DbVolumeFactory(conf, usageCheckFactory,
          this, datanodeUuid, clusterID);
      maxVolumeFailuresTolerated = dnConf.getFailedDbVolumesTolerated();
    } else {
      this.volumeFactory = new HddsVolumeFactory(conf, usageCheckFactory,
          this, datanodeUuid, clusterID);
      maxVolumeFailuresTolerated = dnConf.getFailedDataVolumesTolerated();
    }

    // Ensure metrics are unregistered if the volume set initialization fails.
    this.volumeHealthMetrics = VolumeHealthMetrics.create(volumeType);
    try {
      initializeVolumeSet();
    } catch (Exception e) {
      volumeHealthMetrics.unregister();
      throw e;
    }
  }

  public void setFailedVolumeListener(CheckedRunnable<IOException> runnable) {
    failedVolumeListener = runnable;
  }

  @VisibleForTesting
  public StorageVolumeChecker getVolumeChecker() {
    return volumeChecker;
  }

  /**
   * Add DN volumes configured through ConfigKeys to volumeMap.
   */
  private void initializeVolumeSet() throws IOException {
    volumeMap = new ConcurrentHashMap<>();
    failedVolumeMap = new ConcurrentHashMap<>();

    Collection<String> rawLocations;
    if (volumeType == StorageVolume.VolumeType.META_VOLUME) {
      rawLocations = HddsServerUtil.getOzoneDatanodeRatisDirectory(conf);
    } else if (volumeType == StorageVolume.VolumeType.DB_VOLUME) {
      rawLocations = HddsServerUtil.getDatanodeDbDirs(conf);
    } else {
      rawLocations = HddsServerUtil.getDatanodeStorageDirs(conf);
    }

    for (String locationString : rawLocations) {
      StorageVolume volume = null;
      try {
        StorageLocation location = StorageLocation.parse(locationString);

        volume = volumeFactory.createVolume(
            location.getUri().getPath(), location.getStorageType());

        LOG.info("Added Volume : {} to VolumeSet",
            volume.getStorageDir().getPath());

        if (!volume.getStorageDir().mkdirs() &&
            !volume.getStorageDir().exists()) {
          throw new IOException("Failed to create storage dir " +
              volume.getStorageDir());
        }
        volumeMap.put(volume.getStorageDir().getPath(), volume);
        volumeHealthMetrics.incrementHealthyVolumes();
      } catch (IOException e) {
        volumeHealthMetrics.incrementFailedVolumes();
        if (volume != null) {
          volume.shutdown();
        }

        volume = volumeFactory.createFailedVolume(locationString);
        failedVolumeMap.put(locationString, volume);
        LOG.error("Failed to parse the storage location: " + locationString, e);
      }
    }

    // First checking if we have any volumes, if all volumes are failed the
    // volumeMap size will be zero, and we throw Exception.
    if (volumeMap.isEmpty()) {
      throw new DiskOutOfSpaceException("No storage locations configured");
    }
  }

  /**
   * Run a synchronous parallel check of all volumes, removing
   * failed volumes.
   */
  public void checkAllVolumes() throws IOException {
    checkAllVolumes(volumeChecker);
  }

  @Override
  public void checkAllVolumes(StorageVolumeChecker checker)
      throws IOException {
    if (checker == null) {
      LOG.debug("No volumeChecker, skip checkAllVolumes");
      return;
    }

    List<StorageVolume> allVolumes = getVolumesList();
    Set<? extends StorageVolume> failedVolumes;
    try {
      failedVolumes = checker.checkAllVolumes(allVolumes);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while running disk check", e);
    }

    if (!failedVolumes.isEmpty()) {
      LOG.warn("checkAllVolumes got {} failed volumes - {}",
          failedVolumes.size(), failedVolumes);
      handleVolumeFailures(failedVolumes);
    } else {
      LOG.debug("checkAllVolumes encountered no failures");
    }
  }

  /**
   * Handle one or more failed volumes.
   * @param failedVolumes
   */
  private void handleVolumeFailures(
      Set<? extends StorageVolume> failedVolumes) throws IOException {
    this.writeLock();
    try {
      for (StorageVolume v : failedVolumes) {
        // Immediately mark the volume as failed so it is unavailable
        // for new containers.
        failVolume(v.getStorageDir().getPath());
      }

      // check failed volume tolerated
      if (!hasEnoughVolumes()) {
        context.getParent().handleFatalVolumeFailures();
      }
    } finally {
      this.writeUnlock();
    }

    if (failedVolumeListener != null) {
      failedVolumeListener.run();
    }
    // TODO:
    // 1. Consider stopping IO on open containers and tearing down
    //    active pipelines.
  }

  public void checkVolumeAsync(StorageVolume volume) {
    if (volumeChecker == null) {
      LOG.debug("No volumeChecker, skip checkVolumeAsync");
      return;
    }

    volumeChecker.checkVolume(
        volume, (healthyVolumes, failedVolumes) -> {
          if (!failedVolumes.isEmpty()) {
            LOG.warn("checkVolumeAsync callback got {} failed volumes: {}",
                failedVolumes.size(), failedVolumes);
          } else {
            LOG.debug("checkVolumeAsync: no volume failures detected");
          }
          handleVolumeFailures(failedVolumes);
        });
  }

  public void startAllVolume() throws IOException {
    for (Map.Entry<String, StorageVolume> entry : volumeMap.entrySet()) {
      entry.getValue().start();
    }
  }

  public void refreshAllVolumeUsage() {
    volumeMap.forEach((k, v) -> v.refreshVolumeUsage());
  }

  public void setGatherContainerUsages(Function<HddsVolume, Long> gatherContainerUsages) {
    volumeMap.forEach((k, v) -> v.setGatherContainerUsages(gatherContainerUsages));
  }

  /**
   * Acquire Volume Set Read lock.
   */
  @Override
  public void readLock() {
    volumeSetRWLock.readLock().lock();
  }

  /**
   * Release Volume Set Read lock.
   */
  @Override
  public void readUnlock() {
    volumeSetRWLock.readLock().unlock();
  }

  /**
   * Acquire Volume Set Write lock.
   */
  @Override
  public void writeLock() {
    volumeSetRWLock.writeLock().lock();
  }

  /**
   * Release Volume Set Write lock.
   */
  @Override
  public void writeUnlock() {
    volumeSetRWLock.writeLock().unlock();
  }

  // Mark a volume as failed
  public void failVolume(String volumeRoot) {
    this.writeLock();
    try {
      if (volumeMap.containsKey(volumeRoot)) {
        StorageVolume volume = volumeMap.get(volumeRoot);
        volume.failVolume();

        volumeMap.remove(volumeRoot);
        failedVolumeMap.put(volumeRoot, volume);
        volumeHealthMetrics.decrementHealthyVolumes();
        volumeHealthMetrics.incrementFailedVolumes();
        LOG.info("Moving Volume : {} to failed Volumes", volumeRoot);
      } else if (failedVolumeMap.containsKey(volumeRoot)) {
        LOG.info("Volume : {} is not active", volumeRoot);
      } else {
        LOG.warn("Volume : {} does not exist in VolumeSet", volumeRoot);
      }
    } finally {
      this.writeUnlock();
    }
  }

  /**
   * Shutdown the volumeset.
   */
  public void shutdown() {
    for (StorageVolume volume : volumeMap.values()) {
      try {
        volume.shutdown();
      } catch (Exception ex) {
        LOG.error("Failed to shutdown volume : " + volume.getStorageDir(), ex);
      }
    }
    volumeMap.clear();
    
    if (volumeHealthMetrics != null) {
      volumeHealthMetrics.unregister();
    }
  }

  @Override
  public List<StorageVolume> getVolumesList() {
    return ImmutableList.copyOf(volumeMap.values());
  }

  @VisibleForTesting
  public List<StorageVolume> getFailedVolumesList() {
    return ImmutableList.copyOf(failedVolumeMap.values());
  }

  @VisibleForTesting
  public Map<String, StorageVolume> getVolumeMap() {
    return ImmutableMap.copyOf(volumeMap);
  }

  @VisibleForTesting
  public void setVolumeMap(Map<String, StorageVolume> map) {
    this.volumeMap = map;
  }

  public boolean hasEnoughVolumes() {
    // Max number of bad volumes allowed, should have at least
    // 1 good volume
    boolean hasEnoughVolumes;
    if (maxVolumeFailuresTolerated == StorageVolumeChecker.MAX_VOLUME_FAILURE_TOLERATED_LIMIT) {
      hasEnoughVolumes = !volumeMap.isEmpty();
    } else {
      hasEnoughVolumes = failedVolumeMap.size() <= maxVolumeFailuresTolerated;
    }
    if (!hasEnoughVolumes) {
      LOG.error("Not enough volumes in MutableVolumeSet. DatanodeUUID: {}, VolumeType: {}, " +
              "MaxVolumeFailuresTolerated: {}, ActiveVolumes: {}, FailedVolumes: {}",
          datanodeUuid, volumeType, maxVolumeFailuresTolerated,
          volumeMap.size(), failedVolumeMap.size());
    }
    return hasEnoughVolumes;
  }

  public StorageLocationReport[] getStorageReport() {
    this.readLock();
    try {
      StorageLocationReport[] reports = new StorageLocationReport[volumeMap.size() + failedVolumeMap.size()];
      int counter = 0;
      for (StorageVolume volume : volumeMap.values()) {
        reports[counter++] = volume.getReport();
      }
      for (StorageVolume volume : failedVolumeMap.values()) {
        reports[counter++] = volume.getReport();
      }
      return reports;
    } finally {
      this.readUnlock();
    }
  }

  public StorageVolume.VolumeType getVolumeType() {
    return volumeType;
  }

  @VisibleForTesting
  public VolumeHealthMetrics getVolumeHealthMetrics() {
    return volumeHealthMetrics;
  }
}
