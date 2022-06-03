/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.volume;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.ShutdownHookManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import static org.apache.hadoop.util.RunJar.SHUTDOWN_HOOK_PRIORITY;

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
   * Maintains a list of active volumes per StorageType.
   */
  private EnumMap<StorageType, List<StorageVolume>> volumeStateMap;

  /**
   * A Reentrant Read Write Lock to synchronize volume operations in VolumeSet.
   * Any update to {@link #volumeMap}, {@link #failedVolumeMap}, or
   * {@link #volumeStateMap} should be done after acquiring the write lock.
   */
  private final ReentrantReadWriteLock volumeSetRWLock;

  private final String datanodeUuid;
  private String clusterID;

  private Runnable shutdownHook;
  private final StorageVolumeChecker volumeChecker;
  private Runnable failedVolumeListener;
  private StateContext context;
  private final StorageVolumeFactory volumeFactory;
  private final StorageVolume.VolumeType volumeType;
  private int maxVolumeFailuresTolerated;

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
    this.clusterID = clusterID;
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

    initializeVolumeSet();
  }

  public void setFailedVolumeListener(Runnable runnable) {
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
    volumeStateMap = new EnumMap<>(StorageType.class);

    Collection<String> rawLocations;
    if (volumeType == StorageVolume.VolumeType.META_VOLUME) {
      rawLocations = HddsServerUtil.getOzoneDatanodeRatisDirectory(conf);
    } else if (volumeType == StorageVolume.VolumeType.DB_VOLUME) {
      rawLocations = HddsServerUtil.getDatanodeDbDirs(conf);
    } else {
      rawLocations = HddsServerUtil.getDatanodeStorageDirs(conf);
    }

    for (StorageType storageType : StorageType.values()) {
      volumeStateMap.put(storageType, new ArrayList<>());
    }

    for (String locationString : rawLocations) {
      try {
        StorageLocation location = StorageLocation.parse(locationString);

        StorageVolume volume = volumeFactory.createVolume(
            location.getUri().getPath(), location.getStorageType());

        LOG.info("Added Volume : {} to VolumeSet",
            volume.getStorageDir().getPath());

        if (!volume.getStorageDir().mkdirs() &&
            !volume.getStorageDir().exists()) {
          throw new IOException("Failed to create storage dir " +
              volume.getStorageDir());
        }
        volumeMap.put(volume.getStorageDir().getPath(), volume);
        volumeStateMap.get(volume.getStorageType()).add(volume);
      } catch (IOException e) {
        StorageVolume volume =
            volumeFactory.createFailedVolume(locationString);
        failedVolumeMap.put(locationString, volume);
        LOG.error("Failed to parse the storage location: " + locationString, e);
      }
    }

    // First checking if we have any volumes, if all volumes are failed the
    // volumeMap size will be zero, and we throw Exception.
    if (volumeMap.size() == 0) {
      throw new DiskOutOfSpaceException("No storage locations configured");
    }

    checkAllVolumes();

    // Ensure volume threads are stopped and scm df is saved during shutdown.
    shutdownHook = () -> {
      saveVolumeSetUsed();
    };
    ShutdownHookManager.get().addShutdownHook(shutdownHook,
        SHUTDOWN_HOOK_PRIORITY);
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

    if (failedVolumes.size() > 0) {
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
        // on startup, we could not try to stop uninitialized services
        if (shutdownHook == null) {
          throw new IOException("Don't have enough good volumes on startup,"
              + " bad volumes detected: " + failedVolumes.size()
              + " max tolerated: " + maxVolumeFailuresTolerated);
        }
        if (context != null) {
          context.getParent().handleFatalVolumeFailures();
        }
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
          if (failedVolumes.size() > 0) {
            LOG.warn("checkVolumeAsync callback got {} failed volumes: {}",
                failedVolumes.size(), failedVolumes);
          } else {
            LOG.debug("checkVolumeAsync: no volume failures detected");
          }
          handleVolumeFailures(failedVolumes);
        });
  }

  public void refreshAllVolumeUsage() {
    volumeMap.forEach((k, v) -> v.refreshVolumeInfo());
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

  // Add a volume to VolumeSet
  boolean addVolume(String dataDir) {
    return addVolume(dataDir, StorageType.DEFAULT);
  }

  // Add a volume to VolumeSet
  private boolean addVolume(String volumeRoot, StorageType storageType) {
    boolean success;

    this.writeLock();
    try {
      if (volumeMap.containsKey(volumeRoot)) {
        LOG.warn("Volume : {} already exists in VolumeMap", volumeRoot);
        success = false;
      } else {
        if (failedVolumeMap.containsKey(volumeRoot)) {
          failedVolumeMap.remove(volumeRoot);
        }

        StorageVolume volume =
            volumeFactory.createVolume(volumeRoot, storageType);
        volumeMap.put(volume.getStorageDir().getPath(), volume);
        volumeStateMap.get(volume.getStorageType()).add(volume);

        LOG.info("Added Volume : {} to VolumeSet",
            volume.getStorageDir().getPath());
        success = true;
      }
    } catch (IOException ex) {
      LOG.error("Failed to add volume " + volumeRoot + " to VolumeSet", ex);
      success = false;
    } finally {
      this.writeUnlock();
    }
    return success;
  }

  // Mark a volume as failed
  public void failVolume(String volumeRoot) {
    this.writeLock();
    try {
      if (volumeMap.containsKey(volumeRoot)) {
        StorageVolume volume = volumeMap.get(volumeRoot);
        volume.failVolume();

        volumeMap.remove(volumeRoot);
        volumeStateMap.get(volume.getStorageType()).remove(volume);
        failedVolumeMap.put(volumeRoot, volume);

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

  // Remove a volume from the VolumeSet completely.
  public void removeVolume(String volumeRoot) throws IOException {
    this.writeLock();
    try {
      if (volumeMap.containsKey(volumeRoot)) {
        StorageVolume volume = volumeMap.get(volumeRoot);
        volume.shutdown();

        volumeMap.remove(volumeRoot);
        volumeStateMap.get(volume.getStorageType()).remove(volume);

        LOG.info("Removed Volume : {} from VolumeSet", volumeRoot);
      } else if (failedVolumeMap.containsKey(volumeRoot)) {
        failedVolumeMap.remove(volumeRoot);
        LOG.info("Removed Volume : {} from failed VolumeSet", volumeRoot);
      } else {
        LOG.warn("Volume : {} does not exist in VolumeSet", volumeRoot);
      }
    } finally {
      this.writeUnlock();
    }
  }

  /**
   * This method, call shutdown on each volume to shutdown volume usage
   * thread and write scmUsed on each volume.
   */

  private synchronized void saveVolumeSetUsed() {
    for (StorageVolume hddsVolume : volumeMap.values()) {
      try {
        hddsVolume.shutdown();
      } catch (Exception ex) {
        LOG.error("Failed to shutdown volume : " + hddsVolume.getStorageDir(),
            ex);
      }
    }
    volumeMap.clear();
  }

  /**
   * Shutdown the volumeset.
   */
  public void shutdown() {
    saveVolumeSetUsed();
  }

  @Override
  @VisibleForTesting
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

  @VisibleForTesting
  public Map<StorageType, List<StorageVolume>> getVolumeStateMap() {
    return ImmutableMap.copyOf(volumeStateMap);
  }

  public boolean hasEnoughVolumes() {
    // Max number of bad volumes allowed, should have at least
    // 1 good volume
    if (maxVolumeFailuresTolerated ==
        StorageVolumeChecker.MAX_VOLUME_FAILURE_TOLERATED_LIMIT) {
      return getVolumesList().size() >= 1;
    } else {
      return getFailedVolumesList().size() <= maxVolumeFailuresTolerated;
    }
  }

  public StorageLocationReport[] getStorageReport() {
    boolean failed;
    this.readLock();
    try {
      StorageLocationReport[] reports = new StorageLocationReport[volumeMap
          .size() + failedVolumeMap.size()];
      int counter = 0;
      StorageVolume volume;
      for (Map.Entry<String, StorageVolume> entry : volumeMap.entrySet()) {
        volume = entry.getValue();
        VolumeInfo volumeInfo = volume.getVolumeInfo();
        long scmUsed;
        long remaining;
        long capacity;
        failed = false;
        try {
          scmUsed = volumeInfo.getScmUsed();
          remaining = volumeInfo.getAvailable();
          capacity = volumeInfo.getCapacity();
        } catch (UncheckedIOException ex) {
          LOG.warn("Failed to get scmUsed and remaining for container " +
              "storage location {}", volumeInfo.getRootDir(), ex);
          // reset scmUsed and remaining if df/du failed.
          scmUsed = 0;
          remaining = 0;
          capacity = 0;
          failed = true;
        }

        StorageLocationReport.Builder builder =
            StorageLocationReport.newBuilder();
        builder.setStorageLocation(volumeInfo.getRootDir())
            .setId(volume.getStorageID())
            .setFailed(failed)
            .setCapacity(capacity)
            .setRemaining(remaining)
            .setScmUsed(scmUsed)
            .setStorageType(volume.getStorageType());
        StorageLocationReport r = builder.build();
        reports[counter++] = r;
      }
      for (Map.Entry<String, StorageVolume> entry
          : failedVolumeMap.entrySet()) {
        volume = entry.getValue();
        StorageLocationReport.Builder builder = StorageLocationReport
            .newBuilder();
        builder.setStorageLocation(volume.getStorageDir()
            .getAbsolutePath()).setId(volume.getStorageID()).setFailed(true)
            .setCapacity(0).setRemaining(0).setScmUsed(0).setStorageType(
            volume.getStorageType());
        StorageLocationReport r = builder.build();
        reports[counter++] = r;
      }
      return reports;
    } finally {
      this.readUnlock();
    }
  }
}
