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

package org.apache.hadoop.ozone.jobworker.volume;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerStorageReportProto;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.jobworker.JobworkerConfiguration;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Volatile VolumeSet, no data will be retained after reboot.
 */
public class VolatileJobworkerVolumeSet implements JobworkerVolumeSet {

  private static final Logger LOG = LoggerFactory.getLogger(VolatileJobworkerVolumeSet.class);
  private final ConfigurationSource conf;
  private final JobworkerConfiguration jwConf;

  /**
   * A Reentrant Read-Write Lock to synchronize volume operations in VolumeSet.
   * Any update to {@link #volumeMap} or {@link #failedVolumeMap} should be
   * done after acquiring the write lock.
   */
  private final ReentrantReadWriteLock volumeSetRWLock;
  private final String jobWorkerUuid;
  private final AtomicReference<InitializingStatus> initializingStatus;
  /**
   * Maintains a map of all active volumes in the JobWorker.
   */
  private Map<String, VolatileJobworkerVolume> volumeMap;
  /**
   * Maintains a map of volumes which have failed. The keys in this map and
   * {@link #volumeMap} are mutually exclusive.
   */
  private Map<String, VolatileJobworkerVolume> failedVolumeMap;
  private String clusterID;
  private JobworkerStateContext context;

  /**
   * Constructor for JobWorkerVolumeSet.
   *
   * @param jobWorkerUuid UUID of the job worker
   * @param conf          Configuration
   * @param context       State context
   */
  public VolatileJobworkerVolumeSet(
      String jobWorkerUuid,
      ConfigurationSource conf,
      JobworkerStateContext context) {
    this.jobWorkerUuid = jobWorkerUuid;
    this.conf = conf;
    this.context = context;
    this.volumeSetRWLock = new ReentrantReadWriteLock();
    jwConf = conf.getObject(JobworkerConfiguration.class);
    initializingStatus =
        new AtomicReference<>(InitializingStatus.UNINITIALIZED);
    volumeMap = new ConcurrentHashMap<>();
    failedVolumeMap = new ConcurrentHashMap<>();
  }

  /**
   * Set the state context for this volume set.
   *
   * @param context The state context
   */
  public void setContext(JobworkerStateContext context) {
    this.context = context;
  }

  /**
   * Initialize a volume set by creating and adding volumes from configured paths.
   *
   * @throws IOException if initialization fails
   */
  @Override
  public void initializeVolumeSet(String currentClusterId) throws IOException {
    // If OM HA is enabled, this will be called multi-times
    // from VersionEndpointTask. The first call should do the initializing job,
    // the successive calls should wait until VolumeSet is initialized.
    if (!initializingStatus.compareAndSet(
        InitializingStatus.UNINITIALIZED, InitializingStatus.INITIALIZING)) {
      // wait OzoneContainer to finish its initializing.
      while (initializingStatus.get() != InitializingStatus.INITIALIZED) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      LOG.info("Ignore. Jobworker Volume has been created.");
      return;
    }
    this.clusterID = currentClusterId;

    Collection<String> rawLocations = getConfiguredVolumePaths();

    this.writeLock();
    try {
      for (String locationString : rawLocations) {
        VolatileJobworkerVolume volume = null;
        try {
          StorageLocation location = StorageLocation.parse(locationString);
          volume = createVolume(location.getUri().getPath());
          LOG.info("Added Volume : {} to VolumeSet", volume.getJobworkerDir().getPath());
          if (!volume.getJobworkerDir().exists() || !volume.getJobworkerDir().isDirectory()) {
            throw new IOException("Failed to create storage dir " + volume.getJobworkerDir());
          }
          volumeMap.put(volume.getJobworkerDir().getPath(), volume);
        } catch (IOException e) {
          if (volume != null) {
            volume.shutdown();
          }

          volume = createFailedVolume(locationString);
          failedVolumeMap.put(locationString, volume);
          LOG.error("Failed to parse the storage location: {}", locationString, e);
        }
      }
    } finally {
      this.writeUnlock();
    }
    initializingStatus.set(InitializingStatus.INITIALIZED);
  }

  /**
   * Get storage directory paths from configuration.
   *
   * @return Collection of volume path strings
   */
  private Collection<String> getConfiguredVolumePaths() {
    String rawLocationStr = jwConf.getStorageDirs();
    if (rawLocationStr == null || rawLocationStr.trim().isEmpty()) {
      return Collections.emptyList();
    }
    Collection<String> rawLocations = Arrays.asList(rawLocationStr.trim().split("\\s*[,\n]\\s*"));
    LOG.info("Configured Volume path {}", String.join(", ", rawLocations));
    return rawLocations;
  }

  /**
   * Create a normal volume from a path.
   *
   * @param volumeRoot Root path for the volume
   * @return Created JobWorkerVolume
   * @throws IOException if volume creation fails
   */
  private VolatileJobworkerVolume createVolume(String volumeRoot) throws IOException {
    VolatileJobworkerVolume.Builder builder = new VolatileJobworkerVolume.Builder(volumeRoot)
        .conf(conf)
        .jobWorkerUuid(jobWorkerUuid)
        .clusterID(clusterID);

    return builder.build();
  }

  /**
   * Create a failed volume from a path.
   *
   * @param volumeRoot Root path for the volume
   * @return Created JobWorkerVolume marked as failed
   * @throws IOException if volume creation fails
   */
  private VolatileJobworkerVolume createFailedVolume(String volumeRoot) throws IOException {
    VolatileJobworkerVolume.Builder builder = new VolatileJobworkerVolume.Builder(volumeRoot)
        .clusterID(clusterID)
        .jobWorkerUuid(jobWorkerUuid)
        .failedVolume(true);

    return builder.build();
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

  /**
   * Remove a volume from the set completely.
   *
   * @param volumeRoot Path to the root of the volume to remove
   * @throws IOException if removal fails
   */
  public void removeVolume(String volumeRoot) throws IOException {
    this.writeLock();
    try {
      if (volumeMap.containsKey(volumeRoot)) {
        VolatileJobworkerVolume volume = volumeMap.get(volumeRoot);
        volume.shutdown();

        volumeMap.remove(volumeRoot);
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
   * Shutdown the volume set and all volumes it contains.
   */
  @Override
  public void close() {
    for (VolatileJobworkerVolume volume : volumeMap.values()) {
      try {
        volume.shutdown();
      } catch (Exception ex) {
        LOG.error("Failed to shutdown volume : " + volume.getJobworkerDir(), ex);
      }
    }
    volumeMap.clear();
  }

  /**
   * Get a list of all healthy volumes.
   *
   * @return List of volumes
   */
  @Override
  public List<VolatileJobworkerVolume> getVolumesList() {
    return ImmutableList.copyOf(volumeMap.values());
  }

  /**
   * Get storage reports for all active volumes.
   *
   * @return Array of StorageLocationReportProto
   */
  @Override
  public List<JobworkerStorageReportProto> getStorageReport() {
    this.readLock();
    try {
      List<JobworkerStorageReportProto> reports =
          new ArrayList<>(volumeMap.size() + failedVolumeMap.size());
      // Add reports for normal volumes
      for (VolatileJobworkerVolume volume : volumeMap.values()) {
        JobworkerStorageReportProto.Builder builder =
            JobworkerStorageReportProto.newBuilder();

        builder.setStorageLocation(volume.getVolumeRootDir().getAbsolutePath())
            .setStorageUuid(volume.getStorageID())
            .setFailed(false)
            .setCapacity(volume.getCapacity())
            .setRemaining(volume.getAvailable())
            .setStorageLocation(volume.getVolumeRootDir().getAbsolutePath());
        reports.add(builder.build());
      }
      // Add reports for failed volumes
      for (VolatileJobworkerVolume volume : failedVolumeMap.values()) {
        JobworkerStorageReportProto.Builder builder =
            JobworkerStorageReportProto.newBuilder();

        builder.setStorageLocation(volume.getVolumeRootDir().getAbsolutePath())
            .setStorageUuid(volume.getStorageID())
            .setFailed(true)
            .setCapacity(0)
            .setRemaining(0)
            .setStorageLocation(volume.getVolumeRootDir().getAbsolutePath());
        reports.add(builder.build());
      }

      return reports;
    } finally {
      this.readUnlock();
    }
  }

  /**
   * Choose a volume for a new task based on available space.
   *
   * @return The chosen JobWorkerVolume or null if none available
   */
  @Override
  public VolatileJobworkerVolume chooseVolume() {
    VolatileJobworkerVolume selectedVolume = null;
    long maxAvailable = 0;

    this.readLock();
    try {
      for (VolatileJobworkerVolume volume : volumeMap.values()) {
        if (!volume.isFailed() && volume.getState() == VolatileJobworkerVolume.VolumeState.NORMAL) {
          long available = volume.getAvailable();
          if (available > maxAvailable) {
            maxAvailable = available;
            selectedVolume = volume;
          }
        }
      }
    } finally {
      this.readUnlock();
    }

    return selectedVolume;
  }

  /**
   * Create a new task directory on the a volume.
   *
   * @param taskId The task ID
   * @return The task directory
   * @throws IOException if there's an error creating the directory
   */
  @Override
  public File createTaskDirectory(String taskId) throws IOException {
    VolatileJobworkerVolume volume = chooseVolume();
    if (volume == null) {
      throw new IOException("No viable volumes available for task: " + taskId);
    }

    return volume.createTaskDirectory(taskId);
  }

  /**
   * Clean up all directories associated with a task across all volumes.
   *
   * @param taskId The task ID to clean up
   * @return true if cleanup was successful on all volumes
   */
  @Override
  public boolean cleanupTask(String taskId) {
    boolean success = true;

    this.readLock();
    try {
      for (VolatileJobworkerVolume volume : volumeMap.values()) {
        success = volume.cleanupTaskDirectory(taskId) && success;
      }

      // Also check failed volumes in case task directories exist there
      for (VolatileJobworkerVolume volume : failedVolumeMap.values()) {
        success = volume.cleanupTaskDirectory(taskId) && success;
      }
    } finally {
      this.readUnlock();
    }

    return success;
  }

  enum InitializingStatus {
    UNINITIALIZED, INITIALIZING, INITIALIZED
  }
}
