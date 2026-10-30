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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.jobworker.JobworkerConfiguration;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobWorkerVolume represents a storage volume in a JobWorker. It manages
 * the physical storage and provides information about disk space.
 *
 * <p>Directory Structure:</p>
 * <pre>
 * ${volumeRoot}/
 *  └── jobworker/                      # The jobworker root directory
 *      └── JW-${storageID}/            # current jobworker directory
 *          ├── JOBWORKER_VERSION       # Version file with cluster ID and other metadata
 *          ├── disk-check/             # Directory for disk health checking
 *          └── CID-${clusterID}/       # The cluster directory
 *              └── current/         # The current directory
 *                  ├── ${taskID1}/  # Directory for task 1
 *                  └── ${taskID2}/  # Directory for task 2
 *                  └── ......
 * </pre>
 *
 * <p>Note: As JobWorker is a stateless service, When the volume is shutdown,
 * all created resources will be released.</p>
 */
public final class VolatileJobworkerVolume {

  public static final String JOB_WORKER_DIR_PREFIX = "JW-";
  public static final String VERSION_FILE_NAME = "JOBWORKER_VERSION";
  public static final String JOBWORKER_ROOT_DIR = "jobworker";
  public static final String DISK_CHECK_DIR_NAME = "disk-check";
  public static final String CURRENT_DIR_NAME = "current";
  private static final Logger LOG = LoggerFactory.getLogger(VolatileJobworkerVolume.class);
  private final String storageID;       // id of this volume
  private final ConfigurationSource conf;
  private final File jobworkerRootDir;
  private final File jobworkerDir;
  private final File clusterDir;
  private final File currentDir;
  private final File diskCheckDir;
  // Disk space tracking
  private final File volumeRootDir;
  private final long capacity;
  private final AtomicLong usedSpace = new AtomicLong(0);
  // Track IO test results to mark volume as failed if needed
  private volatile VolumeState state;
  private String clusterID;            // id of the cluster
  private String jobworkerUuid;        // id of the Jobworker
  private long cTime;                  // creation time of the volume
  private int layoutVersion;            // layout version of the volume

  private VolatileJobworkerVolume(Builder b) throws IOException {
    Objects.requireNonNull(b.clusterID, "ClusterID cannot be null");
    Objects.requireNonNull(b.jobWorkerUuid, "JobWorkerUUID cannot be null");
    this.clusterID = b.clusterID;
    this.state = VolumeState.NOT_FORMATTED;
    this.conf = b.conf;
    this.storageID = UUID.randomUUID().toString();
    this.volumeRootDir = new File(b.volumeRootStr);
    this.jobworkerRootDir = new File(volumeRootDir, JOBWORKER_ROOT_DIR);
    this.jobworkerDir = new File(jobworkerRootDir, JOB_WORKER_DIR_PREFIX + "-" + storageID);
    this.clusterDir = new File(jobworkerDir, OzoneConsts.CLUSTER_ID_PREFIX + clusterID);
    this.currentDir = new File(this.clusterDir, CURRENT_DIR_NAME);
    this.diskCheckDir = new File(jobworkerDir, DISK_CHECK_DIR_NAME);

    if (b.failedVolume) {
      this.state = VolumeState.FAILED;
      this.capacity = 0;
    } else {
      this.jobworkerUuid = b.jobWorkerUuid;
      this.capacity = volumeRootDir.getTotalSpace();
      // Initialize IO test parameters from configuration
      JobworkerConfiguration jwConf = conf.getObject(JobworkerConfiguration.class);
      initialize();
    }
  }

  /**
   * Initializes the Volume, creates the Version and required directories.
   * Volume will be initialized only if jobworker directory doesn't exist or is an empty directory.
   *
   * @throws IOException other exception.
   */
  private void initialize() throws IOException {
    try {
      initializeImpl();
    } catch (Exception e) {
      LOG.error("Error initializing jobworker volume {}", jobworkerDir.getPath(), e);
      shutdown();
      throw e;
    }
  }

  private void initializeImpl() throws IOException {
    if (jobworkerDir.exists()) {
      setState(VolumeState.INCONSISTENT);
      LOG.error("Jobworker directory {} already in use", jobworkerDir.getPath());
    }
    if (!jobworkerDir.mkdirs()) {
      throw new IOException("Cannot create the directory " + jobworkerDir);
    }
    createJobworkerRootDir();
    createVersionFile();
    createDiskCheckDir();
    createCurrentDirectory();
    setState(VolumeState.NORMAL);
  }

  /**
   * Create the jobworker root directory if it doesn’t exist.
   */
  private void createJobworkerRootDir() {
    if (jobworkerRootDir.exists()) {
      if (jobworkerRootDir.mkdirs()) {
        LOG.info("Create Jobworker root directory {}", jobworkerRootDir.getPath());
        return;
      }
    } else {
      LOG.info("Jobworker root directory {} already exists", jobworkerRootDir.getPath());
    }
  }

  private void createDiskCheckDir() throws IOException {
    if (!diskCheckDir.exists() && !diskCheckDir.mkdirs()) {
      throw new IOException("Unable to create disk check directory: " + diskCheckDir);
    }
    LOG.info("Create disk check directory for Volume: {}", jobworkerDir.getPath());
  }

  private void createCurrentDirectory() throws IOException {
    if (!currentDir.exists() && !currentDir.mkdirs()) {
      throw new IOException("Unable to create cluster directory: " + currentDir);
    }
    LOG.info("Created cluster directory for Volume: {}", jobworkerDir.getPath());
  }

  /**
   * Create a Version File and write property fields into it.
   */
  private void createVersionFile() throws IOException {
    this.cTime = Time.now();
    this.layoutVersion = JobworkerVolumeLayoutVersion.getLatestVersion().getVersion();

    if (this.clusterID == null || jobworkerUuid == null) {
      throw new IOException("ClusterID is not available. Cannot initialize the volume {}." +
          jobworkerDir.getPath());
    } else {
      writeVersionFile();
      setState(VolumeState.NORMAL);
    }
  }

  private void writeVersionFile() throws IOException {
    Objects.requireNonNull(this.storageID,
        "StorageID cannot be null in Version File");
    Objects.requireNonNull(this.clusterID,
        "ClusterID cannot be null in Version File");
    Objects.requireNonNull(this.jobworkerUuid,
        "JobWorkerUUID cannot be null in Version File");
    Preconditions.checkArgument(this.cTime > 0,
        "Creation Time should be positive");
    String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

    File versionFile = getVersionFile();
    LOG.debug("Writing a Version file to disk, {}", versionFile);
    JobworkerVersionFile versionFileObj = new JobworkerVersionFile(this.storageID,
        this.clusterID, this.jobworkerUuid, this.cTime, this.layoutVersion, pid);
    versionFileObj.createVersionFile(versionFile);
  }

  File getVersionFile() {
    return new File(jobworkerDir, VERSION_FILE_NAME);
  }

  /**
   * Creates a task directory for a specific task,
   * A task is only allowed to be called once this method to create task directory.
   *
   * @param taskId The task ID
   * @return The task directory
   * @throws IOException if there's an error creating the directory
   */
  public File createTaskDirectory(String taskId) throws IOException {
    checkStateNormal();

    File taskDir = new File(currentDir, taskId);
    if (!taskDir.mkdirs()) {
      throw new IOException("Unable to create the task directory: " + taskDir.getAbsolutePath());
    }

    return taskDir;
  }

  /**
   * Clean up a task's storage and temp directories.
   *
   * @param taskId The task ID to clean up
   * @return true if cleanup was successful
   */
  public boolean cleanupTaskDirectory(String taskId) {
    if (state == VolumeState.FAILED) {
      LOG.warn("Volume {} is in FAILED state, skipping cleanup for a task {}",
          jobworkerDir.getPath(), taskId);
      return false;
    }
    File taskDir = new File(currentDir, taskId);
    if (taskDir.exists()) {
      return deleteDirectory(taskDir);
    }
    return true;
  }

  /**
   * Recursively delete a directory.
   *
   * @param directory The directory to delete
   * @return true if deletion was successful
   */
  private boolean deleteDirectory(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else {
            if (!file.delete()) {
              LOG.warn("Failed to delete a file: {}", file.getAbsolutePath());
              return false;
            }
          }
        }
      }
      if (!directory.delete()) {
        LOG.warn("Failed to delete directory: {}", directory.getAbsolutePath());
        return false;
      }
    }
    return true;
  }

  /**
   * Get the current used space for this volume.
   *
   * @return The total space used in bytes
   */
  public long getSpaceUsed() {
    // TODO jobworker Currently, only the capacity space and available space of the disk where the volume
    //  is located are reported, and the actual space used by the volume is not calculated.
    //  This may cause some performance overhead.
    //  we can consider updating the actual space used by the volume in the future.
    return usedSpace.get();
  }

  /**
   * Get the available space on this volume.
   *
   * @return Available space in bytes
   */
  public long getAvailable() {
    return volumeRootDir.getUsableSpace();
  }

  /**
   * Get the total capacity of this volume.
   *
   * @return Total capacity in bytes
   */
  public long getCapacity() {
    return capacity;
  }

  /**
   * Mark the volume as failed.
   */
  public void failVolume() {
    setState(VolumeState.FAILED);
  }

  /**
   * Clean up and shutdown the volume.
   */
  public void shutdown() {
    if (!releaseJobworkerDirectories()) {
      LOG.warn("Jobworker volume {} resource cleanup failed. There may be residual data. Please check",
          jobworkerDir.getPath());
    }
    setState(VolumeState.NOT_FORMATTED);
  }

  /**
   * Release all directories created by the jobworker runtime,
   * including the task directory, but excluding the volume root.
   * For the task directory it should be clean by the Task, cleanup should
   * be completed before shutdown, this is just to prevent resource leaks,
   * so we will also try to clean up the task directory
   * The volume root directory comes from the configuration and may
   * be used by other applications, so it will not be cleaned up.
   *
   * @return true if cleanup was successful
   */
  private boolean releaseJobworkerDirectories() {
    LOG.info("Releasing volume directories for: {}", jobworkerDir.getPath());

    if (jobworkerDir == null || !jobworkerDir.exists()) {
      return true;
    }
    if (!jobworkerDir.isDirectory()) {
      LOG.warn("Storage path exists but is not a directory: {}", jobworkerDir.getPath());
      return false;
    }

    boolean diskCheckDirSuccess = releaseDiskCheckDir();
    LOG.info("Releasing volume directory: {}, succeeded : {}", jobworkerDir.getPath(), diskCheckDirSuccess);
    boolean versionFileDirSuccess = releaseVersionFile();
    LOG.info("Releasing version File: {}, succeeded : {}", getVersionFile(), versionFileDirSuccess);
    boolean clusterDirSuccess = releaseClusterDirectory();
    LOG.info("Releasing cluster directory: {}, succeeded : {}", clusterDir.getPath(), clusterDirSuccess);
    boolean jobworkerDirSuccess = releaseJobworkerDirectory();
    LOG.info("Releasing jobworker directory: {}, succeeded : {}", jobworkerDir.getPath(), jobworkerDirSuccess);
    // We do not release the Jobworker root directory and volume root directory,
    // other Jobworker may use these directories;

    return diskCheckDirSuccess && versionFileDirSuccess && clusterDirSuccess && jobworkerDirSuccess;
  }

  private boolean releaseJobworkerDirectory() {
    return deleteDirectory(jobworkerDir);
  }

  private boolean releaseClusterDirectory() {
    return deleteDirectory(clusterDir);
  }

  private boolean releaseVersionFile() {
    File versionFile = getVersionFile();
    if (versionFile.exists()) {
      if (!versionFile.delete()) {
        LOG.warn("Failed to delete a VERSION file: {}", versionFile.getPath());
        return false;
      }
      LOG.info("Deleted VERSION file: {}", versionFile.getPath());
    }
    return true;
  }

  private boolean releaseDiskCheckDir() {
    if (diskCheckDir == null || !diskCheckDir.exists()) {
      return true;
    }

    if (!diskCheckDir.isDirectory()) {
      LOG.warn("Disk check path exists but is not a directory: {}", diskCheckDir.getPath());
      return false;
    }

    LOG.info("Cleaning disk check directory: {}", diskCheckDir.getPath());

    File[] files = diskCheckDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (!file.delete()) {
          LOG.warn("Failed to delete file in disk check dir: {}", file.getPath());
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Checks if the volume is in NORMAL state and throws an exception if not.
   *
   * @throws IOException if volume is not in NORMAL state
   */
  private void checkStateNormal() throws IOException {
    if (state != VolumeState.NORMAL) {
      throw new IOException("Volume " + jobworkerDir.getPath()
          + " is not in NORMAL state, current state: " + state);
    }
  }

  /**
   * Gets the current state of the volume.
   *
   * @return current state
   */
  public VolumeState getState() {
    return state;
  }

  /**
   * Sets the volume state.
   *
   * @param state new state
   */
  public void setState(VolumeState state) {
    this.state = state;
  }

  /**
   * Returns if the volume is failed.
   *
   * @return true if volume is failed
   */
  public boolean isFailed() {
    return (state == VolumeState.FAILED);
  }

  /**
   * Gets the storage directory for the volume.
   *
   * @return storage directory
   */
  public File getJobworkerDir() {
    return jobworkerDir;
  }

  File getClusterDir() {
    return clusterDir;
  }

  File getCurrentDir() {
    return currentDir;
  }

  File getDiskCheckDir() {
    return diskCheckDir;
  }

  public File getVolumeRootDir() {
    return volumeRootDir;
  }

  public File getJobworkerRootDir() {
    return jobworkerRootDir;
  }

  /**
   * Gets the storage ID for the volume.
   *
   * @return storage ID
   */
  public String getStorageID() {
    return storageID;
  }

  /**
   * Gets the cluster ID associated with this volume.
   *
   * @return cluster ID
   */
  public String getClusterID() {
    return clusterID;
  }

  /**
   * Gets the Jobworker UUID with this volume.
   *
   * @return cluster ID
   */
  public String getJobworkerUuid() {
    return jobworkerUuid;
  }

  @Override
  public String toString() {
    return jobworkerDir.getPath();
  }

  /**
   * VolumeState represents the different states a StorageVolume can be in.
   * NORMAL          =&gt; Volume can be used for storage
   * FAILED          =&gt; Volume has failed due and can no longer be used
   * INCONSISTENT    =&gt; Volume Root dir is not empty and doesn’t have a VERSION file.
   * NOT_FORMATTED   =&gt; Volume Root exists but not formatted (no VERSION file)
   */
  public enum VolumeState {
    NORMAL,
    FAILED,
    INCONSISTENT,
    NOT_FORMATTED,
  }

  /**
   * Builder for JobWorkerVolume.
   */
  public static class Builder {
    private final String volumeRootStr;
    private ConfigurationSource conf;
    private String clusterID;
    private String jobWorkerUuid;
    private boolean failedVolume = false;

    public Builder(String volumeRootStr) {
      this.volumeRootStr = volumeRootStr;
    }

    public Builder conf(ConfigurationSource config) {
      this.conf = config;
      return this;
    }

    public Builder clusterID(String cid) {
      this.clusterID = cid;
      return this;
    }

    public Builder jobWorkerUuid(String uuid) {
      this.jobWorkerUuid = uuid;
      return this;
    }

    public Builder failedVolume(boolean failed) {
      this.failedVolume = failed;
      return this;
    }

    public VolatileJobworkerVolume build() throws IOException {
      return new VolatileJobworkerVolume(this);
    }
  }
}
