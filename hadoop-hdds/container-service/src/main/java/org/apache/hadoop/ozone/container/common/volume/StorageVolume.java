/*
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.checker.Checkable;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.container.common.helpers.DatanodeVersionFile;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.DiskCheckUtil;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.container.common.HDDSVolumeLayoutVersion.getLatestVersion;


/**
 * StorageVolume represents a generic Volume in datanode, could be
 * 1. HddsVolume for container storage.
 * 2. MetadataVolume for metadata(ratis) storage.
 *    This is a special type of volume, because it is managed
 *    by ratis itself, so we don't format or initialize it in Ozone.
 * 3. DbVolume for db instance storage.
 *
 * Each hdds volume has its own VERSION file. The hdds volume will have one
 * clusterUuid directory for each SCM it is a part of.
 *
 * During DN startup, if the VERSION file exists, we verify that the
 * clusterID in the version file matches the clusterID from SCM.
 */
public abstract class StorageVolume
    implements Checkable<Boolean, VolumeCheckResult> {

  private static final Logger LOG =
      LoggerFactory.getLogger(StorageVolume.class);

  // The name of the directory used for temporary files on the volume.
  public static final String TMP_DIR_NAME = "tmp";
  // The name of the directory where temporary files used to check disk
  // health are written to. This will go inside the tmp directory.
  public static final String TMP_DISK_CHECK_DIR_NAME = "disk-check";

  /**
   * Type for StorageVolume.
   */
  public enum VolumeType {
    DATA_VOLUME,
    META_VOLUME,
    DB_VOLUME,
  }

  /**
   * VolumeState represents the different states a StorageVolume can be in.
   * NORMAL          =&gt; Volume can be used for storage
   * FAILED          =&gt; Volume has failed due and can no longer be used for
   *                    storing containers.
   * NON_EXISTENT    =&gt; Volume Root dir does not exist
   * INCONSISTENT    =&gt; Volume Root dir is not empty but VERSION file is
   *                    missing or Volume Root dir is not a directory
   * NOT_FORMATTED   =&gt; Volume Root exists but not formatted(no VERSION file)
   * NOT_INITIALIZED =&gt; VERSION file exists but has not been verified for
   *                    correctness.
   */
  public enum VolumeState {
    NORMAL,
    FAILED,
    NON_EXISTENT,
    INCONSISTENT,
    NOT_FORMATTED,
    NOT_INITIALIZED
  }

  private volatile VolumeState state;

  // VERSION file properties
  private String storageID;       // id of the file system
  private String clusterID;       // id of the cluster
  private String datanodeUuid;    // id of the DataNode
  private long cTime;             // creation time of the file system state
  private int layoutVersion;      // layout version of the storage data

  private ConfigurationSource conf;

  private final File storageDir;
  private String workingDirName;
  private File tmpDir;
  private File diskCheckDir;

  private final Optional<VolumeInfo> volumeInfo;

  private final VolumeSet volumeSet;

  /*
  Fields used to implement IO based disk health checks.
  If more than ioFailureTolerance IO checks fail out of the last ioTestCount
  tests run, then the volume is considered failed.
   */
  private final int ioTestCount;
  private final int ioFailureTolerance;
  private AtomicInteger currentIOFailureCount;
  private Queue<Boolean> ioTestSlidingWindow;
  private int healthCheckFileSize;

  protected StorageVolume(Builder<?> b) throws IOException {
    if (!b.failedVolume) {
      StorageLocation location = StorageLocation.parse(b.volumeRootStr);
      storageDir = new File(location.getUri().getPath(), b.storageDirStr);
      this.volumeInfo = Optional.of(
              new VolumeInfo.Builder(b.volumeRootStr, b.conf)
          .storageType(b.storageType)
          .usageCheckFactory(b.usageCheckFactory)
          .build());
      this.volumeSet = b.volumeSet;
      this.state = VolumeState.NOT_INITIALIZED;
      this.clusterID = b.clusterID;
      this.datanodeUuid = b.datanodeUuid;
      this.conf = b.conf;

      DatanodeConfiguration dnConf =
          conf.getObject(DatanodeConfiguration.class);
      this.ioTestCount = dnConf.getVolumeIOTestCount();
      this.ioFailureTolerance = dnConf.getVolumeIOFailureTolerance();
      this.ioTestSlidingWindow = new LinkedList<>();
      this.currentIOFailureCount = new AtomicInteger(0);
      this.healthCheckFileSize = dnConf.getVolumeHealthCheckFileSize();
    } else {
      storageDir = new File(b.volumeRootStr);
      this.volumeInfo = Optional.empty();
      this.volumeSet = null;
      this.storageID = UUID.randomUUID().toString();
      this.state = VolumeState.FAILED;
      this.ioTestCount = 0;
      this.ioFailureTolerance = 0;
    }
  }

  public void format(String cid) throws IOException {
    Preconditions.checkNotNull(cid, "clusterID cannot be null while " +
        "formatting Volume");
    this.clusterID = cid;
    initialize();
  }

  /**
   * Initializes the volume.
   * Creates the Version file if not present,
   * otherwise returns with IOException.
   * @throws IOException
   */
  protected final void initialize() throws IOException {
    try {
      initializeImpl();
    } catch (Exception e) {
      shutdown();
      throw e;
    }
  }

  protected void initializeImpl() throws IOException {
    VolumeState intialVolumeState = analyzeVolumeState();
    switch (intialVolumeState) {
    case NON_EXISTENT:
      // Root directory does not exist. Create it.
      if (!getStorageDir().mkdirs()) {
        throw new IOException("Cannot create directory " + getStorageDir());
      }
      setState(VolumeState.NOT_FORMATTED);
      createVersionFile();
      break;
    case NOT_FORMATTED:
      // Version File does not exist. Create it.
      createVersionFile();
      break;
    case NOT_INITIALIZED:
      // Version File exists.
      // Verify its correctness and update property fields.
      readVersionFile();
      setState(VolumeState.NORMAL);
      break;
    case INCONSISTENT:
      // Volume Root is in an inconsistent state. Skip loading this volume.
      throw new IOException("Volume is in an " + VolumeState.INCONSISTENT +
          " state. Skipped loading volume: " + getStorageDir().getPath());
    default:
      throw new IOException("Unrecognized initial state : " +
          intialVolumeState + "of volume : " + getStorageDir());
    }
  }

  /**
   * Create the working directory for the volume at
   * <volume>/<hdds>/<workingDirName>.
   * Creates necessary subdirectories of the working directory as well. This
   * includes the tmp directory at <volume>/<hdds>/<workingDirName>/tmp.
   * Child classes may override this method to add volume specific
   * subdirectories, but they should call the parent method first to make
   * sure initial directories are constructed.
   *
   * @param dirName scmID or clusterID according to SCM HA
   *    layout feature upgrade finalization status.
   * @throws IOException
   */
  public void createWorkingDir(String dirName, MutableVolumeSet dbVolumeSet)
      throws IOException {
    File idDir = new File(getStorageDir(), dirName);
    if (!idDir.exists() && !idDir.mkdir()) {
      throw new IOException("Unable to create ID directory " + idDir +
          " for datanode.");
    }
    this.workingDirName = dirName;
  }

  public void createTmpDirs(String workDirName) throws IOException {
    this.tmpDir =
        new File(new File(getStorageDir(), workDirName), TMP_DIR_NAME);
    Files.createDirectories(tmpDir.toPath());
    diskCheckDir = createTmpSubdirIfNeeded(TMP_DISK_CHECK_DIR_NAME);
    cleanTmpDiskCheckDir();
  }

  /**
   * Create a subdirectory within this volume's tmp directory.
   * This subdirectory can be used as a work space for temporary filesystem
   * operations before they are moved to their final destination.
   */
  protected final File createTmpSubdirIfNeeded(String name) throws IOException {
    File newDir = new File(tmpDir, name);
    Files.createDirectories(newDir.toPath());
    return newDir;
  }

  private VolumeState analyzeVolumeState() {
    if (!getStorageDir().exists()) {
      // Volume Root does not exist.
      return VolumeState.NON_EXISTENT;
    }
    if (!getStorageDir().isDirectory()) {
      // Volume Root exists but is not a directory.
      LOG.warn("Volume {} exists but is not a directory,"
              + " current volume state: {}.",
          getStorageDir().getPath(), VolumeState.INCONSISTENT);
      return VolumeState.INCONSISTENT;
    }
    File[] files = getStorageDir().listFiles();
    if (files == null || files.length == 0) {
      // Volume Root exists and is empty.
      return VolumeState.NOT_FORMATTED;
    }
    if (!getVersionFile().exists()) {
      // Volume Root is non empty but VERSION file does not exist.
      LOG.warn("VERSION file does not exist in volume {},"
              + " current volume state: {}.",
          getStorageDir().getPath(), VolumeState.INCONSISTENT);
      return VolumeState.INCONSISTENT;
    }
    // Volume Root and VERSION file exist.
    return VolumeState.NOT_INITIALIZED;
  }

  /**
   * Create Version File and write property fields into it.
   * @throws IOException
   */
  private void createVersionFile() throws IOException {
    this.storageID = StorageVolumeUtil.generateUuid();
    this.cTime = Time.now();
    this.layoutVersion = getLatestVersion().getVersion();

    if (this.clusterID == null || datanodeUuid == null) {
      // HddsDatanodeService does not have the cluster information yet. Wait
      // for registration with SCM.
      LOG.debug("ClusterID not available. Cannot format the volume {}",
          getStorageDir().getPath());
      setState(VolumeState.NOT_FORMATTED);
    } else {
      // Write the version file to disk.
      writeVersionFile();
      setState(VolumeState.NORMAL);
    }
  }

  private void writeVersionFile() throws IOException {
    Preconditions.checkNotNull(this.storageID,
        "StorageID cannot be null in Version File");
    Preconditions.checkNotNull(this.clusterID,
        "ClusterID cannot be null in Version File");
    Preconditions.checkNotNull(this.datanodeUuid,
        "DatanodeUUID cannot be null in Version File");
    Preconditions.checkArgument(this.cTime > 0,
        "Creation Time should be positive");
    Preconditions.checkArgument(this.layoutVersion ==
            getLatestVersion().getVersion(),
        "Version File should have the latest LayOutVersion");

    File versionFile = getVersionFile();
    LOG.debug("Writing Version file to disk, {}", versionFile);

    DatanodeVersionFile dnVersionFile = new DatanodeVersionFile(this.storageID,
        this.clusterID, this.datanodeUuid, this.cTime, this.layoutVersion);
    dnVersionFile.createVersionFile(versionFile);
  }

  /**
   * Read Version File and update property fields.
   * Get common storage fields.
   * Should be overloaded if additional fields need to be read.
   *
   * @throws IOException on error
   */
  private void readVersionFile() throws IOException {
    File versionFile = getVersionFile();
    Properties props = DatanodeVersionFile.readFrom(versionFile);
    if (props.isEmpty()) {
      throw new InconsistentStorageStateException(
          "Version file " + versionFile + " is missing");
    }

    LOG.debug("Reading Version file from disk, {}", versionFile);
    this.storageID = StorageVolumeUtil.getStorageID(props, versionFile);
    this.clusterID = StorageVolumeUtil.getClusterID(props, versionFile,
        this.clusterID);
    this.datanodeUuid = StorageVolumeUtil.getDatanodeUUID(props, versionFile,
        this.datanodeUuid);
    this.cTime = StorageVolumeUtil.getCreationTime(props, versionFile);
    this.layoutVersion = StorageVolumeUtil.getLayOutVersion(props, versionFile);
  }

  private File getVersionFile() {
    return StorageVolumeUtil.getVersionFile(getStorageDir());
  }

  /**
   * Builder class for StorageVolume.
   * @param <T> subclass Builder
   */
  public abstract static class Builder<T extends Builder<T>> {
    private final String volumeRootStr;
    private String storageDirStr;
    private ConfigurationSource conf;
    private StorageType storageType;
    private SpaceUsageCheckFactory usageCheckFactory;
    private VolumeSet volumeSet;
    private boolean failedVolume = false;
    private String datanodeUuid;
    private String clusterID;

    public Builder(String volumeRootStr, String storageDirStr) {
      this.volumeRootStr = volumeRootStr;
      this.storageDirStr = storageDirStr;
    }

    public abstract T getThis();

    public T conf(ConfigurationSource config) {
      this.conf = config;
      return this.getThis();
    }

    public T storageType(StorageType st) {
      this.storageType = st;
      return this.getThis();
    }

    public T usageCheckFactory(SpaceUsageCheckFactory factory) {
      this.usageCheckFactory = factory;
      return this.getThis();
    }

    public T volumeSet(VolumeSet volSet) {
      this.volumeSet = volSet;
      return this.getThis();
    }

    // This is added just to create failed volume objects, which will be used
    // to create failed StorageVolume objects in the case of any exceptions
    // caused during creating StorageVolume object.
    public T failedVolume(boolean failed) {
      this.failedVolume = failed;
      return this.getThis();
    }

    public T datanodeUuid(String datanodeUUID) {
      this.datanodeUuid = datanodeUUID;
      return this.getThis();
    }

    public T clusterID(String cid) {
      this.clusterID = cid;
      return this.getThis();
    }

    public abstract StorageVolume build() throws IOException;

    public String getVolumeRootStr() {
      return this.volumeRootStr;
    }

    public boolean getFailedVolume() {
      return this.failedVolume;
    }

    public StorageType getStorageType() {
      return this.storageType;
    }
  }

  public String getVolumeRootDir() {
    return volumeInfo.map(VolumeInfo::getRootDir).orElse(null);
  }

  public long getCapacity() {
    return volumeInfo.map(VolumeInfo::getCapacity).orElse(0L);
  }

  public long getAvailable() {
    return volumeInfo.map(VolumeInfo::getAvailable).orElse(0L);

  }

  public SpaceUsageSource getCurrentUsage() {
    return volumeInfo.map(VolumeInfo::getCurrentUsage)
        .orElse(SpaceUsageSource.UNKNOWN);
  }

  public long getUsedSpace() {
    return volumeInfo.map(VolumeInfo::getScmUsed).orElse(0L);

  }

  public File getStorageDir() {
    return this.storageDir;
  }

  public String getWorkingDirName() {
    return this.workingDirName;
  }

  public File getTmpDir() {
    return this.tmpDir;
  }

  @VisibleForTesting
  public File getDiskCheckDir() {
    return this.diskCheckDir;
  }

  public void refreshVolumeInfo() {
    volumeInfo.ifPresent(VolumeInfo::refreshNow);
  }

  public Optional<VolumeInfo> getVolumeInfo() {
    return this.volumeInfo;
  }

  public void incrementUsedSpace(long usedSpace) {
    volumeInfo.ifPresent(volInfo -> volInfo
            .incrementUsedSpace(usedSpace));
  }

  public void decrementUsedSpace(long reclaimedSpace) {
    volumeInfo.ifPresent(volInfo -> volInfo
            .decrementUsedSpace(reclaimedSpace));
  }

  public VolumeSet getVolumeSet() {
    return this.volumeSet;
  }

  public StorageType getStorageType() {
    return volumeInfo.map(VolumeInfo::getStorageType)
            .orElse(StorageType.DEFAULT);
  }

  public String getStorageID() {
    return storageID;
  }

  public String getClusterID() {
    return clusterID;
  }

  public String getDatanodeUuid() {
    return datanodeUuid;
  }

  public long getCTime() {
    return cTime;
  }

  public int getLayoutVersion() {
    return layoutVersion;
  }

  public VolumeState getStorageState() {
    return state;
  }

  public void setState(VolumeState state) {
    this.state = state;
  }

  public boolean isFailed() {
    return (state == VolumeState.FAILED);
  }

  public ConfigurationSource getConf() {
    return conf;
  }

  public void failVolume() {
    setState(VolumeState.FAILED);
    volumeInfo.ifPresent(VolumeInfo::shutdownUsageThread);
  }

  public void shutdown() {
    setState(VolumeState.NON_EXISTENT);
    volumeInfo.ifPresent(VolumeInfo::shutdownUsageThread);
    cleanTmpDiskCheckDir();
  }

  /**
   * Delete all temporary files in the directory used ot check disk health.
   */
  private void cleanTmpDiskCheckDir() {
    // If the volume was shut down before initialization completed, skip
    // emptying the directory.
    if (diskCheckDir == null) {
      return;
    }

    if (!diskCheckDir.exists()) {
      LOG.warn("Unable to clear disk check files from {}. Directory does " +
          "not exist.", diskCheckDir);
      return;
    }

    if (!diskCheckDir.isDirectory()) {
      LOG.warn("Unable to clear disk check files from {}. Location is not a" +
          " directory", diskCheckDir);
      return;
    }

    try (Stream<Path> files = Files.list(diskCheckDir.toPath())) {
      files.map(Path::toFile).filter(File::isFile).forEach(file -> {
        try {
          Files.delete(file.toPath());
        } catch (IOException ex) {
          LOG.warn("Failed to delete temporary volume health check file {}",
              file);
        }
      });
    } catch (IOException ex) {
      LOG.warn("Failed to list contents of volume health check directory {} " +
          "for deleting.", diskCheckDir);
    }
  }

  /**
   * Run a check on the current volume to determine if it is healthy. The
   * check consists of a directory check and an IO check.
   *
   * If the directory check fails, the volume check fails immediately.
   * The IO check is allows to fail up to {@code ioFailureTolerance} times
   * out of the last {@code ioTestCount} IO checks before this volume check is
   * failed. Each call to this method runs one IO check.
   *
   * @param unused context for the check, ignored.
   * @return result of checking the volume.
   * @throws InterruptedException if there was an error during the volume
   *    check because the thread was interrupted.
   * @throws Exception if an exception was encountered while running
   *    the volume check and the thread was not interrupted.
   */
  @Override
  public synchronized VolumeCheckResult check(@Nullable Boolean unused)
      throws Exception {
    boolean directoryChecksPassed =
        DiskCheckUtil.checkExistence(storageDir) &&
        DiskCheckUtil.checkPermissions(storageDir);
    // If the directory is not present or has incorrect permissions, fail the
    // volume immediately. This is not an intermittent error.
    if (!directoryChecksPassed) {
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException("Directory check of volume " + this +
            " interrupted.");
      }
      return VolumeCheckResult.FAILED;
    }

    // If IO test count is set to 0, IO tests for disk health are disabled.
    if (ioTestCount == 0) {
      return VolumeCheckResult.HEALTHY;
    }

    // Since IO errors may be intermittent, volume remains healthy until the
    // threshold of failures is crossed.
    boolean diskChecksPassed = DiskCheckUtil.checkReadWrite(storageDir,
        diskCheckDir, healthCheckFileSize);
    if (Thread.currentThread().isInterrupted()) {
      // Thread interrupt may have caused IO operations to abort. Do not
      // consider this a failure.
      throw new InterruptedException("IO check of volume " + this +
          " interrupted.");
    }

    // Move the sliding window of IO test results forward 1 by adding the
    // latest entry and removing the oldest entry from the window.
    // Update the failure counter for the new window.
    ioTestSlidingWindow.add(diskChecksPassed);
    if (!diskChecksPassed) {
      currentIOFailureCount.incrementAndGet();
    }
    if (ioTestSlidingWindow.size() > ioTestCount &&
        Objects.equals(ioTestSlidingWindow.poll(), Boolean.FALSE)) {
      currentIOFailureCount.decrementAndGet();
    }

    // If the failure threshold has been crossed, fail the volume without
    // further scans.
    // Once the volume is failed, it will not be checked anymore.
    // The failure counts can be left as is.
    if (currentIOFailureCount.get() > ioFailureTolerance) {
      LOG.info("Failed IO test for volume {}: the last {} runs " +
              "encountered {} out of {} tolerated failures.", this,
          ioTestSlidingWindow.size(), currentIOFailureCount,
          ioFailureTolerance);
      return VolumeCheckResult.FAILED;
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("IO test results for volume {}: the last {} runs encountered " +
              "{} out of {} tolerated failures", this,
          ioTestSlidingWindow.size(),
          currentIOFailureCount, ioFailureTolerance);
    }

    return VolumeCheckResult.HEALTHY;
  }

  @Override
  public int hashCode() {
    return Objects.hash(storageDir);
  }

  @Override
  public boolean equals(Object other) {
    return this == other
        || other instanceof StorageVolume
        && ((StorageVolume) other).storageDir.equals(this.storageDir);
  }

  @Override
  public String toString() {
    return getStorageDir().toString();
  }
}
