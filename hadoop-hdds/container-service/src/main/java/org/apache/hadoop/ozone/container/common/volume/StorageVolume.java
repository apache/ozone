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

import static org.apache.hadoop.ozone.container.common.HDDSVolumeLayoutVersion.getLatestVersion;
import static org.apache.hadoop.ozone.container.common.volume.HddsVolume.HDDS_VOLUME_DIR;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import jakarta.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckParams;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.checker.Checkable;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.container.common.helpers.DatanodeVersionFile;
import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.DiskCheckUtil;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public abstract class StorageVolume implements Checkable<Boolean, VolumeCheckResult> {

  private static final Logger LOG = LoggerFactory.getLogger(StorageVolume.class);

  // The name of the directory used for temporary files on the volume.
  public static final String TMP_DIR_NAME = "tmp";
  // The name of the directory where temporary files used to check disk
  // health are written to. This will go inside the tmp directory.
  public static final String TMP_DISK_CHECK_DIR_NAME = "disk-check";

  private volatile VolumeState state;

  // VERSION file properties
  private String storageID;       // id of the file system
  private String clusterID;       // id of the cluster
  private String datanodeUuid;    // id of the DataNode
  private long cTime;             // creation time of the file system state
  private int layoutVersion;      // layout version of the storage data

  private ConfigurationSource conf;
  private DatanodeConfiguration dnConf;

  private final StorageType storageType;
  private final String volumeRoot;
  private final File storageDir;
  /** This is the raw storage dir location, saved for logging, to avoid repeated filesystem lookup. */
  private final String storageDirStr;
  private String workingDirName;
  private File tmpDir;
  private File diskCheckDir;

  private final VolumeUsage volumeUsage;

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

  protected StorageVolume(Builder<?> b) throws IOException {
    storageType = b.storageType;
    volumeRoot = b.volumeRootStr;
    if (!b.failedVolume) {
      StorageLocation location = StorageLocation.parse(volumeRoot);
      storageDir = new File(location.getUri().getPath(), b.storageDirStr);
      SpaceUsageCheckParams checkParams = getSpaceUsageCheckParams(b, this::getContainerDirsPath);
      checkParams.setContainerUsedSpace(this::containerUsedSpace);
      volumeUsage = new VolumeUsage(checkParams, b.conf);
      this.volumeSet = b.volumeSet;
      this.state = VolumeState.NOT_INITIALIZED;
      this.clusterID = b.clusterID;
      this.datanodeUuid = b.datanodeUuid;

      this.conf = b.conf;
      this.dnConf = conf.getObject(DatanodeConfiguration.class);
      this.ioTestCount = dnConf.getVolumeIOTestCount();
      this.ioFailureTolerance = dnConf.getVolumeIOFailureTolerance();
      this.ioTestSlidingWindow = new LinkedList<>();
      this.currentIOFailureCount = new AtomicInteger(0);
      this.healthCheckFileSize = dnConf.getVolumeHealthCheckFileSize();
    } else {
      storageDir = new File(b.volumeRootStr);
      volumeUsage = null;
      this.volumeSet = null;
      this.storageID = UUID.randomUUID().toString();
      this.state = VolumeState.FAILED;
      this.ioTestCount = 0;
      this.ioFailureTolerance = 0;
      this.conf = null;
      this.dnConf = null;
    }
    this.storageDirStr = storageDir.getAbsolutePath();
  }

  protected long containerUsedSpace() {
    // container used space applicable only for HddsVolume
    return 0;
  }

  public File getContainerDirsPath() {
    // container dir path applicable only for HddsVolume
    return null;
  }

  public void setGatherContainerUsages(Function<HddsVolume, Long> gatherContainerUsages) {
    // Operation only for HddsVolume which have container data
  }

  public void format(String cid) throws IOException {
    this.clusterID = Objects.requireNonNull(cid, "clusterID == null");
    initialize();
  }

  public void start() throws IOException {
    if (volumeUsage != null) {
      volumeUsage.start();
    }
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
      // Set permissions on storage directory (e.g., hdds subdirectory)
      setStorageDirPermissions();
      setState(VolumeState.NOT_FORMATTED);
      createVersionFile();
      break;
    case NOT_FORMATTED:
      // Version File does not exist. Create it.
      // Ensure permissions are correct even if directory already existed
      setStorageDirPermissions();
      createVersionFile();
      break;
    case NOT_INITIALIZED:
      // Version File exists.
      // Verify its correctness and update property fields.
      // Ensure permissions are correct even if directory already existed
      setStorageDirPermissions();
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
    Objects.requireNonNull(storageID, "storageID == null");
    Objects.requireNonNull(clusterID, "clusterID == null");
    Objects.requireNonNull(datanodeUuid, "datanodeUuid == null");
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
    private StorageType storageType = StorageType.DEFAULT;
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
      this.storageType = Objects.requireNonNull(st, "storageType == null");
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

    public String getStorageDirStr() {
      return this.storageDirStr;
    }
  }

  public String getVolumeRootDir() {
    return volumeRoot;
  }

  /** Get current usage of the volume. */
  public SpaceUsageSource.Fixed getCurrentUsage() {
    return volumeUsage != null ? volumeUsage.getCurrentUsage() : SpaceUsageSource.UNKNOWN;
  }

  protected StorageLocationReport.Builder reportBuilder() {
    StorageLocationReport.Builder builder = StorageLocationReport.newBuilder()
        .setFailed(isFailed())
        .setId(getStorageID())
        .setStorageLocation(storageDirStr)
        .setStorageType(storageType);

    if (!builder.isFailed()) {
      SpaceUsageSource.Fixed fsUsage = volumeUsage.realUsage();
      SpaceUsageSource usage = volumeUsage.getCurrentUsage(fsUsage);
      builder.setCapacity(usage.getCapacity())
          .setRemaining(usage.getAvailable())
          .setScmUsed(usage.getUsedSpace())
          .setReserved(volumeUsage.getReservedInBytes())
          .setFsCapacity(fsUsage.getCapacity())
          .setFsAvailable(fsUsage.getAvailable());
    }

    return builder;
  }

  public StorageLocationReport getReport() {
    return reportBuilder().build();
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

  public void refreshVolumeUsage() {
    if (volumeUsage != null) {
      volumeUsage.refreshNow();
    }
  }

  /** @see #getCurrentUsage() */
  public VolumeUsage getVolumeUsage() {
    return volumeUsage;
  }

  public void incrementUsedSpace(long usedSpace) {
    if (volumeUsage != null) {
      volumeUsage.incrementUsedSpace(usedSpace);
    }
  }

  public void decrementUsedSpace(long reclaimedSpace) {
    if (volumeUsage != null) {
      volumeUsage.decrementUsedSpace(reclaimedSpace);
    }
  }

  public VolumeSet getVolumeSet() {
    return this.volumeSet;
  }

  public StorageType getStorageType() {
    return storageType;
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

  @VisibleForTesting
  public void setConf(ConfigurationSource newConf) {
    this.conf = newConf;
    this.dnConf = newConf.getObject(DatanodeConfiguration.class);
  }

  public DatanodeConfiguration getDatanodeConfig() {
    return dnConf;
  }

  public void failVolume() {
    setState(VolumeState.FAILED);
    if (volumeUsage != null) {
      volumeUsage.shutdown();
    }
  }

  public void shutdown() {
    setState(VolumeState.NON_EXISTENT);
    if (volumeUsage != null) {
      volumeUsage.shutdown();
    }
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

    // At least some space required to check disk read/write
    // If there are not enough space remaining,
    // to avoid volume failure we can ignore checking disk read/write
    int minimumDiskSpace = healthCheckFileSize * 2;
    if (getCurrentUsage().getAvailable() < minimumDiskSpace) {
      ioTestSlidingWindow.add(true);
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

    // As WRITE keeps happening there is probability, disk has become full during above check.
    // We can check again if disk is full. If it is full,
    // in this case keep volume as healthy so that READ can still be served
    if (!diskChecksPassed && getCurrentUsage().getAvailable() < minimumDiskSpace) {
      ioTestSlidingWindow.add(true);
      return VolumeCheckResult.HEALTHY;
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
      LOG.error("Failed IO test for volume {}: the last {} runs " +
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

  private static SpaceUsageCheckParams getSpaceUsageCheckParams(Builder b, Supplier<File> exclusionProvider)
      throws IOException {
    File root = new File(b.volumeRootStr);

    boolean succeeded = root.isDirectory() || root.mkdirs();

    if (!succeeded) {
      LOG.error("Unable to create the volume root dir at : {}", root);
      throw new IOException("Unable to create the volume root dir at " + root);
    }

    // Set permissions on volume root directory immediately after creation/check
    // (for data volumes, we want to ensure the root has secure permissions,
    // even if the directory already existed from a previous run)
    // This follows the same pattern as metadata directories in getDirectoryFromConfig()
    if (b.conf != null && root.exists() && HDDS_VOLUME_DIR.equals(b.getStorageDirStr())) {
      ServerUtils.setDataDirectoryPermissions(root, b.conf,
          ScmConfigKeys.HDDS_DATANODE_DATA_DIR_PERMISSIONS);
    }

    SpaceUsageCheckFactory usageCheckFactory = b.usageCheckFactory;
    if (usageCheckFactory == null) {
      usageCheckFactory = SpaceUsageCheckFactory.create(b.conf);
    }

    return usageCheckFactory.paramsFor(root, exclusionProvider);
  }

  /**
   * Sets permissions on the storage directory (e.g., hdds subdirectory).
   */
  private void setStorageDirPermissions() {
    if (conf != null && getStorageDir().exists()) {
      ServerUtils.setDataDirectoryPermissions(getStorageDir(), conf,
          ScmConfigKeys.HDDS_DATANODE_DATA_DIR_PERMISSIONS);
    }
  }
}
