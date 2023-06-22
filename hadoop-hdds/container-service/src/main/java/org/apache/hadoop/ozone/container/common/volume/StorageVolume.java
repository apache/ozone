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

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.checker.Checkable;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.container.common.helpers.DatanodeVersionFile;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

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

  private final Optional<VolumeInfo> volumeInfo;

  private final VolumeSet volumeSet;

  private String workingDir;

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
    } else {
      storageDir = new File(b.volumeRootStr);
      this.volumeInfo = Optional.empty();
      this.volumeSet = null;
      this.storageID = UUID.randomUUID().toString();
      this.state = VolumeState.FAILED;
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
  protected void initialize() throws IOException {
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
   * Create working directory for cluster io loads.
   * @param workingDirName scmID or clusterID according to SCM HA config
   * @param dbVolumeSet optional dbVolumes
   * @throws IOException
   */
  public void createWorkingDir(String workingDirName,
      MutableVolumeSet dbVolumeSet) throws IOException {
    File idDir = new File(getStorageDir(), workingDirName);
    if (!idDir.mkdir()) {
      throw new IOException("Unable to create ID directory " + idDir +
          " for datanode.");
    }
    this.workingDir = workingDirName;
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

  public long getUsedSpace() {
    return volumeInfo.map(VolumeInfo::getScmUsed).orElse(0L);

  }

  public File getStorageDir() {
    return this.storageDir;
  }

  public String getWorkingDir() {
    return this.workingDir;
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
  }

  /**
   * Run a check on the current volume to determine if it is healthy.
   * @param unused context for the check, ignored.
   * @return result of checking the volume.
   * @throws Exception if an exception was encountered while running
   *            the volume check.
   */
  @Override
  public VolumeCheckResult check(@Nullable Boolean unused) throws Exception {
    if (!storageDir.exists()) {
      return VolumeCheckResult.FAILED;
    }
    DiskChecker.checkDir(storageDir);
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
