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

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_DATANODE_IO_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_NAME;
import static org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil.initPerDiskDBStore;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;
import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.common.utils.RawDB;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures.SchemaV3;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HddsVolume represents volume in a datanode. {@link MutableVolumeSet}
 * maintains a list of HddsVolumes, one for each volume in the Datanode.
 * {@link VolumeUsage} in encompassed by this class.
 * <p>
 * The disk layout per volume is as follows:
 * <p>../hdds/VERSION
 * <p>{@literal ../hdds/<<clusterUuid>>/current/<<containerDir>>/<<containerID
 * >>/metadata}
 * <p>{@literal ../hdds/<<clusterUuid>>/current/<<containerDir>>/<<containerID
 * >>/<<dataDir>>}
 * <p>
 * Each hdds volume has its own VERSION file. The hdds volume will have one
 * clusterUuid directory for each SCM it is a part of (currently only one SCM is
 * supported).
 * <p>
 * During DN startup, if the VERSION file exists, we verify that the
 * clusterID in the version file matches the clusterID from SCM.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@SuppressWarnings("finalclass")
public class HddsVolume extends StorageVolume {

  private static final Logger LOG = LoggerFactory.getLogger(HddsVolume.class);

  public static final String HDDS_VOLUME_DIR = "hdds";
  public static final String TMP_CONTAINER_DELETE_DIR_NAME =
      "deleted-containers";

  private final VolumeIOStats volumeIOStats;
  private final VolumeInfoMetrics volumeInfoMetrics;

  private ContainerController controller;

  private final AtomicLong committedBytes = new AtomicLong(); // till Open containers become full
  private Function<HddsVolume, Long> gatherContainerUsages = (K) -> 0L;

  private final ConcurrentSkipListSet<Long> containerIds = new ConcurrentSkipListSet<>();

  // Mentions the type of volume
  private final VolumeType type = VolumeType.DATA_VOLUME;
  // The dedicated DbVolume that the db instance of this HddsVolume resides.
  // This is optional, if null then the db instance resides on this HddsVolume.
  private DbVolume dbVolume;
  // The subdirectory with storageID as its name, used to build the
  // container db path. This is initialized only once together with dbVolume,
  // and stored as a member to prevent spawning lots of File objects.
  private File dbParentDir;
  private File deletedContainerDir;
  private final AtomicBoolean dbLoaded = new AtomicBoolean(false);
  private final AtomicBoolean dbLoadFailure = new AtomicBoolean(false);

  /**
   * Builder for HddsVolume.
   */
  public static class Builder extends StorageVolume.Builder<Builder> {

    public Builder(String volumeRootStr) {
      super(volumeRootStr, HDDS_VOLUME_DIR);
    }

    @Override
    public Builder getThis() {
      return this;
    }

    @Override
    public HddsVolume build() throws IOException {
      return new HddsVolume(this);
    }
  }

  private HddsVolume(Builder b) throws IOException {
    super(b);

    if (!b.getFailedVolume()) {
      this.setState(VolumeState.NOT_INITIALIZED);
      ConfigurationSource conf = getConf();
      int[] intervals = conf.getInts(OZONE_DATANODE_IO_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY);
      this.volumeIOStats = new VolumeIOStats(b.getVolumeRootStr(),
          this.getStorageDir().toString(), intervals);
      this.volumeInfoMetrics =
          new VolumeInfoMetrics(b.getVolumeRootStr(), this);

      initialize();
    } else {
      // Builder is called with failedVolume set, so create a failed volume
      // HddsVolume Object.
      this.setState(VolumeState.FAILED);
      volumeIOStats = null;
      volumeInfoMetrics = new VolumeInfoMetrics(b.getVolumeRootStr(), this);
    }

    LOG.info("HddsVolume: {}", getReport());
  }

  @Override
  public void createWorkingDir(String dirName, MutableVolumeSet dbVolumeSet)
      throws IOException {
    super.createWorkingDir(dirName, dbVolumeSet);

    // Create DB store for a newly formatted volume
    if (VersionedDatanodeFeatures.isFinalized(
        HDDSLayoutFeature.DATANODE_SCHEMA_V3)) {
      createDbStore(dbVolumeSet);
    }
  }

  @Override
  public void createTmpDirs(String workDirName) throws IOException {
    super.createTmpDirs(workDirName);
    deletedContainerDir =
        createTmpSubdirIfNeeded(TMP_CONTAINER_DELETE_DIR_NAME);
    cleanDeletedContainerDir();
  }

  public File getHddsRootDir() {
    return super.getStorageDir();
  }

  public VolumeType getType() {
    return type;
  }

  public VolumeIOStats getVolumeIOStats() {
    return volumeIOStats;
  }

  public VolumeInfoMetrics getVolumeInfoStats() {
    return volumeInfoMetrics;
  }

  @Override
  protected StorageLocationReport.Builder reportBuilder() {
    StorageLocationReport.Builder builder = super.reportBuilder();
    if (!builder.isFailed()) {
      builder.setCommitted(getCommittedBytes())
          .setFreeSpaceToSpare(getFreeSpaceToSpare(builder.getCapacity()));
    }
    return builder;
  }

  @Override
  public void failVolume() {
    super.failVolume();
    if (volumeIOStats != null) {
      volumeIOStats.unregister();
    }
    closeDbStore();
  }

  @Override
  public void shutdown() {
    super.shutdown();
    if (volumeIOStats != null) {
      volumeIOStats.unregister();
    }
    if (volumeInfoMetrics != null) {
      volumeInfoMetrics.unregister();
    }
    closeDbStore();
    cleanDeletedContainerDir();
  }

  /**
   * Delete all files under
   * volume/hdds/cluster-id/tmp/deleted-containers.
   * This is the directory where containers are moved when they are deleted
   * from the system, but before being removed from the filesystem. This
   * makes the deletion atomic.
   */
  public void cleanDeletedContainerDir() {
    // If the volume was shut down before initialization completed, skip
    // emptying the directory.
    if (deletedContainerDir == null) {
      return;
    }

    if (!deletedContainerDir.exists()) {
      LOG.warn("Unable to clear deleted containers from {}. Directory does " +
          "not exist.", deletedContainerDir);
      return;
    }

    if (!deletedContainerDir.isDirectory()) {
      LOG.warn("Unable to clear deleted containers from {}. Location is not a" +
          " directory", deletedContainerDir);
      return;
    }

    File[] containerDirs = deletedContainerDir.listFiles(File::isDirectory);
    if (containerDirs == null) {
      // Either directory does not exist or IO error. Either way we cannot
      // proceed with deletion.
      LOG.warn("Failed to clear container delete directory {}. Directory " +
          "could not be accessed.", deletedContainerDir);
      return;
    }

    for (File containerDir: containerDirs) {
      // --------------------------------------------
      // On datanode restart, we populate the container set
      // based on the available datanode volumes and
      // populate the container metadata based on the values in RocksDB.
      // The container is in the tmp directory,
      // so it won't be loaded in the container set
      // --------------------------------------------
      try {
        if (containerDir.isDirectory()) {
          FileUtils.deleteDirectory(containerDir);
        } else {
          FileUtils.delete(containerDir);
        }
      } catch (IOException ex) {
        LOG.warn("Failed to remove container directory {}.",
            deletedContainerDir, ex);
      }
    }
  }

  @Override
  public synchronized VolumeCheckResult check(@Nullable Boolean unused)
      throws Exception {
    volumeInfoMetrics.incNumScans();
    checkVolumeUsages();

    VolumeCheckResult result = super.check(unused);

    if (isDbLoadFailure()) {
      LOG.warn("Volume {} failed to access RocksDB: RocksDB parent directory is null, " +
          "the volume might not have been loaded properly.", getStorageDir());
      return VolumeCheckResult.FAILED;
    }

    if (result != VolumeCheckResult.HEALTHY ||
        !getDatanodeConfig().getContainerSchemaV3Enabled() || !isDbLoaded()) {
      return result;
    }

    // Check that per-volume RocksDB is present.
    File dbFile = new File(dbParentDir, CONTAINER_DB_NAME);
    if (!dbFile.exists() || !dbFile.canRead()) {
      LOG.warn("Volume {} failed health check. Could not access RocksDB at " +
          "{}", getStorageDir(), dbFile);
      return VolumeCheckResult.FAILED;
    }

    return checkDbHealth(dbFile);
  }

  @VisibleForTesting
  public VolumeCheckResult checkDbHealth(File dbFile) throws InterruptedException {
    if (getIoTestCount() == 0) {
      return VolumeCheckResult.HEALTHY;
    }

    try (ManagedOptions managedOptions = new ManagedOptions();
         ManagedRocksDB readOnlyDb = ManagedRocksDB.openReadOnly(managedOptions, dbFile.toString())) {
      // Do nothing. Only check if rocksdb is accessible.
      LOG.debug("Successfully opened the database at \"{}\" for HDDS volume {}.", dbFile, getStorageDir());
    } catch (Exception e) {
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException("Check of database for volume " + this + " interrupted.");
      }
      LOG.warn("Could not open Volume DB located at {}", dbFile, e);
      getIoTestSlidingWindow().add();
    }

    if (getIoTestSlidingWindow().isExceeded()) {
      LOG.error("Failed to open the database at \"{}\" for HDDS volume {}: " +
              "encountered more than the {} tolerated failures.",
          dbFile, this, getIoTestSlidingWindow().getWindowSize());
      return VolumeCheckResult.FAILED;
    }

    LOG.debug("Successfully opened the database at \"{}\" for HDDS volume {}: " +
            "encountered {} out of {} tolerated failures",
        dbFile, this, getIoTestSlidingWindow().getNumEventsInWindow(), getIoTestSlidingWindow().getWindowSize());
    return VolumeCheckResult.HEALTHY;
  }

  void checkVolumeUsages() {
    boolean isEnoughSpaceAvailable = true;
    SpaceUsageSource currentUsage = getCurrentUsage();
    long getFreeSpaceToSpare = getFreeSpaceToSpare(currentUsage.getCapacity());
    final long committed = committedBytes.get();
    final long available = currentUsage.getAvailable();
    if (available < getFreeSpaceToSpare) {
      LOG.warn("Volume {} has insufficient space for write operation. Available: {}, Free space to spare: {}",
          getStorageDir(), available, getFreeSpaceToSpare);
      isEnoughSpaceAvailable = false;
    } else if (committed > 0 && available < committed + getFreeSpaceToSpare) {
      LOG.warn("Volume {} has insufficient space for on-going container write operation. " +
              "Committed: {}, Available: {}, Free space to spare: {}",
          getStorageDir(), committed, available, getFreeSpaceToSpare);
      isEnoughSpaceAvailable = false;
    }

    volumeInfoMetrics.setAvailableSpaceInsufficient(!isEnoughSpaceAvailable);

    final VolumeUsage usage = getVolumeUsage();
    if (usage != null && usage.getReservedInBytes() > 0) {
      final SpaceUsageSource realUsage = usage.realUsage();
      long reservedUsed = VolumeUsage.getOtherUsed(realUsage);
      final boolean crossesLimit = reservedUsed > usage.getReservedInBytes();
      if (crossesLimit) {
        LOG.warn("Volume {} reserved usages {} is higher than actual allocated reserved space {}. (Real usage: {})",
            getStorageDir(), reservedUsed, usage.getReservedInBytes(), realUsage);
      }
      volumeInfoMetrics.setReservedCrossesLimit(crossesLimit);
    } else {
      volumeInfoMetrics.setReservedCrossesLimit(false);
    }
  }

  /**
   * add "delta" bytes to committed space in the volume.
   *
   * @param delta bytes to add to committed space counter
   * @return bytes of committed space
   */
  public long incCommittedBytes(long delta) {
    return committedBytes.addAndGet(delta);
  }

  /**
   * return the committed space in the volume.
   *
   * @return bytes of committed space
   */
  public long getCommittedBytes() {
    return committedBytes.get();
  }

  public long getFreeSpaceToSpare(long volumeCapacity) {
    return getDatanodeConfig().getMinFreeSpace(volumeCapacity);
  }

  @Override
  public void setGatherContainerUsages(Function<HddsVolume, Long> gatherContainerUsages) {
    this.gatherContainerUsages = gatherContainerUsages;
  }

  @Override
  protected long containerUsedSpace() {
    return gatherContainerUsages.apply(this);
  }

  @Override
  public File getContainerDirsPath() {
    if (getStorageState() != VolumeState.NORMAL) {
      return null;
    }
    File hddsVolumeRootDir = getHddsRootDir();
    //filtering storage directory
    File[] storageDirs = hddsVolumeRootDir.listFiles(File::isDirectory);
    if (storageDirs == null) {
      LOG.error("IO error for the volume {}, directory not found", hddsVolumeRootDir);
      return null;
    }
    File clusterIDDir = new File(hddsVolumeRootDir, getClusterID());
    if (storageDirs.length == 1 && !clusterIDDir.exists()) {
      // If this volume was formatted pre SCM HA, this will be the SCM ID.
      // A cluster ID symlink will exist in this case only if this cluster is finalized for SCM HA.
      // If the volume was formatted post SCM HA, this will be the cluster ID.
      clusterIDDir = storageDirs[0];
    }
    return new File(clusterIDDir, Storage.STORAGE_DIR_CURRENT);
  }

  public void setDbVolume(DbVolume dbVolume) {
    this.dbVolume = dbVolume;
  }

  public DbVolume getDbVolume() {
    return this.dbVolume;
  }

  public File getDbParentDir() {
    return this.dbParentDir;
  }

  @VisibleForTesting
  public void setDbParentDir(File dbParentDir) {
    this.dbParentDir = dbParentDir;
  }

  public File getDeletedContainerDir() {
    return this.deletedContainerDir;
  }

  @VisibleForTesting
  public void setDeletedContainerDir(File deletedContainerDir) {
    this.deletedContainerDir = deletedContainerDir;
  }

  public boolean isDbLoaded() {
    return dbLoaded.get();
  }

  public boolean isDbLoadFailure() {
    return dbLoadFailure.get();
  }

  public void loadDbStore(boolean readOnly) throws IOException {
    // DN startup for the first time, not registered yet,
    // so the DbVolume is not formatted.
    if (!getStorageState().equals(VolumeState.NORMAL)) {
      return;
    }

    // DB is already loaded
    if (dbLoaded.get()) {
      LOG.warn("Schema V3 db is already loaded from {} for volume {}",
          getDbParentDir(), getStorageID());
      return;
    }

    File clusterIdDir = new File(dbVolume == null ?
        getStorageDir() : dbVolume.getStorageDir(),
        getClusterID());
    if (!clusterIdDir.exists()) {
      throw new IOException("Working dir " + clusterIdDir.getAbsolutePath() +
          " not created for HddsVolume: " + getStorageDir().getAbsolutePath());
    }

    File storageIdDir = new File(clusterIdDir, getStorageID());
    if (!storageIdDir.exists()) {
      throw new IOException("Db parent dir " + storageIdDir.getAbsolutePath() +
          " not found for HddsVolume: " + getStorageDir().getAbsolutePath());
    }

    File containerDBFile = new File(storageIdDir, CONTAINER_DB_NAME);
    if (!containerDBFile.exists()) {
      throw new IOException("Db dir " + storageIdDir.getAbsolutePath() +
          " not found for HddsVolume: " + getStorageDir().getAbsolutePath());
    }

    String containerDBPath = containerDBFile.getAbsolutePath();
    try {
      initPerDiskDBStore(containerDBPath, getConf(), readOnly);
    } catch (Throwable e) {
      dbLoadFailure.set(true);
      throw new IOException("Can't init db instance under path "
          + containerDBPath + " for volume " + getStorageID(), e);
    }

    dbParentDir = storageIdDir;
    dbLoaded.set(true);
    LOG.info("SchemaV3 db is loaded at {} for volume {}", containerDBPath,
        getStorageID());
  }

  public void setController(ContainerController controller) {
    this.controller = controller;
  }

  public long getContainers() {
    if (controller != null) {
      return controller.getContainerCount(this);
    }
    return 0;
  }

  public void addContainer(long containerId) {
    containerIds.add(containerId);
  }

  public void removeContainer(long containerId) {
    containerIds.remove(containerId);
  }

  public Iterator<Long> getContainerIterator() {
    return containerIds.iterator();
  }

  public long getContainerCount() {
    return containerIds.size();
  }

  /**
   * Pick a DbVolume for HddsVolume and init db instance.
   * Use the HddsVolume directly if no DbVolume found.
   */
  public void createDbStore(MutableVolumeSet dbVolumeSet) throws IOException {
    DbVolume chosenDbVolume = null;
    File clusterIdDir;
    String workingDirName = getWorkingDirName() == null ? getClusterID() :
        getWorkingDirName();

    if (dbVolumeSet == null || dbVolumeSet.getVolumesList().isEmpty()) {
      // No extra db volumes specified, just create db under the HddsVolume.
      clusterIdDir = new File(getStorageDir(), workingDirName);
    } else {
      // Randomly choose a DbVolume for simplicity.
      List<DbVolume> dbVolumeList = StorageVolumeUtil.getDbVolumesList(
          dbVolumeSet.getVolumesList());
      chosenDbVolume = dbVolumeList.get(
          ThreadLocalRandom.current().nextInt(dbVolumeList.size()));
      clusterIdDir = new File(chosenDbVolume.getStorageDir(), workingDirName);
    }

    if (!clusterIdDir.exists()) {
      throw new IOException("The working dir "
          + clusterIdDir.getAbsolutePath() + " is missing for volume "
          + getStorageID());
    }

    // Init subdir with the storageID of HddsVolume.
    File storageIdDir = new File(clusterIdDir, getStorageID());
    if (!storageIdDir.mkdirs() && !storageIdDir.exists()) {
      throw new IOException("Can't make subdir under "
          + clusterIdDir.getAbsolutePath() + " for volume "
          + getStorageID());
    }

    // Create the db instance for HddsVolume under the subdir above.
    String containerDBPath = new File(storageIdDir, CONTAINER_DB_NAME)
        .getAbsolutePath();
    try {
      HddsVolumeUtil.initPerDiskDBStore(containerDBPath, getConf(), false);
      dbLoaded.set(true);
      dbLoadFailure.set(false);
      LOG.info("SchemaV3 db is created and loaded at {} for volume {}",
          containerDBPath, getStorageID());
    } catch (IOException e) {
      dbLoadFailure.set(true);
      String errMsg = "Can't create db instance under path "
          + containerDBPath + " for volume " + getStorageID();
      LOG.error(errMsg, e);
      throw new IOException(errMsg);
    }

    // Set the dbVolume and dbParentDir of the HddsVolume for db path lookup.
    dbVolume = chosenDbVolume;
    dbParentDir = storageIdDir;
    if (chosenDbVolume != null) {
      chosenDbVolume.addHddsDbStorePath(getStorageID(), containerDBPath);
    }

    // If SchemaV3 is disabled, close the DB instance
    if (!SchemaV3.isFinalizedAndEnabled(getConf())) {
      closeDbStore();
    }
  }

  private void closeDbStore() {
    if (!dbLoaded.get()) {
      return;
    }

    String containerDBPath = new File(dbParentDir, CONTAINER_DB_NAME)
        .getAbsolutePath();
    DatanodeStoreCache.getInstance().removeDB(containerDBPath);
    dbLoaded.set(false);
    dbLoadFailure.set(false);
    LOG.info("SchemaV3 db is stopped at {} for volume {}", containerDBPath,
        getStorageID());
  }

  public void compactDb() {
    File dbFile = new File(getDbParentDir(), CONTAINER_DB_NAME);
    String dbFilePath = dbFile.getAbsolutePath();
    try {
      // Calculate number of files per level and size per level
      RawDB rawDB =
          DatanodeStoreCache.getInstance().getDB(dbFilePath, getConf());
      long start = Time.monotonicNowNanos();
      rawDB.getStore().compactionIfNeeded();
      volumeInfoMetrics.dbCompactTimesNanoSecondsIncr(
          Time.monotonicNowNanos() - start);
    } catch (Exception e) {
      LOG.warn("compact rocksdb error in {}", dbFilePath, e);
    }
  }
}
