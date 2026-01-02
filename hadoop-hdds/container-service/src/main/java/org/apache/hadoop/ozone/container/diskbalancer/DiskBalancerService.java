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

package org.apache.hadoop.ozone.container.diskbalancer;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.calculateVolumeDataDensity;
import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.getIdealUsage;
import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.getVolumeUsages;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerRunningStatus;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.utils.ContainerLogger;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.VolumeFixedUsage;
import org.apache.hadoop.ozone.container.diskbalancer.policy.ContainerChoosingPolicy;
import org.apache.hadoop.ozone.container.diskbalancer.policy.DiskBalancerVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A per-datanode disk balancing service takes in charge
 * of moving contains among disks.
 */
public class DiskBalancerService extends BackgroundService {

  private static final Logger LOG =
      LoggerFactory.getLogger(DiskBalancerService.class);

  public static final String DISK_BALANCER_DIR = "diskBalancer";
  private static long replicaDeletionDelayMills = 60 * 60 * 1000L; // 60 minutes

  private OzoneContainer ozoneContainer;
  private final ConfigurationSource conf;

  private double threshold;
  private long bandwidthInMB;
  private int parallelThread;
  private boolean stopAfterDiskEven;
  private DiskBalancerVersion version;

  // State field using proto enum
  private volatile DiskBalancerRunningStatus operationalState =
      DiskBalancerRunningStatus.STOPPED;

  private AtomicLong totalBalancedBytes = new AtomicLong(0L);
  private AtomicLong balancedBytesInLastWindow = new AtomicLong(0L);
  private AtomicLong nextAvailableTime = new AtomicLong(Time.monotonicNow());

  private Set<ContainerID> inProgressContainers;
  private ConcurrentSkipListMap<Long, Container> pendingDeletionContainers = new ConcurrentSkipListMap();
  private static FaultInjector injector;

  /**
   * A map that tracks the total bytes which will be freed from each source volume
   * during container moves in the current disk balancing cycle.
   *
   * Unlike committedBytes, which is used for pre-allocating space on
   * destination volumes, deltaSizes helps track how many space will be
   * freed on the source volumes without modifying their
   * committedBytes (which could otherwise go negative).
   */
  private Map<HddsVolume, Long> deltaSizes;
  private MutableVolumeSet volumeSet;

  private DiskBalancerVolumeChoosingPolicy volumeChoosingPolicy;
  private ContainerChoosingPolicy containerChoosingPolicy;
  private final File diskBalancerInfoFile;

  private DiskBalancerServiceMetrics metrics;
  private long containerDefaultSize;

  public DiskBalancerService(OzoneContainer ozoneContainer,
      long serviceCheckInterval, long serviceCheckTimeout, TimeUnit timeUnit,
      int workerSize, ConfigurationSource conf) throws IOException {
    super("DiskBalancerService", serviceCheckInterval, timeUnit, workerSize,
        serviceCheckTimeout);
    this.ozoneContainer = ozoneContainer;
    this.conf = conf;

    String diskBalancerInfoPath = getDiskBalancerInfoPath();
    Objects.requireNonNull(diskBalancerInfoPath);
    diskBalancerInfoFile = new File(diskBalancerInfoPath);

    inProgressContainers = ConcurrentHashMap.newKeySet();
    deltaSizes = new ConcurrentHashMap<>();
    volumeSet = ozoneContainer.getVolumeSet();
    containerDefaultSize = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);

    try {
      volumeChoosingPolicy = VolumeChoosingPolicyFactory.getDiskBalancerPolicy(conf);
      containerChoosingPolicy = (ContainerChoosingPolicy)
          conf.getObject(DiskBalancerConfiguration.class)
              .getContainerChoosingPolicyClass().newInstance();
    } catch (Exception e) {
      LOG.error("Got exception when initializing DiskBalancerService", e);
      throw new IOException(e);
    }

    metrics = DiskBalancerServiceMetrics.create();

    loadDiskBalancerInfo();
  }

  /**
   * Update DiskBalancerService based on new DiskBalancerInfo.
   * @param diskBalancerInfo
   * @throws IOException
   */
  public synchronized void refresh(DiskBalancerInfo diskBalancerInfo) throws IOException {
    applyDiskBalancerInfo(diskBalancerInfo);
  }

  /**
   * Cleans up stale diskBalancer temporary directories on startup.
   *
   * @throws IOException if cleanup fails
   */
  private void cleanupTmpDir() throws IOException {
    for (HddsVolume volume:
        StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList())) {
      Path diskBalancerTmpDir = null;
      try {
        File tmpDir = volume.getTmpDir();
        if (tmpDir != null) {
          // If tmpDir is initialized, use it directly
          diskBalancerTmpDir = tmpDir.toPath().resolve(DISK_BALANCER_DIR);
        } else {
          // If tmpDir is not initialized, construct the path manually
          // This handles the case where stale directories exist from previous
          // failed moves even though volumes haven't been initialized yet
          String clusterId = volume.getClusterID();
          if (clusterId == null) {
            // Skip volumes without clusterID - they're not properly formatted
            continue;
          }
          String workDirName = volume.getWorkingDirName();
          if (workDirName == null) {
            workDirName = clusterId;
          }
          diskBalancerTmpDir = Paths.get(volume.getStorageDir().toString(),
              workDirName, "tmp", DISK_BALANCER_DIR);
        }

        // Clean up any existing diskBalancer directory from previous runs
        if (diskBalancerTmpDir.toFile().exists()) {
          FileUtils.deleteDirectory(diskBalancerTmpDir.toFile());
          LOG.info("Cleaned up stale diskBalancer tmp directory: {}", diskBalancerTmpDir);
        }
      } catch (IOException ex) {
        LOG.warn("Failed to clean up diskBalancer tmp directory under volume {}: {}",
            volume, diskBalancerTmpDir, ex);
      }
    }
  }

  /**
   * If the diskBalancer.info file exists, load the file. If not exists,
   * return the default config.
   * @throws IOException
   */
  private void loadDiskBalancerInfo() throws IOException {
    DiskBalancerInfo diskBalancerInfo;
    try {
      if (diskBalancerInfoFile.exists()) {
        diskBalancerInfo = readDiskBalancerInfoFile(diskBalancerInfoFile);
      } else {
        boolean shouldRunDefault =
            conf.getObject(DiskBalancerConfiguration.class)
                .getDiskBalancerShouldRun();
        diskBalancerInfo = new DiskBalancerInfo(shouldRunDefault,
            new DiskBalancerConfiguration());
      }
    } catch (IOException e) {
      LOG.warn("Can not load diskBalancerInfo from diskBalancer.info file. " +
          "Falling back to default configs", e);
      throw e;
    }

    applyDiskBalancerInfo(diskBalancerInfo);
  }

  private void applyDiskBalancerInfo(DiskBalancerInfo diskBalancerInfo)
      throws IOException {
    // First store in local file, then update in memory variables
    writeDiskBalancerInfoTo(diskBalancerInfo, diskBalancerInfoFile);

    updateOperationalStateFromInfo(diskBalancerInfo);

    setThreshold(diskBalancerInfo.getThreshold());
    setBandwidthInMB(diskBalancerInfo.getBandwidthInMB());
    setParallelThread(diskBalancerInfo.getParallelThread());
    setStopAfterDiskEven(diskBalancerInfo.isStopAfterDiskEven());
    setVersion(diskBalancerInfo.getVersion());

    // Default executorService is ScheduledThreadPoolExecutor, so we can
    // update the poll size by setting corePoolSize.
    if ((getExecutorService() instanceof ScheduledThreadPoolExecutor)) {
      ((ScheduledThreadPoolExecutor) getExecutorService())
          .setCorePoolSize(parallelThread);
    }
  }

  /**
   * Determines the new operational state based on the provided DiskBalancerInfo
   * and updates the service's operationalState if it has changed.
   *
   * @param diskBalancerInfo The DiskBalancerInfo containing shouldRun and paused flags.
   */
  private void updateOperationalStateFromInfo(DiskBalancerInfo diskBalancerInfo) {
    DiskBalancerRunningStatus newOperationalState = diskBalancerInfo.getOperationalState();

    if (this.operationalState != newOperationalState) {
      LOG.info("DiskBalancer operational state changing from {} to {} " +
              "based on DiskBalancerInfo (derived: shouldRun={}, paused={}).",
          this.operationalState, newOperationalState,
          diskBalancerInfo.isShouldRun(), diskBalancerInfo.isPaused());
      this.operationalState = newOperationalState;
    }
  }

  private String getDiskBalancerInfoPath() {
    String diskBalancerInfoDir =
        conf.getObject(DiskBalancerConfiguration.class)
            .getDiskBalancerInfoDir();
    if (Strings.isNullOrEmpty(diskBalancerInfoDir)) {
      File metaDirPath = ServerUtils.getOzoneMetaDirPath(conf);
      if (metaDirPath == null) {
        // this means meta data is not found, in theory should not happen at
        // this point because should've failed earlier.
        throw new IllegalArgumentException("Unable to locate meta data" +
            "directory when getting datanode disk balancer file path");
      }
      diskBalancerInfoDir = metaDirPath.toString();
    }
    // Use default datanode disk balancer file name for file path
    return new File(diskBalancerInfoDir,
        OzoneConsts.OZONE_SCM_DATANODE_DISK_BALANCER_INFO_FILE_DEFAULT)
        .toString();
  }

  /**
   * Read {@link DiskBalancerInfo} from a local info file.
   *
   * @param path DiskBalancerInfo file local path
   * @return {@link DatanodeDetails}
   * @throws IOException If the conf file is malformed or other I/O exceptions
   */
  private synchronized DiskBalancerInfo readDiskBalancerInfoFile(
      File path) throws IOException {
    if (!path.exists()) {
      throw new IOException("DiskBalancerConf file not found.");
    }
    try {
      return DiskBalancerYaml.readDiskBalancerInfoFile(path);
    } catch (IOException e) {
      LOG.warn("Error loading DiskBalancerInfo yaml from {}",
          path.getAbsolutePath(), e);
      throw new IOException("Failed to parse DiskBalancerInfo from "
          + path.getAbsolutePath(), e);
    }
  }

  /**
   * Persistent a {@link DiskBalancerInfo} to a local file.
   *
   * @throws IOException when read/write error occurs
   */
  private synchronized void writeDiskBalancerInfoTo(
      DiskBalancerInfo diskBalancerInfo, File path)
      throws IOException {
    if (path.exists()) {
      if (!path.delete() || !path.createNewFile()) {
        throw new IOException("Unable to overwrite the DiskBalancerInfo file.");
      }
    } else {
      if (!path.getParentFile().exists() &&
          !path.getParentFile().mkdirs()) {
        throw new IOException("Unable to create DiskBalancerInfo directories.");
      }
    }
    DiskBalancerYaml.createDiskBalancerInfoFile(diskBalancerInfo, path);
  }

  public void setThreshold(double threshold) {
    this.threshold = threshold;
  }

  public void setBandwidthInMB(long bandwidthInMB) {
    this.bandwidthInMB = bandwidthInMB;
  }

  public void setParallelThread(int parallelThread) {
    this.parallelThread = parallelThread;
  }

  public void setStopAfterDiskEven(boolean stopAfterDiskEven) {
    this.stopAfterDiskEven = stopAfterDiskEven;
  }

  public void setVersion(DiskBalancerVersion version) {
    this.version = version;
  }

  @Override
  public synchronized void start() {
    // Clean up any stale diskBalancer tmp directories from previous runs
    try {
      cleanupTmpDir();
    } catch (IOException e) {
      LOG.warn("Failed to clean up diskBalancer tmp directories before starting service", e);
    }
    super.start();
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();

    if (this.operationalState == DiskBalancerRunningStatus.STOPPED ||
        this.operationalState == DiskBalancerRunningStatus.PAUSED) {
      cleanupPendingDeletionContainers();
      return queue;
    }
    metrics.incrRunningLoopCount();

    if (shouldDelay()) {
      metrics.incrIdleLoopExceedsBandwidthCount();
      cleanupPendingDeletionContainers();
      return queue;
    }

    int availableTaskCount = parallelThread - inProgressContainers.size();
    if (availableTaskCount <= 0) {
      LOG.info("No available thread for disk balancer service. " +
          "Current thread count is {}.", parallelThread);
      return queue;
    }

    for (int i = 0; i < availableTaskCount; i++) {
      Pair<HddsVolume, HddsVolume> pair = volumeChoosingPolicy
          .chooseVolume(volumeSet, threshold, deltaSizes, containerDefaultSize);
      if (pair == null) {
        continue;
      }
      HddsVolume sourceVolume = pair.getLeft(), destVolume = pair.getRight();
      ContainerData toBalanceContainer = containerChoosingPolicy
          .chooseContainer(ozoneContainer, sourceVolume, destVolume,
              inProgressContainers, threshold, volumeSet, deltaSizes);
      if (toBalanceContainer != null) {
        DiskBalancerTask task = new DiskBalancerTask(toBalanceContainer, sourceVolume,
            destVolume);
        queue.add(task);
        inProgressContainers.add(ContainerID.valueOf(toBalanceContainer.getContainerID()));
        deltaSizes.put(sourceVolume, deltaSizes.getOrDefault(sourceVolume, 0L)
            - toBalanceContainer.getBytesUsed());
      } else {
        // release destVolume committed bytes
        destVolume.incCommittedBytes(0 - containerDefaultSize);
      }
    }

    if (queue.isEmpty() && inProgressContainers.isEmpty()) {
      if (stopAfterDiskEven) {
        LOG.info("Disk balancer is stopped due to disk even as" +
            " the property StopAfterDiskEven is set to true.");
        this.operationalState = DiskBalancerRunningStatus.STOPPED;
        try {
          // Persist the updated shouldRun status into the YAML file
          writeDiskBalancerInfoTo(getDiskBalancerInfo(), diskBalancerInfoFile);
        } catch (IOException e) {
          LOG.warn("Failed to persist updated DiskBalancerInfo to file.", e);
        }
      }
      metrics.incrIdleLoopNoAvailableVolumePairCount();
      cleanupPendingDeletionContainers();
    }

    return queue;
  }

  private boolean shouldDelay() {
    // We should wait for next AvailableTime.
    if (Time.monotonicNow() <= nextAvailableTime.get()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping balancing until nextAvailableTime ({} ms)",
            (nextAvailableTime.get() - Time.monotonicNow()));
      }
      return true;
    }
    // Calculate the next AvailableTime based on bandwidth
    long bytesBalanced = balancedBytesInLastWindow.getAndSet(0L);
    final int megaByte = 1024 * 1024;

    // converting disk bandwidth in byte/millisec
    float bytesPerMillisec = bandwidthInMB * megaByte / 1000f;
    long delayInMillisec = (long) (bytesBalanced / bytesPerMillisec);
    nextAvailableTime.set(Time.monotonicNow() + delayInMillisec);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Bytes balanced: {} MB, Calculated delay: {} ms ({} sec)",
          bytesBalanced / megaByte, delayInMillisec, delayInMillisec / 1000);
    }
    return false;
  }

  protected class DiskBalancerTask implements BackgroundTask {

    private HddsVolume sourceVolume;
    private HddsVolume destVolume;
    private ContainerData containerData;

    DiskBalancerTask(ContainerData containerData,
        HddsVolume sourceVolume, HddsVolume destVolume) {
      this.containerData = containerData;
      this.sourceVolume = sourceVolume;
      this.destVolume = destVolume;
    }

    @Override
    public BackgroundTaskResult call() {
      long startTime = Time.monotonicNow();
      boolean moveSucceeded = true;
      long containerId = containerData.getContainerID();
      Container container = ozoneContainer.getContainerSet().getContainer(containerId);
      boolean readLockReleased = false;
      Path diskBalancerTmpDir = null, diskBalancerDestDir = null;
      long containerSize = containerData.getBytesUsed();
      if (container == null) {
        LOG.warn("Container " + containerId + " doesn't exist in ContainerSet");
        postCall(false, startTime);
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }
      // hold read lock on the container first, to avoid other threads to update the container state,
      // such as block deletion.
      container.readLock();
      try {
        // Step 1: Copy container to new Volume's tmp Dir
        diskBalancerTmpDir = getDiskBalancerTmpDir(destVolume)
            .resolve(String.valueOf(containerId));
        ozoneContainer.getController().copyContainer(containerData, diskBalancerTmpDir);

        // Step 2: verify checksum and Transition Temp container to Temp C1-RECOVERING
        File tempContainerFile = ContainerUtils.getContainerFile(diskBalancerTmpDir.toFile());
        if (!tempContainerFile.exists()) {
          throw new IOException("ContainerFile for container " + containerId
              + " doesn't exist in temp directory " + tempContainerFile.getAbsolutePath());
        }
        ContainerData tempContainerData = ContainerDataYaml.readContainerFile(tempContainerFile);
        ContainerUtils.verifyContainerFileChecksum(tempContainerData, conf);
        // Before move the container directory to final place, the destination dir is empty and doesn't have
        // a metadata directory. Writing the .container file will fail as the metadata dir doesn't exist.
        // So we instead save the container file to the diskBalancerTmpDir.
        ContainerProtos.ContainerDataProto.State originalState = tempContainerData.getState();
        tempContainerData.setState(ContainerProtos.ContainerDataProto.State.RECOVERING);
        // update tempContainerData volume to point to destVolume
        tempContainerData.setVolume(destVolume);
        // overwrite the .container file with the new state.
        ContainerDataYaml.createContainerFile(tempContainerData, tempContainerFile);
        // reset to original state
        tempContainerData.setState(originalState);

        // Step 3: Move container directory to final place on new volume and import
        String idDir = VersionedDatanodeFeatures.ScmHA.chooseContainerPathID(
            destVolume, destVolume.getClusterID());
        diskBalancerDestDir =
            Paths.get(KeyValueContainerLocationUtil.getBaseContainerLocation(
                destVolume.getHddsRootDir().toString(), idDir,
                containerData.getContainerID()));
        if (!Files.exists(diskBalancerDestDir)) {
          Files.createDirectories(diskBalancerDestDir);
        }

        if (FileUtils.isEmptyDirectory(diskBalancerDestDir.toFile())) {
          Files.move(diskBalancerTmpDir, diskBalancerDestDir,
              StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } else {
          String errorMessage = "Container " + containerId +
              " move failed because Container Directory " +
              diskBalancerDestDir.toAbsolutePath() + " already exists and are not empty";
          throw new StorageContainerException(errorMessage, CONTAINER_ALREADY_EXISTS);
        }

        // Import the container. importContainer will reset container back to original state
        Container newContainer = ozoneContainer.getController().importContainer(tempContainerData);

        // Step 4: Update container for containerID and mark old container for deletion
        // first, update the in-memory set to point to the new replica.
        // After this update, new caller will get the new Container object,
        // old caller can still hold the old Container object.
        ozoneContainer.getContainerSet().updateContainer(newContainer);
        destVolume.incrementUsedSpace(containerSize);
        // Mark old container as DELETED and persist state.
        // markContainerForDelete require writeLock, so release readLock first
        container.readUnlock();
        readLockReleased = true;
        try {
          container.markContainerForDelete();
        } catch (Throwable e) {
          // mark container for deleted failure will not fail the whole process, it will leave both old and new replica
          // on disk, while new container in the ContainerSet.
          LOG.warn("Failed to mark the old container {} for delete. " +
              "It will be handled after DN restart.", containerId, e);
        }
        // The move is now successful.
        balancedBytesInLastWindow.addAndGet(containerSize);
        metrics.incrSuccessBytes(containerSize);
        totalBalancedBytes.addAndGet(containerSize);
      } catch (IOException e) {
        if (injector != null) {
          try {
            injector.pause();
          } catch (IOException ex) {
            // do nothing
          }
        }
        moveSucceeded = false;
        LOG.warn("Failed to move container {}", containerId, e);
        if (diskBalancerTmpDir != null) {
          try {
            File dir = new File(String.valueOf(diskBalancerTmpDir));
            FileUtils.deleteDirectory(dir);
          } catch (IOException ex) {
            LOG.warn("Failed to delete tmp directory {}", diskBalancerTmpDir, ex);
          }
        }
        if (diskBalancerDestDir != null && e instanceof StorageContainerException
            && ((StorageContainerException) e).getResult() != CONTAINER_ALREADY_EXISTS) {
          try {
            File dir = new File(String.valueOf(diskBalancerDestDir));
            FileUtils.deleteDirectory(dir);
          } catch (IOException ex) {
            LOG.warn("Failed to delete dest directory {}", diskBalancerDestDir, ex);
          }
        }
      } finally {
        if (!readLockReleased) {
          container.readUnlock();
        }
        if (moveSucceeded) {
          // Add current old container to pendingDeletionContainers.
          pendingDeletionContainers.put(System.currentTimeMillis() + replicaDeletionDelayMills, container);
          ContainerLogger.logMoveSuccess(containerId, sourceVolume,
              destVolume, containerSize, Time.monotonicNow() - startTime);
        }
        postCall(moveSucceeded, startTime);

        // pick one expired container from pendingDeletionContainers to delete
        tryCleanupOnePendingDeletionContainer();
      }
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }

    @Override
    public int getPriority() {
      return BackgroundTask.super.getPriority();
    }

    private void postCall(boolean success, long startTime) {
      inProgressContainers.remove(ContainerID.valueOf(containerData.getContainerID()));
      deltaSizes.put(sourceVolume, deltaSizes.get(sourceVolume) +
          containerData.getBytesUsed());
      destVolume.incCommittedBytes(0 - containerDefaultSize);
      long endTime = Time.monotonicNow();
      if (success) {
        metrics.incrSuccessCount(1);
        metrics.getMoveSuccessTime().add(endTime - startTime);
      } else {
        metrics.incrFailureCount(1);
        metrics.getMoveFailureTime().add(endTime - startTime);
      }
    }
  }

  private void deleteContainer(Container container) {
    try {
      KeyValueContainerData containerData = (KeyValueContainerData) container.getContainerData();
      KeyValueContainerUtil.removeContainer(containerData, conf);
      container.delete();
      container.getContainerData().getVolume().decrementUsedSpace(containerData.getBytesUsed());
      LOG.info("Deleted expired container {} after delay {} ms.",
          containerData.getContainerID(), replicaDeletionDelayMills);
    } catch (IOException ex) {
      LOG.warn("Failed to delete old container {} after it's marked as DELETED. " +
          "It will be handled by background scanners.", container.getContainerData().getContainerID(), ex);
    }
  }

  private void cleanupPendingDeletionContainers() {
    // delete all pending deletion containers before stop the service
    boolean ret;
    do {
      ret = tryCleanupOnePendingDeletionContainer();
    } while (ret);
  }

  private boolean tryCleanupOnePendingDeletionContainer() {
    Map.Entry<Long, Container> entry = pendingDeletionContainers.pollFirstEntry();
    if (entry != null) {
      if (entry.getKey() <= System.currentTimeMillis()) {
        // entry container is expired
        deleteContainer(entry.getValue());
        return true;
      } else {
        // put back the container
        pendingDeletionContainers.put(entry.getKey(), entry.getValue());
      }
    }
    return false;
  }

  public DiskBalancerInfo getDiskBalancerInfo() {
    final List<VolumeFixedUsage> volumeUsages = getVolumeUsages(volumeSet, deltaSizes);

    // Calculate volumeDataDensity
    final double volumeDataDensity = calculateVolumeDataDensity(volumeUsages);

    long bytesToMove = 0;
    if (this.operationalState == DiskBalancerRunningStatus.RUNNING) {
      // this calculates live changes in bytesToMove
      // calculate bytes to move if the balancer is in a running state, else 0.
      bytesToMove = calculateBytesToMove(volumeUsages);
    }

    return new DiskBalancerInfo(operationalState, threshold, bandwidthInMB,
        parallelThread, stopAfterDiskEven, version, metrics.getSuccessCount(),
        metrics.getFailureCount(), bytesToMove, metrics.getSuccessBytes(), volumeDataDensity);
  }

  public long calculateBytesToMove(List<VolumeFixedUsage> inputVolumeSet) {
    // If there are no available volumes or only one volume, return 0 bytes to move
    if (inputVolumeSet.isEmpty() || inputVolumeSet.size() < 2) {
      return 0;
    }

    // Calculate actual threshold
    final double actualThreshold = getIdealUsage(inputVolumeSet) + threshold / 100.0;

    long totalBytesToMove = 0;

    // Calculate excess data in overused volumes
    for (VolumeFixedUsage volumeUsage : inputVolumeSet) {
      final SpaceUsageSource.Fixed usage = volumeUsage.getUsage();
      if (usage.getCapacity() == 0) {
        continue;
      }

      final double excess = volumeUsage.getUtilization() - actualThreshold;
      // Only consider volumes that exceed the threshold (source volumes)
      if (excess > 0) {
        // Calculate excess bytes that need to be moved from this volume
        final long excessBytes = (long) (excess * usage.getCapacity());
        totalBytesToMove += Math.max(0, excessBytes);
      }
    }
    return totalBytesToMove;
  }

  private Path getDiskBalancerTmpDir(HddsVolume hddsVolume) throws IOException {
    return hddsVolume.getTmpDir().toPath().resolve(DISK_BALANCER_DIR);
  }

  public DiskBalancerServiceMetrics getMetrics() {
    return metrics;
  }

  @VisibleForTesting
  public void setBalancedBytesInLastWindow(long bytes) {
    this.balancedBytesInLastWindow.set(bytes);
  }

  public ContainerChoosingPolicy getContainerChoosingPolicy() {
    return containerChoosingPolicy;
  }

  public DiskBalancerVolumeChoosingPolicy getVolumeChoosingPolicy() {
    return volumeChoosingPolicy;
  }

  @VisibleForTesting
  public void setVolumeChoosingPolicy(DiskBalancerVolumeChoosingPolicy volumeChoosingPolicy) {
    this.volumeChoosingPolicy = volumeChoosingPolicy;
  }

  @VisibleForTesting
  public void setContainerChoosingPolicy(ContainerChoosingPolicy containerChoosingPolicy) {
    this.containerChoosingPolicy = containerChoosingPolicy;
  }

  @VisibleForTesting
  public Set<ContainerID> getInProgressContainers() {
    return inProgressContainers;
  }

  @VisibleForTesting
  public Map<HddsVolume, Long> getDeltaSizes() {
    return deltaSizes;
  }

  /**
   * Handle state changes for DiskBalancerService.
   */
  public synchronized void nodeStateUpdated(HddsProtos.NodeOperationalState state) {
    DiskBalancerRunningStatus originalServiceState = this.operationalState;
    boolean stateChanged = false;

    if ((state == HddsProtos.NodeOperationalState.DECOMMISSIONING ||
        state == HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE) &&
        this.operationalState == DiskBalancerRunningStatus.RUNNING) {
      LOG.info("Stopping DiskBalancerService as Node state changed to {}.", state);
      this.operationalState = DiskBalancerRunningStatus.PAUSED;
      stateChanged = true;
    } else if (state == HddsProtos.NodeOperationalState.IN_SERVICE &&
        this.operationalState == DiskBalancerRunningStatus.PAUSED) {
      LOG.info("Resuming DiskBalancerService to running state as Node state changed to {}. ", state);
      this.operationalState = DiskBalancerRunningStatus.RUNNING;
      stateChanged = true;
    }

    if (stateChanged) {
      LOG.info("DiskBalancer operational state changed from {} to {} due to Datanode state update . Persisting.",
          originalServiceState, this.operationalState);
      try {
        writeDiskBalancerInfoTo(getDiskBalancerInfo(), diskBalancerInfoFile);
      } catch (IOException e) {
        LOG.error("Failed to persist DiskBalancerInfo after state change in nodeStateUpdated. " +
            "Reverting operational state to {} to maintain consistency.", originalServiceState, e);
        // Revert state on persistence error to keep in-memory state consistent with last known persisted state.
        this.operationalState = originalServiceState;
        LOG.warn("DiskBalancer operational state reverted to {} due to persistence failure.", this.operationalState);
      }
    }
  }

  @Override
  public void shutdown() {
    super.shutdown();
    if (metrics != null) {
      DiskBalancerServiceMetrics.unRegister();
    }
  }

  @VisibleForTesting
  public static void setInjector(FaultInjector instance) {
    injector = instance;
  }

  @VisibleForTesting
  public static void setReplicaDeletionDelayMills(long durationMills) {
    replicaDeletionDelayMills = durationMills;
  }
}
