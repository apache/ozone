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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DiskBalancerReportProto;
import org.apache.hadoop.hdds.scm.storage.DiskBalancerConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.utils.ContainerLogger;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
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

  private static final String DISK_BALANCER_TMP_DIR = "tmp";

  private OzoneContainer ozoneContainer;
  private final ConfigurationSource conf;

  private double threshold;
  private long bandwidthInMB;
  private int parallelThread;
  private boolean stopAfterDiskEven;
  private DiskBalancerVersion version;

  // State field using the new enum
  private volatile DiskBalancerOperationalState operationalState =
      DiskBalancerOperationalState.STOPPED;

  private AtomicLong totalBalancedBytes = new AtomicLong(0L);
  private AtomicLong balancedBytesInLastWindow = new AtomicLong(0L);
  private AtomicLong nextAvailableTime = new AtomicLong(Time.monotonicNow());

  private Set<Long> inProgressContainers;

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
  private long bytesToMove;

  /**
   * Defines the operational states of the DiskBalancerService.
   */
  public enum DiskBalancerOperationalState {
    /**
     * DiskBalancer is stopped and will not run unless explicitly started.
     * This is the initial state, can be set by admin STOP commands,
     * or if the balancer stops itself after disks are even.
     */
    STOPPED,

    /**
     * DiskBalancer is running normally.
     * The service is actively performing disk balancing operations.
     */
    RUNNING,

    /**
     * DiskBalancer was running but is temporarily paused due to node state changes
     * (e.g., node entering maintenance or decommissioning).
     * When the node returns to IN_SERVICE, it can resume to RUNNING state.
     */
    PAUSED_BY_NODE_STATE
  }

  public DiskBalancerService(OzoneContainer ozoneContainer,
      long serviceCheckInterval, long serviceCheckTimeout, TimeUnit timeUnit,
      int workerSize, ConfigurationSource conf) throws IOException {
    super("DiskBalancerService", serviceCheckInterval, timeUnit, workerSize,
        serviceCheckTimeout);
    this.ozoneContainer = ozoneContainer;
    this.conf = conf;

    String diskBalancerInfoPath = getDiskBalancerInfoPath();
    Preconditions.checkNotNull(diskBalancerInfoPath);
    diskBalancerInfoFile = new File(diskBalancerInfoPath);

    inProgressContainers = ConcurrentHashMap.newKeySet();
    deltaSizes = new ConcurrentHashMap<>();
    volumeSet = ozoneContainer.getVolumeSet();

    try {
      volumeChoosingPolicy = (DiskBalancerVolumeChoosingPolicy)
          conf.getObject(DiskBalancerConfiguration.class)
          .getVolumeChoosingPolicyClass().newInstance();
      containerChoosingPolicy = (ContainerChoosingPolicy)
          conf.getObject(DiskBalancerConfiguration.class)
              .getContainerChoosingPolicyClass().newInstance();
    } catch (Exception e) {
      LOG.error("Got exception when initializing DiskBalancerService", e);
      throw new RuntimeException(e);
    }

    metrics = DiskBalancerServiceMetrics.create();

    loadDiskBalancerInfo();

    constructTmpDir();
  }

  /**
   * Update DiskBalancerService based on new DiskBalancerInfo.
   * @param diskBalancerInfo
   * @throws IOException
   */
  public synchronized void refresh(DiskBalancerInfo diskBalancerInfo) throws IOException {
    applyDiskBalancerInfo(diskBalancerInfo);
  }

  private void constructTmpDir() throws IOException {
    for (HddsVolume volume:
        StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList())) {
      Path tmpDir = getDiskBalancerTmpDir(volume);
      try {
        FileUtils.deleteDirectory(tmpDir.toFile());
        FileUtils.forceMkdir(tmpDir.toFile());
      } catch (IOException ex) {
        LOG.warn("Can not reconstruct tmp directory under volume {}", volume,
            ex);
        throw ex;
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
    DiskBalancerOperationalState newOperationalState = diskBalancerInfo.getOperationalState();

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

  public DiskBalancerReportProto getDiskBalancerReportProto() {
    DiskBalancerReportProto.Builder builder =
        DiskBalancerReportProto.newBuilder();
    return builder.setIsRunning(this.operationalState == DiskBalancerOperationalState.RUNNING)
        .setBalancedBytes(totalBalancedBytes.get())
        .setDiskBalancerConf(
            HddsProtos.DiskBalancerConfigurationProto.newBuilder()
                .setThreshold(threshold)
                .setDiskBandwidthInMB(bandwidthInMB)
                .setParallelThread(parallelThread)
                .setStopAfterDiskEven(stopAfterDiskEven)
                .build())
        .build();
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();

    if (this.operationalState == DiskBalancerOperationalState.STOPPED ||
        this.operationalState == DiskBalancerOperationalState.PAUSED_BY_NODE_STATE) {
      return queue;
    }
    metrics.incrRunningLoopCount();

    if (shouldDelay()) {
      metrics.incrIdleLoopExceedsBandwidthCount();
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
          .chooseVolume(volumeSet, threshold, deltaSizes);
      if (pair == null) {
        continue;
      }
      HddsVolume sourceVolume = pair.getLeft(), destVolume = pair.getRight();
      ContainerData toBalanceContainer = containerChoosingPolicy
          .chooseContainer(ozoneContainer, sourceVolume, inProgressContainers);
      if (toBalanceContainer != null) {
        DiskBalancerTask task = new DiskBalancerTask(toBalanceContainer, sourceVolume,
            destVolume);
        queue.add(task);
        inProgressContainers.add(toBalanceContainer.getContainerID());
        deltaSizes.put(sourceVolume, deltaSizes.getOrDefault(sourceVolume, 0L)
            - toBalanceContainer.getBytesUsed());
        destVolume.incCommittedBytes(toBalanceContainer.getBytesUsed());
      }
    }

    if (queue.isEmpty()) {
      bytesToMove = 0;
      if (stopAfterDiskEven) {
        LOG.info("Disk balancer is stopped due to disk even as" +
            " the property StopAfterDiskEven is set to true.");
        this.operationalState = DiskBalancerOperationalState.STOPPED;
        try {
          // Persist the updated shouldRun status into the YAML file
          writeDiskBalancerInfoTo(getDiskBalancerInfo(), diskBalancerInfoFile);
        } catch (IOException e) {
          LOG.warn("Failed to persist updated DiskBalancerInfo to file.", e);
        }
      }
      metrics.incrIdleLoopNoAvailableVolumePairCount();
    } else {
      bytesToMove = calculateBytesToMove(volumeSet);
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
      boolean destVolumeIncreased = false;
      Path diskBalancerTmpDir = null, diskBalancerDestDir = null;
      long containerSize = containerData.getBytesUsed();
      String originalContainerChecksum = containerData.getContainerFileChecksum();
      try {
        // Step 1: Copy container to new Volume's tmp Dir
        diskBalancerTmpDir = destVolume.getTmpDir().toPath()
            .resolve(DISK_BALANCER_DIR).resolve(String.valueOf(containerId));
        ozoneContainer.getController().copyContainer(containerData,
            diskBalancerTmpDir);

        // Step 2: verify checksum and Transition Temp container to Temp C1-RECOVERING
        File tempContainerFile = ContainerUtils.getContainerFile(
            diskBalancerTmpDir.toFile());
        if (!tempContainerFile.exists()) {
          throw new IOException("ContainerFile for container " + containerId
              + " doesn't exist in temp directory "
              + tempContainerFile.getAbsolutePath());
        }
        ContainerData tempContainerData = ContainerDataYaml
            .readContainerFile(tempContainerFile);
        String copiedContainerChecksum = tempContainerData.getContainerFileChecksum();
        if (!originalContainerChecksum.equals(copiedContainerChecksum)) {
          throw new IOException("Container checksum mismatch for container "
              + containerId + ". Original: " + originalContainerChecksum
              + ", Copied: " + copiedContainerChecksum);
        }
        tempContainerData.setState(ContainerProtos.ContainerDataProto.State.RECOVERING);

        // overwrite the .container file with the new state.
        ContainerDataYaml.createContainerFile(tempContainerData, tempContainerFile);

        // Step 3: Move container directory to final place on new volume
        String idDir = VersionedDatanodeFeatures.ScmHA.chooseContainerPathID(
            destVolume, destVolume.getClusterID());
        diskBalancerDestDir =
            Paths.get(KeyValueContainerLocationUtil.getBaseContainerLocation(
                destVolume.getHddsRootDir().toString(), idDir,
                containerData.getContainerID()));
        Path destDirParent = diskBalancerDestDir.getParent();
        if (destDirParent != null) {
          Files.createDirectories(destDirParent);
        }
        Files.move(diskBalancerTmpDir, diskBalancerDestDir,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);

        // Generate a new Container based on destDir which is in C1-RECOVERING state.
        File containerFile = ContainerUtils.getContainerFile(
            diskBalancerDestDir.toFile());
        if (!containerFile.exists()) {
          throw new IOException("ContainerFile for container " + containerId
          + " doesn't exists.");
        }
        ContainerData originalContainerData = ContainerDataYaml
            .readContainerFile(containerFile);
        Container newContainer = ozoneContainer.getController()
            .importContainer(originalContainerData, diskBalancerDestDir);
        newContainer.getContainerData().getVolume()
            .incrementUsedSpace(containerSize);
        destVolumeIncreased = true;

        // The import process loaded the temporary RECOVERING state from disk.
        // Now, restore the original state and persist it to the .container file.
        newContainer.getContainerData().setState(containerData.getState());
        newContainer.update(newContainer.getContainerData().getMetadata(), true);

        // Step 5: Update container for containerID and delete old container.
        Container oldContainer = ozoneContainer.getContainerSet()
            .getContainer(containerId);
        oldContainer.writeLock();
        try {
          // First, update the in-memory set to point to the new replica.
          ozoneContainer.getContainerSet().updateContainer(newContainer);

          // Mark old container as DELETED and persist state.
          oldContainer.getContainerData().setState(
              ContainerProtos.ContainerDataProto.State.DELETED);
          oldContainer.update(oldContainer.getContainerData().getMetadata(),
              true);

          // Remove the old container from the KeyValueContainerUtil.
          try {
            KeyValueContainerUtil.removeContainer(
                (KeyValueContainerData) oldContainer.getContainerData(), conf);
            oldContainer.delete();
          } catch (IOException ex) {
            LOG.warn("Failed to cleanup old container {} after move. It is " +
                    "marked DELETED and will be handled by background scanners.",
                containerId, ex);
          }
        } finally {
          oldContainer.writeUnlock();
        }

        //The move is now successful.
        oldContainer.getContainerData().getVolume()
            .decrementUsedSpace(containerSize);
        balancedBytesInLastWindow.addAndGet(containerSize);
        metrics.incrSuccessCount(1);
        metrics.incrSuccessBytes(containerSize);
        totalBalancedBytes.addAndGet(containerSize);
      } catch (IOException e) {
        moveSucceeded = false;
        LOG.warn("Failed to move container {}", containerId, e);
        if (diskBalancerTmpDir != null) {
          try {
            File dir = new File(String.valueOf(diskBalancerTmpDir));
            FileUtils.deleteDirectory(dir);
          } catch (IOException ex) {
            LOG.warn("Failed to delete tmp directory {}", diskBalancerTmpDir,
                ex);
          }
        }
        if (diskBalancerDestDir != null) {
          try {
            File dir = new File(String.valueOf(diskBalancerDestDir));
            FileUtils.deleteDirectory(dir);
          } catch (IOException ex) {
            LOG.warn("Failed to delete dest directory {}",
                diskBalancerDestDir, ex);
          }
        }
        // Only need to check for destVolume, sourceVolume's usedSpace is
        // updated at last, if it reaches there, there is no exception.
        if (destVolumeIncreased) {
          destVolume.decrementUsedSpace(containerSize);
        }
        metrics.incrFailureCount();
      } finally {
        long endTime = Time.monotonicNow();
        if (moveSucceeded) {
          metrics.getMoveSuccessTime().add(endTime - startTime);
          ContainerLogger.logMoveSuccess(containerId, sourceVolume,
              destVolume, containerSize, endTime - startTime);
        } else {
          metrics.getMoveFailureTime().add(endTime - startTime);
        }
        postCall();
      }
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }

    @Override
    public int getPriority() {
      return BackgroundTask.super.getPriority();
    }

    private void postCall() {
      inProgressContainers.remove(containerData.getContainerID());
      deltaSizes.put(sourceVolume, deltaSizes.get(sourceVolume) +
          containerData.getBytesUsed());
      destVolume.incCommittedBytes(-containerData.getBytesUsed());
    }
  }

  public DiskBalancerInfo getDiskBalancerInfo() {
    return new DiskBalancerInfo(operationalState, threshold, bandwidthInMB,
        parallelThread, stopAfterDiskEven, version, metrics.getSuccessCount(),
        metrics.getFailureCount(), bytesToMove, metrics.getSuccessBytes());
  }

  public long calculateBytesToMove(MutableVolumeSet inputVolumeSet) {
    long bytesPendingToMove = 0;
    long totalFreeSpace = 0;
    long totalCapacity = 0;

    for (HddsVolume volume : StorageVolumeUtil.getHddsVolumesList(inputVolumeSet.getVolumesList())) {
      totalFreeSpace += volume.getCurrentUsage().getAvailable();
      totalCapacity += volume.getCurrentUsage().getCapacity();
    }

    if (totalCapacity == 0) {
      return 0;
    }

    double datanodeUtilization = ((double) (totalCapacity - totalFreeSpace)) / totalCapacity;

    double thresholdFraction = threshold / 100.0;
    double upperLimit = datanodeUtilization + thresholdFraction;

    // Calculate excess data in overused volumes
    for (HddsVolume volume : StorageVolumeUtil.getHddsVolumesList(inputVolumeSet.getVolumesList())) {
      long freeSpace = volume.getCurrentUsage().getAvailable();
      long capacity = volume.getCurrentUsage().getCapacity();
      double volumeUtilization = ((double) (capacity - freeSpace)) / capacity;

      // Consider only volumes exceeding the upper threshold
      if (volumeUtilization > upperLimit) {
        long excessData = (capacity - freeSpace) - (long) (upperLimit * capacity);
        bytesPendingToMove += excessData;
      }
    }
    return bytesPendingToMove;
  }

  private Path getDiskBalancerTmpDir(HddsVolume hddsVolume) {
    return Paths.get(hddsVolume.getVolumeRootDir())
        .resolve(DISK_BALANCER_TMP_DIR).resolve(DISK_BALANCER_DIR);
  }

  public boolean isBalancingContainer(long containerId) {
    return inProgressContainers.contains(containerId);
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
  public DiskBalancerTask createDiskBalancerTask(ContainerData containerData, HddsVolume source, HddsVolume dest) {
    inProgressContainers.add(containerData.getContainerID());
    deltaSizes.put(source, deltaSizes.getOrDefault(source, 0L)
        - containerData.getBytesUsed());
    dest.incCommittedBytes(containerData.getBytesUsed());
    return new DiskBalancerTask(containerData, source, dest);
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
  public Set<Long> getInProgressContainers() {
    return inProgressContainers;
  }

  /**
   * Handle state changes for DiskBalancerService.
   */
  public synchronized void nodeStateUpdated(HddsProtos.NodeOperationalState state) {
    DiskBalancerOperationalState originalServiceState = this.operationalState;
    boolean stateChanged = false;

    if ((state == HddsProtos.NodeOperationalState.DECOMMISSIONING ||
        state == HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE) &&
        this.operationalState == DiskBalancerOperationalState.RUNNING) {
      LOG.info("Stopping DiskBalancerService as Node state changed to {}.", state);
      this.operationalState = DiskBalancerOperationalState.PAUSED_BY_NODE_STATE;
      stateChanged = true;
    } else if (state == HddsProtos.NodeOperationalState.IN_SERVICE &&
        this.operationalState == DiskBalancerOperationalState.PAUSED_BY_NODE_STATE) {
      LOG.info("Resuming DiskBalancerService to running state as Node state changed to {}. ", state);
      this.operationalState = DiskBalancerOperationalState.RUNNING;
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
}
