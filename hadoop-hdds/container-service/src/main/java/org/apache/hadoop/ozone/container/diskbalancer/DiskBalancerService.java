/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.diskbalancer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DiskBalancerReportProto;
import org.apache.hadoop.hdds.scm.storage.DiskBalancerConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.diskbalancer.policy.ContainerChoosingPolicy;
import org.apache.hadoop.ozone.container.diskbalancer.policy.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

  private boolean shouldRun = false;
  private double threshold;
  private long bandwidthInMB;
  private int parallelThread;

  private DiskBalancerVersion version;

  private AtomicLong totalBalancedBytes = new AtomicLong(0L);
  private AtomicLong balancedBytesInLastWindow = new AtomicLong(0L);
  private AtomicLong nextAvailableTime = new AtomicLong(Time.monotonicNow());

  private Map<DiskBalancerTask, Integer> inProgressTasks;
  private Set<Long> inProgressContainers;

  // Every time a container is decided to be moved from Vol A to Vol B,
  // the size will be deducted from Vol A and added to Vol B.
  // This map is used to help calculate the expected storage size after
  // the container balancing finished.
  private Map<HddsVolume, Long> deltaSizes;
  private MutableVolumeSet volumeSet;

  private VolumeChoosingPolicy volumeChoosingPolicy;
  private ContainerChoosingPolicy containerChoosingPolicy;
  private final File diskBalancerInfoFile;

  private DiskBalancerServiceMetrics metrics;

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

    inProgressTasks = new ConcurrentHashMap<>();
    inProgressContainers = ConcurrentHashMap.newKeySet();
    deltaSizes = new ConcurrentHashMap<>();
    volumeSet = ozoneContainer.getVolumeSet();

    try {
      volumeChoosingPolicy = (VolumeChoosingPolicy)
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
  public void refresh(DiskBalancerInfo diskBalancerInfo) throws IOException {
    applyDiskBalancerInfo(diskBalancerInfo);
  }

  private void constructTmpDir() throws IOException {
    for (HddsVolume volume:
        StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList())) {
      Path tmpDir = getDiskBalancerTmpDir(volume);
      try {
        FileUtils.deleteFully(tmpDir);
        FileUtils.createDirectories(tmpDir);
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

    setShouldRun(diskBalancerInfo.isShouldRun());
    setThreshold(diskBalancerInfo.getThreshold());
    setBandwidthInMB(diskBalancerInfo.getBandwidthInMB());
    setParallelThread(diskBalancerInfo.getParallelThread());
    setVersion(diskBalancerInfo.getVersion());

    // Default executorService is ScheduledThreadPoolExecutor, so we can
    // update the poll size by setting corePoolSize.
    if ((getExecutorService() instanceof ScheduledThreadPoolExecutor)) {
      ((ScheduledThreadPoolExecutor) getExecutorService())
          .setCorePoolSize(parallelThread);
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



  public void setShouldRun(boolean shouldRun) {
    this.shouldRun = shouldRun;
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

  public void setVersion(DiskBalancerVersion version) {
    this.version = version;
  }

  public DiskBalancerInfo getDiskBalancerInfo() {
    return new DiskBalancerInfo(shouldRun, threshold, bandwidthInMB,
        parallelThread, version);
  }

  public DiskBalancerReportProto getDiskBalancerReportProto() {
    DiskBalancerReportProto.Builder builder =
        DiskBalancerReportProto.newBuilder();
    return builder.setIsRunning(shouldRun)
        .setBalancedBytes(totalBalancedBytes.get())
        .setDiskBalancerConf(
            HddsProtos.DiskBalancerConfigurationProto.newBuilder()
                .setThreshold(threshold)
                .setDiskBandwidthInMB(bandwidthInMB)
                .setParallelThread(parallelThread)
                .build())
        .build();
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();

    if (!shouldRun) {
      return queue;
    }
    metrics.incrRunningLoopCount();

    if (shouldDelay()) {
      metrics.incrIdleLoopExceedsBandwidthCount();
      return queue;
    }

    int availableTaskCount = parallelThread - inProgressTasks.size();
    if (availableTaskCount <= 0) {
      LOG.info("No available thread for disk balancer service. " +
          "Current thread count is {}.", parallelThread);
      return queue;
    }

    // TODO: Implementation for choose tasks

    if (queue.isEmpty()) {
      metrics.incrIdleLoopNoAvailableVolumePairCount();
    }
    return queue;
  }

  private boolean shouldDelay() {
    // We should wait for next AvailableTime.
    if (Time.monotonicNow() <= nextAvailableTime.get()) {
      return true;
    }
    // Calculate the next AvailableTime based on bandwidth
    long bytesBalanced = balancedBytesInLastWindow.getAndSet(0L);

    final int megaByte = 1024 * 1024;

    // converting disk bandwidth in byte/millisec
    float bytesPerMillisec = bandwidthInMB * megaByte / 1000f;
    nextAvailableTime.set(Time.monotonicNow() +
        ((long) (bytesBalanced / bytesPerMillisec)));
    return false;
  }

  private class DiskBalancerTask implements BackgroundTask {

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
      // TODO: Details of handling tasks
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
      deltaSizes.put(destVolume, deltaSizes.get(destVolume)
          - containerData.getBytesUsed());
    }
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

  public VolumeChoosingPolicy getVolumeChoosingPolicy() {
    return volumeChoosingPolicy;
  }

  @Override
  public void shutdown() {
    super.shutdown();
    if (metrics != null) {
      DiskBalancerServiceMetrics.unRegister();
    }
  }
}
