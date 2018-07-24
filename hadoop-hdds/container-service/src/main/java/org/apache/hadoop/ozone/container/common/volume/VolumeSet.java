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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.util.RunJar.SHUTDOWN_HOOK_PRIORITY;

import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume.VolumeState;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.InstrumentedLock;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * VolumeSet to manage volumes in a DataNode.
 */
public class VolumeSet {

  private static final Logger LOG = LoggerFactory.getLogger(VolumeSet.class);

  private Configuration conf;

  /**
   * {@link VolumeSet#volumeMap} maintains a map of all active volumes in the
   * DataNode. Each volume has one-to-one mapping with a volumeInfo object.
   */
  private Map<String, HddsVolume> volumeMap;
  /**
   * {@link VolumeSet#failedVolumeMap} maintains a map of volumes which have
   * failed. The keys in this map and {@link VolumeSet#volumeMap} are
   * mutually exclusive.
   */
  private Map<String, HddsVolume> failedVolumeMap;

  /**
   * {@link VolumeSet#volumeStateMap} maintains a list of active volumes per
   * StorageType.
   */
  private EnumMap<StorageType, List<HddsVolume>> volumeStateMap;

  /**
   * Lock to synchronize changes to the VolumeSet. Any update to
   * {@link VolumeSet#volumeMap}, {@link VolumeSet#failedVolumeMap}, or
   * {@link VolumeSet#volumeStateMap} should be done after acquiring this lock.
   */
  private final AutoCloseableLock volumeSetLock;

  private final String datanodeUuid;
  private String clusterID;

  private Runnable shutdownHook;

  public VolumeSet(String dnUuid, Configuration conf)
      throws IOException {
    this(dnUuid, null, conf);
  }

  public VolumeSet(String dnUuid, String clusterID, Configuration conf)
      throws IOException {
    this.datanodeUuid = dnUuid;
    this.clusterID = clusterID;
    this.conf = conf;
    this.volumeSetLock = new AutoCloseableLock(
        new InstrumentedLock(getClass().getName(), LOG,
            new ReentrantLock(true),
            conf.getTimeDuration(
                OzoneConfigKeys.HDDS_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY,
                OzoneConfigKeys.HDDS_WRITE_LOCK_REPORTING_THRESHOLD_MS_DEFAULT,
                TimeUnit.MILLISECONDS),
            conf.getTimeDuration(
                OzoneConfigKeys.HDDS_LOCK_SUPPRESS_WARNING_INTERVAL_MS_KEY,
                OzoneConfigKeys.HDDS_LOCK_SUPPRESS_WARNING_INTERVAL_MS_DEAFULT,
                TimeUnit.MILLISECONDS)));

    initializeVolumeSet();
  }

  // Add DN volumes configured through ConfigKeys to volumeMap.
  private void initializeVolumeSet() throws IOException {
    volumeMap = new ConcurrentHashMap<>();
    failedVolumeMap = new ConcurrentHashMap<>();
    volumeStateMap = new EnumMap<>(StorageType.class);

    Collection<String> rawLocations = conf.getTrimmedStringCollection(
        HDDS_DATANODE_DIR_KEY);
    if (rawLocations.isEmpty()) {
      rawLocations = conf.getTrimmedStringCollection(DFS_DATANODE_DATA_DIR_KEY);
    }
    if (rawLocations.isEmpty()) {
      throw new IllegalArgumentException("No location configured in either "
          + HDDS_DATANODE_DIR_KEY + " or " + DFS_DATANODE_DATA_DIR_KEY);
    }

    for (StorageType storageType : StorageType.values()) {
      volumeStateMap.put(storageType, new ArrayList<HddsVolume>());
    }

    for (String locationString : rawLocations) {
      try {
        StorageLocation location = StorageLocation.parse(locationString);

        HddsVolume hddsVolume = createVolume(location.getUri().getPath(),
            location.getStorageType());

        checkAndSetClusterID(hddsVolume.getClusterID());

        volumeMap.put(hddsVolume.getHddsRootDir().getPath(), hddsVolume);
        volumeStateMap.get(hddsVolume.getStorageType()).add(hddsVolume);
        LOG.info("Added Volume : {} to VolumeSet",
            hddsVolume.getHddsRootDir().getPath());
      } catch (IOException e) {
        HddsVolume volume = new HddsVolume.Builder(locationString)
            .failedVolume(true).build();
        failedVolumeMap.put(locationString, volume);
        LOG.error("Failed to parse the storage location: " + locationString, e);
      }
    }

    if (volumeMap.size() == 0) {
      throw new DiskOutOfSpaceException("No storage location configured");
    }

    // Ensure volume threads are stopped and scm df is saved during shutdown.
    shutdownHook = () -> {
      shutdown();
    };
    ShutdownHookManager.get().addShutdownHook(shutdownHook,
        SHUTDOWN_HOOK_PRIORITY);
  }

  /**
   * If Version file exists and the {@link VolumeSet#clusterID} is not set yet,
   * assign it the value from Version file. Otherwise, check that the given
   * id matches with the id from version file.
   * @param idFromVersionFile value of the property from Version file
   * @throws InconsistentStorageStateException
   */
  private void checkAndSetClusterID(String idFromVersionFile)
      throws InconsistentStorageStateException {
    // If the clusterID is null (not set), assign it the value
    // from version file.
    if (this.clusterID == null) {
      this.clusterID = idFromVersionFile;
      return;
    }

    // If the clusterID is already set, it should match with the value from the
    // version file.
    if (!idFromVersionFile.equals(this.clusterID)) {
      throw new InconsistentStorageStateException(
          "Mismatched ClusterIDs. VolumeSet has: " + this.clusterID +
              ", and version file has: " + idFromVersionFile);
    }
  }

  public void acquireLock() {
    volumeSetLock.acquire();
  }

  public void releaseLock() {
    volumeSetLock.release();
  }

  private HddsVolume createVolume(String locationString,
      StorageType storageType) throws IOException {
    HddsVolume.Builder volumeBuilder = new HddsVolume.Builder(locationString)
        .conf(conf)
        .datanodeUuid(datanodeUuid)
        .clusterID(clusterID)
        .storageType(storageType);
    return volumeBuilder.build();
  }


  // Add a volume to VolumeSet
  public boolean addVolume(String dataDir) {
    return addVolume(dataDir, StorageType.DEFAULT);
  }

  // Add a volume to VolumeSet
  public boolean addVolume(String volumeRoot, StorageType storageType) {
    String hddsRoot = HddsVolumeUtil.getHddsRoot(volumeRoot);
    boolean success;

    try (AutoCloseableLock lock = volumeSetLock.acquire()) {
      if (volumeMap.containsKey(hddsRoot)) {
        LOG.warn("Volume : {} already exists in VolumeMap", hddsRoot);
        success = false;
      } else {
        if (failedVolumeMap.containsKey(hddsRoot)) {
          failedVolumeMap.remove(hddsRoot);
        }

        HddsVolume hddsVolume = createVolume(volumeRoot, storageType);
        volumeMap.put(hddsVolume.getHddsRootDir().getPath(), hddsVolume);
        volumeStateMap.get(hddsVolume.getStorageType()).add(hddsVolume);

        LOG.info("Added Volume : {} to VolumeSet",
            hddsVolume.getHddsRootDir().getPath());
        success = true;
      }
    } catch (IOException ex) {
      LOG.error("Failed to add volume " + volumeRoot + " to VolumeSet", ex);
      success = false;
    }
    return success;
  }

  // Mark a volume as failed
  public void failVolume(String dataDir) {
    String hddsRoot = HddsVolumeUtil.getHddsRoot(dataDir);

    try (AutoCloseableLock lock = volumeSetLock.acquire()) {
      if (volumeMap.containsKey(hddsRoot)) {
        HddsVolume hddsVolume = volumeMap.get(hddsRoot);
        hddsVolume.failVolume();

        volumeMap.remove(hddsRoot);
        volumeStateMap.get(hddsVolume.getStorageType()).remove(hddsVolume);
        failedVolumeMap.put(hddsRoot, hddsVolume);

        LOG.info("Moving Volume : {} to failed Volumes", hddsRoot);
      } else if (failedVolumeMap.containsKey(hddsRoot)) {
        LOG.info("Volume : {} is not active", hddsRoot);
      } else {
        LOG.warn("Volume : {} does not exist in VolumeSet", hddsRoot);
      }
    }
  }

  // Remove a volume from the VolumeSet completely.
  public void removeVolume(String dataDir) throws IOException {
    String hddsRoot = HddsVolumeUtil.getHddsRoot(dataDir);

    try (AutoCloseableLock lock = volumeSetLock.acquire()) {
      if (volumeMap.containsKey(hddsRoot)) {
        HddsVolume hddsVolume = volumeMap.get(hddsRoot);
        hddsVolume.shutdown();

        volumeMap.remove(hddsRoot);
        volumeStateMap.get(hddsVolume.getStorageType()).remove(hddsVolume);

        LOG.info("Removed Volume : {} from VolumeSet", hddsRoot);
      } else if (failedVolumeMap.containsKey(hddsRoot)) {
        HddsVolume hddsVolume = failedVolumeMap.get(hddsRoot);
        hddsVolume.setState(VolumeState.NON_EXISTENT);

        failedVolumeMap.remove(hddsRoot);
        LOG.info("Removed Volume : {} from failed VolumeSet", hddsRoot);
      } else {
        LOG.warn("Volume : {} does not exist in VolumeSet", hddsRoot);
      }
    }
  }

  public HddsVolume chooseVolume(long containerSize,
      VolumeChoosingPolicy choosingPolicy) throws IOException {
    return choosingPolicy.chooseVolume(getVolumesList(), containerSize);
  }

  public void shutdown() {
    for (HddsVolume hddsVolume : volumeMap.values()) {
      try {
        hddsVolume.shutdown();
      } catch (Exception ex) {
        LOG.error("Failed to shutdown volume : " + hddsVolume.getHddsRootDir(),
            ex);
      }
    }

    if (shutdownHook != null) {
      ShutdownHookManager.get().removeShutdownHook(shutdownHook);
    }
  }

  @VisibleForTesting
  public List<HddsVolume> getVolumesList() {
    return ImmutableList.copyOf(volumeMap.values());
  }

  @VisibleForTesting
  public List<HddsVolume> getFailedVolumesList() {
    return ImmutableList.copyOf(failedVolumeMap.values());
  }

  @VisibleForTesting
  public Map<String, HddsVolume> getVolumeMap() {
    return ImmutableMap.copyOf(volumeMap);
  }

  @VisibleForTesting
  public Map<StorageType, List<HddsVolume>> getVolumeStateMap() {
    return ImmutableMap.copyOf(volumeStateMap);
  }

  public StorageContainerDatanodeProtocolProtos.NodeReportProto getNodeReport()
      throws IOException {
    boolean failed;
    StorageLocationReport[] reports = new StorageLocationReport[volumeMap
        .size() + failedVolumeMap.size()];
    int counter = 0;
    HddsVolume hddsVolume;
    for (Map.Entry<String, HddsVolume> entry : volumeMap.entrySet()) {
      hddsVolume = entry.getValue();
      VolumeInfo volumeInfo = hddsVolume.getVolumeInfo();
      long scmUsed = 0;
      long remaining = 0;
      failed = false;
      try {
        scmUsed = volumeInfo.getScmUsed();
        remaining = volumeInfo.getAvailable();
      } catch (IOException ex) {
        LOG.warn("Failed to get scmUsed and remaining for container " +
            "storage location {}", volumeInfo.getRootDir());
        // reset scmUsed and remaining if df/du failed.
        scmUsed = 0;
        remaining = 0;
        failed = true;
      }

      StorageLocationReport.Builder builder =
          StorageLocationReport.newBuilder();
      builder.setStorageLocation(volumeInfo.getRootDir())
          .setId(hddsVolume.getStorageID())
          .setFailed(failed)
          .setCapacity(hddsVolume.getCapacity())
          .setRemaining(remaining)
          .setScmUsed(scmUsed)
          .setStorageType(hddsVolume.getStorageType());
      StorageLocationReport r = builder.build();
      reports[counter++] = r;
    }
    for (Map.Entry<String, HddsVolume> entry : failedVolumeMap.entrySet()) {
      hddsVolume = entry.getValue();
      StorageLocationReport.Builder builder = StorageLocationReport
          .newBuilder();
      builder.setStorageLocation(hddsVolume.getHddsRootDir()
          .getAbsolutePath()).setId(hddsVolume.getStorageID()).setFailed(true)
          .setCapacity(0).setRemaining(0).setScmUsed(0).setStorageType(
              hddsVolume.getStorageType());
      StorageLocationReport r = builder.build();
      reports[counter++] = r;
    }
    NodeReportProto.Builder nrb = NodeReportProto.newBuilder();
    for (int i = 0; i < reports.length; i++) {
      nrb.addStorageReport(reports[i].getProtoBufMessage());
    }
    return nrb.build();
  }
}