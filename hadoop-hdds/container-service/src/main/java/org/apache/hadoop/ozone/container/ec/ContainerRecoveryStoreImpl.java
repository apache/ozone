/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.ec;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.keyvalue.impl.BlockManagerImpl;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerFactory;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;

import static org.apache.hadoop.ozone.OzoneConsts.STORAGE_DIR_CHUNKS;

/**
 * Temp store implementation for ec containers under recovery.
 * For each ec container under recovery, we use an "all-or-nothing" policy
 * that each single failed operation will clean up all for this container.
 */
public class ContainerRecoveryStoreImpl implements ContainerRecoveryStore {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerRecoveryStoreImpl.class);

  public static final String RECOVER_DIR = "container-recover";
  public static final String CHUNK_DIR = STORAGE_DIR_CHUNKS;

  private final MutableVolumeSet hddsVolumeSet;
  private final VolumeChoosingPolicy volumeChoosingPolicy;
  private final ConfigurationSource config;

  private BlockManager blockManager;
  private ChunkManager chunkManager;
  private DispatcherContext dispatcherContext;

  // TODO(markgui): We may choose to persist meta into db so as to
  // control memory usage, but with certain throttling on the number of
  // ec containers under recovery concurrently it may not be a big problem.
  // For now we could cache meta of containers under recovery
  // in memory for simplicity and it gives us "Schema Independent"
  // container metadata management.
  private final ContainerRecoveryMetaCache metaCache;

  public ContainerRecoveryStoreImpl(MutableVolumeSet hddsVolumeSet,
      ConfigurationSource conf) throws IOException {
    this.hddsVolumeSet = hddsVolumeSet;
    this.config = conf;
    this.volumeChoosingPolicy = new RoundRobinVolumeChoosingPolicy();
    this.blockManager = new BlockManagerImpl(config);
    this.chunkManager = ChunkManagerFactory.createChunkManager(conf,
        blockManager, hddsVolumeSet);
    this.dispatcherContext = new DispatcherContext.Builder()
        .setStage(DispatcherContext.WriteChunkStage.COMBINED).build();
    this.metaCache = ContainerRecoveryMetaCache.getInstance();

    initialize();
  }

  @Override
  public void writeChunk(KeyValueContainer container, BlockID blockID,
      ChunkInfo chunkInfo, ChunkBuffer data, Checksum checksum,
      boolean last) throws IOException {
    KeyValueContainerData containerData = container.getContainerData();

    Preconditions.checkState(!containerData.getState().equals(State.OPEN));
    Preconditions.checkNotNull(containerData.getSchemaVersion());
    Preconditions.checkState(containerData.getReplicaIndex() != 0);

    try {
      // choose a volume for new container
      chooseVolumeForContainer(container);

      // write chunk data to disk
      writeToChunkFile(container, blockID, chunkInfo, data, last);

      if (checksum != null) {
        computeChecksumForChunk(chunkInfo, checksum, data);
      }
      metaCache.addChunkToBlock(blockID, chunkInfo);
    } catch (DiskOutOfSpaceException e) {
      LOG.error("No volume with enough space to recover container {}",
          containerData.getContainerID());
      cleanupContainerAll(container);
      throw e;
    } catch (IOException e) {
      LOG.error("Write recovered chunk {} for block {} failed",
          chunkInfo.getChunkName(), blockID.getContainerBlockID());
      cleanupContainerAll(container);
      throw e;
    }
  }

  @Override
  public void consolidateContainer(KeyValueContainer container)
      throws IOException {
    KeyValueContainerData containerData = container.getContainerData();

    Preconditions.checkState(!containerData.getState().equals(State.OPEN));
    Preconditions.checkNotNull(containerData.getSchemaVersion());
    Preconditions.checkState(containerData.getReplicaIndex() != 0);

    long containerID = containerData.getContainerID();
    HddsVolume hddsVolume = containerData.getVolume();
    if (hddsVolume == null) {
      throw new IOException("Container chunk files not recovered completely.");
    }

    try {
      initContainerLayout(container);
      populateContainerMeta(container);

      // move the block data files under the container directory
      File chunksSrc = getRecoverChunksDir(hddsVolume, containerID);
      File chunksDst = new File(containerData.getChunksPath());
      Files.move(chunksSrc.toPath(), chunksDst.toPath(),
          StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      LOG.error("Consolidate container {} failed, cleaning it up",
          containerID);
      container.delete();
      throw e;
    } finally {
      cleanupContainerAll(container);
    }
  }

  @Override
  public void cleanupContainerAll(KeyValueContainer container) {
    KeyValueContainerData containerData = container.getContainerData();
    long containerID = containerData.getContainerID();
    HddsVolume hddsVolume = containerData.getVolume();
    if (hddsVolume == null) {
      return;
    }

    metaCache.dropContainerAll(containerID);
    try {
      FileUtils.deleteDirectory(getRecoverContainerDir(
          container.getContainerData().getVolume(), containerID));
    } catch (IOException e) {
      LOG.warn("Failed to cleanup for container {} on volume {}",
          containerID, hddsVolume.getStorageDir(), e);
    }
  }

  private void chooseVolumeForContainer(KeyValueContainer container)
      throws IOException {
    KeyValueContainerData containerData = container.getContainerData();
    if (containerData.getVolume() != null) {
      return;
    }

    hddsVolumeSet.readLock();
    try {
      HddsVolume hddsVolume = volumeChoosingPolicy.chooseVolume(
          StorageVolumeUtil.getHddsVolumesList(hddsVolumeSet.getVolumesList()),
          containerData.getMaxSize());
      containerData.setVolume(hddsVolume);
    } catch (IOException e) {
      LOG.error("No volume chosen for new container {}",
          containerData.getContainerID(), e);
      throw e;
    } finally {
      hddsVolumeSet.readUnlock();
    }
  }

  private void initialize() throws IOException {
    for (HddsVolume hddsVolume : StorageVolumeUtil
        .getHddsVolumesList(hddsVolumeSet.getVolumesList())) {
      File recovDir = getRecoverDir(hddsVolume);
      if (recovDir.exists()) {
        try {
          FileUtils.deleteDirectory(recovDir);
        } catch (IOException e) {
          LOG.warn("Failed to cleanup ec recover dir on volume {}",
              hddsVolume.getStorageDir(), e);
          throw e;
        }
      }
    }
  }

  private void initContainerLayout(KeyValueContainer container)
      throws IOException {
    KeyValueContainerData containerData = container.getContainerData();
    HddsVolume hddsVolume = containerData.getVolume();
    String hddsVolumeDir = hddsVolume.getStorageDir().getAbsolutePath();
    String clusterId = VersionedDatanodeFeatures.ScmHA.chooseContainerPathID(
        hddsVolume, hddsVolume.getClusterID());

    container.populatePathFields(clusterId, hddsVolume, hddsVolumeDir);

    // create container meta structure(directories and db)
    KeyValueContainerUtil.createContainerMetaData(
        containerData.getContainerID(),
        new File(containerData.getMetadataPath()),
        new File(containerData.getChunksPath()),
        containerData.getDbFile(), containerData.getSchemaVersion(), config);

    // create container meta file(.container file)
    container.createContainerFile(container.getContainerFile());
  }

  private void populateContainerMeta(KeyValueContainer container)
      throws IOException {
    populateContainerMetaFromCache(container);
  }

  private void populateContainerMetaFromCache(KeyValueContainer container)
      throws IOException {
    Iterator<BlockData> iter = metaCache.getBlockIterator(
        container.getContainerData().getContainerID());
    while (iter.hasNext()) {
      blockManager.putBlock(container, iter.next());
    }
  }

  private void computeChecksumForChunk(ChunkInfo chunkInfo,
      Checksum checksum, ChunkBuffer data)
      throws IOException {
    try {
      chunkInfo.setChecksumData(checksum.computeChecksum(data));
    } catch (OzoneChecksumException e) {
      LOG.error("Failed to checksum chunk {}", chunkInfo.getChunkName());
      throw e;
    }
  }

  private File getRecoverChunksDir(HddsVolume hddsVolume, long containerID) {
    // e.g. ../container-recover/<containerID>/chunks
    return new File(getRecoverContainerDir(hddsVolume, containerID),
        CHUNK_DIR);
  }

  private File getRecoverContainerDir(HddsVolume hddsVolume,
      long containerID) {
    // e.g. ../container-recover/<containerID>
    return new File(getRecoverDir(hddsVolume), Long.toString(containerID));
  }

  private File getRecoverDir(HddsVolume hddsVolume) {
    // e.g. /data1/hdds/CID-<clusterID>/container-recover
    return new File(new File(hddsVolume.getStorageDir(),
        hddsVolume.getClusterID()), RECOVER_DIR);
  }

  private void writeToChunkFile(KeyValueContainer container,
      BlockID blockID, ChunkInfo chunkInfo, ChunkBuffer data, boolean last)
      throws IOException {
    KeyValueContainerData containerData = container.getContainerData();
    File chunksDir = getRecoverChunksDir(containerData.getVolume(),
        containerData.getContainerID());

    ensureDirs(chunksDir);
    containerData.setChunksPath(chunksDir.getAbsolutePath());

    chunkManager.writeChunk(container, blockID, chunkInfo, data,
        dispatcherContext);
    if (last) {
      chunkManager.finishWriteChunks(container, new BlockData(blockID));
    }
  }

  private void ensureDirs(File dir) throws IOException {
    if (!dir.mkdirs() && !dir.exists()) {
      throw new IOException("Unable to create directories along " +
          dir.getAbsolutePath());
    }
  }

  @VisibleForTesting
  void setBlockManager(BlockManager manager) {
    this.blockManager = manager;
  }

  @VisibleForTesting
  void setChunkManager(ChunkManager manager) {
    this.chunkManager = manager;
  }

  public static String getChunkName(BlockID blockID, int chunkIndex) {
    return blockID.getLocalID() + "_chunk_" + chunkIndex;
  }
}
