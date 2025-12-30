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

package org.apache.hadoop.ozone.container.ozoneimpl;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.DELETED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.RECOVERING;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.metadata.ContainerCreateInfo;
import org.apache.hadoop.ozone.container.metadata.WitnessedContainerMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to read .container files from Volume and build container map.
 *
 * Layout of the container directory on disk is as follows:
 *
 * <p>../hdds/VERSION
 * <p>{@literal ../hdds/<<clusterUuid>>/current/<<containerDir>>/<<containerID
 * >/metadata/<<containerID>>.container}
 * <p>{@literal ../hdds/<<clusterUuid>>/current/<<containerDir>>/<<containerID
 * >/<<dataPath>>}
 * <p>
 * Some ContainerTypes will have extra metadata other than the .container
 * file. For example, KeyValueContainer will have a .db file. This .db file
 * will also be stored in the metadata folder along with the .container file.
 * <p>
 * {@literal ../hdds/<<clusterUuid>>/current/<<containerDir>>/<<KVcontainerID
 * >/metadata/<<KVcontainerID>>.db}
 * <p>
 * Note that the {@literal <<dataPath>>} is dependent on the ContainerType.
 * For KeyValueContainers, the data is stored in a "chunks" folder. As such,
 * the {@literal <<dataPath>>} layout for KeyValueContainers is:
 * <p>{@literal ../hdds/<<clusterUuid>>/current/<<containerDir>>/<<KVcontainerID
 * >/chunks/<<chunksFile>>}
 *
 */
public class ContainerReader implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(
      ContainerReader.class);
  private HddsVolume hddsVolume;
  private final ContainerSet containerSet;
  private final ConfigurationSource config;
  private final File hddsVolumeDir;
  private final MutableVolumeSet volumeSet;
  private final boolean shouldDelete;

  public ContainerReader(
      MutableVolumeSet volSet, HddsVolume volume, ContainerSet cset,
      ConfigurationSource conf, boolean shouldDelete) {
    Objects.requireNonNull(volume,  "volume == null");
    this.hddsVolume = volume;
    this.hddsVolumeDir = hddsVolume.getHddsRootDir();
    this.containerSet = cset;
    this.config = conf;
    this.volumeSet = volSet;
    this.shouldDelete = shouldDelete;
  }

  @Override
  public void run() {
    try {
      readVolume(hddsVolumeDir);
    } catch (Throwable t) {
      LOG.error("Caught an exception during reading container files" +
          " from Volume {} {}", hddsVolumeDir, t);
      volumeSet.failVolume(hddsVolumeDir.getPath());
    }
  }

  public void readVolume(File hddsVolumeRootDir) {
    Objects.requireNonNull(hddsVolumeRootDir, "hddsVolumeRootDir == null");

    //filtering storage directory
    File[] storageDirs = hddsVolumeRootDir.listFiles(File::isDirectory);

    if (storageDirs == null) {
      LOG.error("IO error for the volume {}, skipped loading",
          hddsVolumeRootDir);
      volumeSet.failVolume(hddsVolumeRootDir.getPath());
      return;
    }

    // If there are no storage dirs yet, the volume needs to be formatted
    // by HddsUtil#checkVolume once we have a cluster ID from SCM. No
    // operations to perform here in that case.
    if (storageDirs.length > 0) {
      File clusterIDDir = new File(hddsVolumeRootDir,
          hddsVolume.getClusterID());
      // The subdirectory we should verify containers within.
      // If this volume was formatted pre SCM HA, this will be the SCM ID.
      // A cluster ID symlink will exist in this case only if this cluster is
      // finalized for SCM HA.
      // If the volume was formatted post SCM HA, this will be the cluster ID.
      File idDir = clusterIDDir;
      if (storageDirs.length == 1 && !clusterIDDir.exists()) {
        // If the one directory is not the cluster ID directory, assume it is
        // the old SCM ID directory used before SCM HA.
        idDir = storageDirs[0];
      } else {
        // There are 1 or more storage directories. We only care about the
        // cluster ID directory.
        if (!clusterIDDir.exists()) {
          LOG.error("Volume {} is in an inconsistent state. Expected " +
              "clusterID directory {} not found.", hddsVolumeRootDir,
              clusterIDDir);
          volumeSet.failVolume(hddsVolumeRootDir.getPath());
          return;
        }
      }

      LOG.info("Start to verify containers on volume {}", hddsVolumeRootDir);
      File currentDir = new File(idDir, Storage.STORAGE_DIR_CURRENT);
      File[] containerTopDirs = currentDir.listFiles();
      if (containerTopDirs != null && containerTopDirs.length > 0) {
        for (File containerTopDir : containerTopDirs) {
          if (containerTopDir.isDirectory()) {
            File[] containerDirs = containerTopDir.listFiles();
            if (containerDirs != null) {
              for (File containerDir : containerDirs) {
                try {
                  File containerFile = ContainerUtils.getContainerFile(
                      containerDir);
                  long containerID =
                      ContainerUtils.getContainerID(containerDir);
                  if (containerFile.exists()) {
                    verifyContainerFile(containerID, containerFile);
                  } else {
                    LOG.error("Missing .container file for ContainerID: {}",
                        containerDir.getName());
                  }
                } catch (Throwable e) {
                  LOG.error("Failed to load container from {}",
                      containerDir.getAbsolutePath(), e);
                }
              }
            }
          }
        }
      }
    }
    LOG.info("Finish verifying containers on volume {}", hddsVolumeRootDir);
  }

  private void verifyContainerFile(long containerID,
                                   File containerFile) {
    try {
      ContainerData containerData = ContainerDataYaml.readContainerFile(
          containerFile);
      if (containerID != containerData.getContainerID()) {
        LOG.error("Invalid ContainerID in file {}. " +
            "Skipping loading of this container.", containerFile);
        return;
      }
      verifyAndFixupContainerData(containerData);
    } catch (IOException ex) {
      LOG.error("Failed to parse ContainerFile for ContainerID: {}",
          containerID, ex);
    }
  }

  /**
   * Verify ContainerData loaded from disk and fix-up stale members.
   * Specifically the in memory values of blockCommitSequenceId, delete related
   * metadata, bytesUsed and block count.
   * @param containerData
   * @throws IOException
   */
  public void verifyAndFixupContainerData(ContainerData containerData)
      throws IOException {
    switch (containerData.getContainerType()) {
    case KeyValueContainer:
      if (!(containerData instanceof KeyValueContainerData)) {
        throw new StorageContainerException("Container File is corrupted. " +
            "ContainerType is KeyValueContainer but cast to " +
            "KeyValueContainerData failed. ",
            ContainerProtos.Result.CONTAINER_METADATA_ERROR);
      }

      KeyValueContainerData kvContainerData = (KeyValueContainerData)
          containerData;
      containerData.setVolume(hddsVolume);
      KeyValueContainerUtil.parseKVContainerData(kvContainerData, config);
      KeyValueContainer kvContainer = new KeyValueContainer(kvContainerData,
          config);
      if (kvContainer.getContainerState() == RECOVERING) {
        if (shouldDelete) {
          // delete Ratis replicated RECOVERING containers
          if (kvContainer.getContainerData().getReplicaIndex() == 0) {
            cleanupContainer(hddsVolume, kvContainer);
          } else {
            kvContainer.markContainerUnhealthy();
            LOG.info("Stale recovering container {} marked UNHEALTHY",
                kvContainerData.getContainerID());
            containerSet.addContainer(kvContainer);
          }
        }
        return;
      } else if (kvContainer.getContainerState() == DELETED) {
        if (shouldDelete) {
          cleanupContainer(hddsVolume, kvContainer);
        }
        return;
      }

      if (!isMatchedLastLoadedECContainer(kvContainer, containerSet.getContainerMetadataStore())) {
        return;
      }

      try {
        containerSet.addContainer(kvContainer);
        // this should be the last step of this block
        containerData.commitSpace();
      } catch (StorageContainerException e) {
        if (e.getResult() != ContainerProtos.Result.CONTAINER_EXISTS) {
          throw e;
        }
        if (shouldDelete) {
          KeyValueContainer existing = (KeyValueContainer) containerSet.getContainer(
              kvContainer.getContainerData().getContainerID());
          boolean swapped = resolveDuplicate(existing, kvContainer);
          if (swapped) {
            existing.getContainerData().releaseCommitSpace();
            kvContainer.getContainerData().commitSpace();
          }
        }
      }
      break;
    default:
      throw new StorageContainerException("Unrecognized ContainerType " +
          containerData.getContainerType(),
          ContainerProtos.Result.UNKNOWN_CONTAINER_TYPE);
    }
  }

  private boolean isMatchedLastLoadedECContainer(
      KeyValueContainer kvContainer, WitnessedContainerMetadataStore containerMetadataStore) throws IOException {
    if (null != containerMetadataStore && kvContainer.getContainerData().getReplicaIndex() != 0) {
      ContainerCreateInfo containerCreateInfo = containerMetadataStore.getContainerCreateInfoTable()
          .get(ContainerID.valueOf(kvContainer.getContainerData().getContainerID()));
      // check for EC container replica index matching if db entry is present for container as last loaded,
      // and ignore loading container if not matched.
      // Ignore matching container replica index -1 in db as no previous replica index
      if (null != containerCreateInfo
          && containerCreateInfo.getReplicaIndex() != ContainerCreateInfo.INVALID_REPLICA_INDEX
          && containerCreateInfo.getReplicaIndex() != kvContainer.getContainerData().getReplicaIndex()) {
        LOG.info("EC Container {} with replica index {} present at path {} is not matched with DB replica index {}," +
                " ignoring the load of the container.",
            kvContainer.getContainerData().getContainerID(),
            kvContainer.getContainerData().getReplicaIndex(),
            kvContainer.getContainerData().getContainerPath(),
            containerCreateInfo.getReplicaIndex());
        return false;
      }
    }
    // return true if not an EC container or entry not present in db or matching replica index
    return true;
  }

  /**
   * Resolve duplicate containers.
   * @param existing
   * @param toAdd
   * @return true if the container was swapped, false otherwise
   * @throws IOException
   */
  private boolean resolveDuplicate(KeyValueContainer existing,
      KeyValueContainer toAdd) throws IOException {
    if (existing.getContainerData().getReplicaIndex() != 0 ||
        toAdd.getContainerData().getReplicaIndex() != 0) {
      // This is an EC container. As EC Containers don't have a BSCID, we can't
      // know which one has the most recent data. Additionally, it is possible
      // for both copies to have a different replica index for the same
      // container. Therefore we just let whatever one is loaded first win AND
      // leave the other one on disk.
      LOG.warn("Container {} is present at {} and at {}. Both are EC " +
              "containers. Leaving both containers on disk.",
          existing.getContainerData().getContainerID(),
          existing.getContainerData().getContainerPath(),
          toAdd.getContainerData().getContainerPath());
      return false;
    }

    long existingBCSID = existing.getBlockCommitSequenceId();
    ContainerProtos.ContainerDataProto.State existingState
        = existing.getContainerState();
    long toAddBCSID = toAdd.getBlockCommitSequenceId();
    ContainerProtos.ContainerDataProto.State toAddState
        = toAdd.getContainerState();

    if (existingState != toAddState) {
      if (existingState == CLOSED) {
        // If we have mis-matched states, always pick a closed one
        LOG.warn("Container {} is present at {} with state CLOSED and at " +
                "{} with state {}. Removing the latter container.",
            existing.getContainerData().getContainerID(),
            existing.getContainerData().getContainerPath(),
            toAdd.getContainerData().getContainerPath(), toAddState);
        KeyValueContainerUtil.removeContainer(toAdd.getContainerData(),
            hddsVolume.getConf());
        return false;
      } else if (toAddState == CLOSED) {
        LOG.warn("Container {} is present at {} with state CLOSED and at " +
                "{} with state {}. Removing the latter container.",
            toAdd.getContainerData().getContainerID(),
            toAdd.getContainerData().getContainerPath(),
            existing.getContainerData().getContainerPath(), existingState);
        swapAndRemoveContainer(existing, toAdd);
        return true;
      }
    }

    if (existingBCSID >= toAddBCSID) {
      // existing is newer or equal, so remove the one we have yet to load.
      LOG.warn("Container {} is present at {} with a newer or equal BCSID " +
          "than at {}. Removing the latter container.",
          existing.getContainerData().getContainerID(),
          existing.getContainerData().getContainerPath(),
          toAdd.getContainerData().getContainerPath());
      KeyValueContainerUtil.removeContainer(toAdd.getContainerData(),
          hddsVolume.getConf());
      return false;
    } else {
      LOG.warn("Container {} is present at {} with a lesser BCSID " +
              "than at {}. Removing the former container.",
          existing.getContainerData().getContainerID(),
          existing.getContainerData().getContainerPath(),
          toAdd.getContainerData().getContainerPath());
      swapAndRemoveContainer(existing, toAdd);
      return true;
    }
  }

  private void swapAndRemoveContainer(KeyValueContainer existing,
      KeyValueContainer toAdd) throws IOException {
    containerSet.removeContainerOnlyFromMemory(existing.getContainerData().getContainerID());
    containerSet.addContainer(toAdd);
    KeyValueContainerUtil.removeContainer(existing.getContainerData(),
        hddsVolume.getConf());
  }

  private void cleanupContainer(
      HddsVolume volume, KeyValueContainer kvContainer) {
    try {
      LOG.info("Finishing delete of container {}.",
          kvContainer.getContainerData().getContainerID());
      // container information from db is removed for V3
      // and container moved to tmp folder
      // then container content removed from tmp folder
      KeyValueContainerUtil.removeContainer(kvContainer.getContainerData(),
          volume.getConf());
      kvContainer.delete();
    } catch (IOException ex) {
      LOG.warn("Failed to remove deleted container {}.",
          kvContainer.getContainerData().getContainerID(), ex);
    }
  }
}
