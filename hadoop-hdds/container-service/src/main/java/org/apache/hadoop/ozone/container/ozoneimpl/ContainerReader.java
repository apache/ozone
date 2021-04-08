/*
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

package org.apache.hadoop.ozone.container.ozoneimpl;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import com.google.common.base.Preconditions;
import org.apache.hadoop.io.nativeio.NativeIO;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

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
  private final boolean isInUpgradeMode;

  public ContainerReader(
      MutableVolumeSet volSet, HddsVolume volume, ContainerSet cset,
      ConfigurationSource conf
  ) {
    Preconditions.checkNotNull(volume);
    this.hddsVolume = volume;
    this.hddsVolumeDir = hddsVolume.getHddsRootDir();
    this.containerSet = cset;
    this.config = conf;
    this.volumeSet = volSet;
    this.isInUpgradeMode =
        conf.getBoolean(ScmConfigKeys.HDDS_DATANODE_UPGRADE_LAYOUT_INLINE,
            ScmConfigKeys.HDDS_DATANODE_UPGRADE_LAYOUT_INLINE_DEFAULT);
    LOG.info("Running in upgrade mode:{}", this.isInUpgradeMode);
  }


  private File getClusterDir() {
    File hddsVolumeRootDir = hddsVolume.getHddsRootDir();
    return new File(hddsVolumeRootDir, hddsVolume.getClusterID());
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

  public void readVolume(File hddsVolumeRootDir) throws Exception {
    Preconditions.checkNotNull(hddsVolumeRootDir, "hddsVolumeRootDir" +
        "cannot be null");

    //filtering storage directory
    File[] storageDir = hddsVolumeRootDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.isDirectory();
      }
    });

    if (storageDir == null) {
      LOG.error("IO error for the volume {}, skipped loading",
          hddsVolumeRootDir);
      volumeSet.failVolume(hddsVolumeRootDir.getPath());
      return;
    }

    if (storageDir.length > 1) {
      LOG.error("Volume {} is in Inconsistent state", hddsVolumeRootDir);
      volumeSet.failVolume(hddsVolumeRootDir.getPath());
      return;
    }

    LOG.info("Start to verify containers on volume {}", hddsVolumeRootDir);
    for (File storageLoc : storageDir) {
      File location = preProcessStorageLoc(storageLoc);
      File currentDir = new File(location, Storage.STORAGE_DIR_CURRENT);
      File[] containerTopDirs = currentDir.listFiles();
      if (containerTopDirs != null) {
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
                    verifyContainerFile(storageLoc, containerID, containerFile);
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

  public File preProcessStorageLoc(File storageLoc) throws Exception {
    File clusterDir = getClusterDir();

    if (!isInUpgradeMode) {
      Preconditions.checkArgument(clusterDir.exists(),
          "Storage Dir:" + clusterDir + " doesn't exists");
      Preconditions.checkArgument(storageLoc.equals(clusterDir),
          "configured storage location path" + storageLoc +
              " does not container the clusterId:" +
              hddsVolume.getClusterID());
      return storageLoc;
    }

    if (clusterDir.exists()) {
      return storageLoc;
    }

    try {
      LOG.info("Storage dir based on clusterId doesn't exists." +
          "Renaming storage location:{} to {}", storageLoc, clusterDir);
      NativeIO.renameTo(storageLoc, clusterDir);
      return clusterDir;
    } catch (Throwable t) {
      LOG.error("DN Layout upgrade failed. Renaming of storage" +
          "location:{} to {} failed", storageLoc, clusterDir, t);
      throw t;
    }
  }

  private void verifyContainerFile(File storageLoc, long containerID,
                                   File containerFile) {
    try {
      ContainerData containerData = ContainerDataYaml.readContainerFile(
          containerFile);
      if (containerID != containerData.getContainerID()) {
        LOG.error("Invalid ContainerID in file {}. " +
            "Skipping loading of this container.", containerFile);
        return;
      }
      verifyAndFixupContainerData(storageLoc, containerData);
    } catch (IOException ex) {
      LOG.error("Failed to parse ContainerFile for ContainerID: {}",
          containerID, ex);
    }
  }

  /**
   * This function upgrades the container layout in following steps.
   * a) Converts the chunk and metadata path to the new clusterID
   *    based location.
   * b) Re-computes the new container checksum.
   * b) Persists the new container layout to disk.
   * @param storageLoc
   * @param kvContainerData
   * @return upgraded KeyValueContainer
   * @throws IOException
   */
  public KeyValueContainer upgradeContainerLayout(File storageLoc,
      KeyValueContainerData kvContainerData) throws IOException {
    kvContainerData.setMetadataPath(
        findNormalizedPath(storageLoc,
            kvContainerData.getMetadataPath()));
    kvContainerData.setChunksPath(
        findNormalizedPath(storageLoc,
            kvContainerData.getChunksPath()));

    Yaml yaml = ContainerDataYaml.getYamlForContainerType(
        kvContainerData.getContainerType());
    kvContainerData.computeAndSetChecksum(yaml);

    KeyValueContainerUtil.parseKVContainerData(kvContainerData, config);
    KeyValueContainer kvContainer = new KeyValueContainer(
        kvContainerData, config);
    kvContainer.update(Collections.emptyMap(), true);
    return kvContainer;
  }

  /**
   * verify ContainerData loaded from disk and fix-up stale members.
   * Specifically blockCommitSequenceId, delete related metadata
   * and bytesUsed
   * @param containerData
   * @throws IOException
   */
  public void verifyAndFixupContainerData(File storageLoc,
      ContainerData containerData) throws IOException {
    switch (containerData.getContainerType()) {
    case KeyValueContainer:
      if (containerData instanceof KeyValueContainerData) {
        KeyValueContainerData kvContainerData = (KeyValueContainerData)
            containerData;
        containerData.setVolume(hddsVolume);
        KeyValueContainer kvContainer = null;
        if (isInUpgradeMode) {
          kvContainer =
              upgradeContainerLayout(storageLoc, kvContainerData);
        } else {
          KeyValueContainerUtil.parseKVContainerData(kvContainerData, config);
          kvContainer = new KeyValueContainer(
              kvContainerData, config);
        }
        containerSet.addContainer(kvContainer);
      } else {
        throw new StorageContainerException("Container File is corrupted. " +
            "ContainerType is KeyValueContainer but cast to " +
            "KeyValueContainerData failed. ",
            ContainerProtos.Result.CONTAINER_METADATA_ERROR);
      }
      break;
    default:
      throw new StorageContainerException("Unrecognized ContainerType " +
          containerData.getContainerType(),
          ContainerProtos.Result.UNKNOWN_CONTAINER_TYPE);
    }
  }

  public String findNormalizedPath(File storageLoc, String path) {
    Path p = Paths.get(path);
    Path relativePath = storageLoc.toPath().relativize(p);
    Path newPath = getClusterDir().toPath().resolve(relativePath);

    if (!isInUpgradeMode) {
      Preconditions.checkArgument(newPath.toFile().exists(),
          "modified path:" + newPath + " doesn't exists");
    }

    LOG.debug("new Normalized Path is:{}", newPath);
    return newPath.toAbsolutePath().toString();
  }
}
