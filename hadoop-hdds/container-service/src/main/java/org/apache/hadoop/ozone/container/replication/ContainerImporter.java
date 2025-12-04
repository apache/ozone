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

package org.apache.hadoop.ozone.container.replication;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Imports container from tarball.
 */
public class ContainerImporter {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerImporter.class);

  public static final String CONTAINER_COPY_DIR = "container-copy";
  private static final String CONTAINER_COPY_TMP_DIR = "tmp";
  private final ContainerSet containerSet;
  private final ContainerController controller;
  private final MutableVolumeSet volumeSet;
  private final VolumeChoosingPolicy volumeChoosingPolicy;
  private final long defaultContainerSize;

  private final Set<Long> importContainerProgress
      = Collections.synchronizedSet(new HashSet<>());

  private final ConfigurationSource conf;

  public ContainerImporter(@Nonnull ConfigurationSource conf,
                           @Nonnull ContainerSet containerSet,
                           @Nonnull ContainerController controller,
                           @Nonnull MutableVolumeSet volumeSet,
                           @Nonnull VolumeChoosingPolicy volumeChoosingPolicy) {
    this.containerSet = containerSet;
    this.controller = controller;
    this.volumeSet = volumeSet;
    this.volumeChoosingPolicy = volumeChoosingPolicy;
    defaultContainerSize = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    this.conf = conf;
  }

  public boolean isAllowedContainerImport(long containerID) {
    return !importContainerProgress.contains(containerID) &&
        containerSet.getContainer(containerID) == null;
  }

  public void importContainer(long containerID, Path tarFilePath,
      HddsVolume targetVolume, CopyContainerCompression compression)
      throws IOException {
    if (!importContainerProgress.add(containerID)) {
      deleteFileQuietely(tarFilePath);
      String log = "Container import in progress with container Id " + containerID;
      LOG.warn(log);
      throw new StorageContainerException(log,
          ContainerProtos.Result.CONTAINER_EXISTS);
    }

    try {
      if (containerSet.getContainer(containerID) != null) {
        String log = "Container already exists with container Id " + containerID;
        LOG.warn(log);
        throw new StorageContainerException(log,
            ContainerProtos.Result.CONTAINER_EXISTS);
      }

      KeyValueContainerData containerData;
      TarContainerPacker packer = getPacker(compression);

      try (InputStream input = Files.newInputStream(tarFilePath)) {
        byte[] containerDescriptorYaml =
            packer.unpackContainerDescriptor(input);
        containerData = getKeyValueContainerData(containerDescriptorYaml);
      }
      ContainerUtils.verifyContainerFileChecksum(containerData, conf);
      containerData.setVolume(targetVolume);
      // lastDataScanTime should be cleared for an imported container
      containerData.setDataScanTimestamp(null);

      try (InputStream input = Files.newInputStream(tarFilePath)) {
        Container container = controller.importContainer(
            containerData, input, packer);
        // After container import is successful, increase used space for the volume and schedule an OnDemand scan for it
        targetVolume.incrementUsedSpace(container.getContainerData().getBytesUsed());
        containerSet.addContainerByOverwriteMissingContainer(container);
        containerSet.scanContainer(containerID, "Imported container");
      } catch (Exception e) {
        // Trigger a volume scan if the import failed.
        StorageVolumeUtil.onFailure(containerData.getVolume());
        throw e;
      }
    } finally {
      importContainerProgress.remove(containerID);
      deleteFileQuietely(tarFilePath);
    }
  }

  private static void deleteFileQuietely(Path tarFilePath) {
    try {
      Files.delete(tarFilePath);
    } catch (Exception ex) {
      LOG.error("Got exception while deleting temporary container file: "
          + tarFilePath.toAbsolutePath(), ex);
    }
  }

  HddsVolume chooseNextVolume(long spaceToReserve) throws IOException {
    // Choose volume that can hold both container in tmp and dest directory
    LOG.debug("Choosing volume to reserve space : {}", spaceToReserve);
    return volumeChoosingPolicy.chooseVolume(
        StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList()),
        spaceToReserve);
  }

  public static Path getUntarDirectory(HddsVolume hddsVolume)
      throws IOException {
    return Paths.get(hddsVolume.getVolumeRootDir())
        .resolve(CONTAINER_COPY_TMP_DIR).resolve(CONTAINER_COPY_DIR);
  }

  protected KeyValueContainerData getKeyValueContainerData(
      byte[] containerDescriptorYaml) throws IOException {
    return  (KeyValueContainerData) ContainerDataYaml
        .readContainer(containerDescriptorYaml);
  }

  protected Set<Long> getImportContainerProgress() {
    return this.importContainerProgress;
  }

  protected TarContainerPacker getPacker(CopyContainerCompression compression) {
    return new TarContainerPacker(compression);
  }

  public long getDefaultReplicationSpace() {
    return HddsServerUtil.requiredReplicationSpace(defaultContainerSize);
  }

  /**
   * Calculate required replication space based on actual container size.
   *
   * @param actualContainerSize the actual size of the container in bytes
   * @return required space for replication (2 * actualContainerSize)
   */
  public long getRequiredReplicationSpace(long actualContainerSize) {
    return HddsServerUtil.requiredReplicationSpace(actualContainerSize);
  }

  /**
   * Get space to reserve for replication. If replicateSize is provided,
   * calculate required space based on that, otherwise return default
   * replication space.
   *
   * @param replicateSize the size of the container to replicate in bytes
   *                      (can be null)
   * @return space to reserve for replication
   */
  public long getSpaceToReserve(Long replicateSize) {
    if (replicateSize != null) {
      return getRequiredReplicationSpace(replicateSize);
    } else {
      return getDefaultReplicationSpace();
    }
  }
}
