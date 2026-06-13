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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
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
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
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
    markContainerImportInProgress(containerID, tarFilePath);

    try {
      checkContainerCanBeImported(containerID);
      doImportContainer(containerID, tarFilePath, targetVolume, compression);
    } finally {
      importContainerProgress.remove(containerID);
      FileUtils.deleteQuietly(tarFilePath.toFile());
    }
  }

  /**
   * Imports a container and retries on alternate volumes only when the selected
   * volume already has the container directory.
   *
   * The caller is responsible for releasing committed bytes on
   * initialTargetVolume. This method releases committed bytes only for
   * retry-selected volumes.
   */
  public void importContainerWithVolumeRetry(long containerID, Path tarFilePath,
      HddsVolume initialTargetVolume, CopyContainerCompression compression,
      long spaceToReserve) throws IOException {
    markContainerImportInProgress(containerID, tarFilePath);

    try {
      checkContainerCanBeImported(containerID);
      List<HddsVolume> remainingVolumes =
          getCandidateVolumesExcluding(initialTargetVolume);

      IOException lastException = null;
      if (initialTargetVolume != null) {
        lastException = tryImportContainerToVolume(containerID, tarFilePath,
            initialTargetVolume, compression);
        if (lastException == null) {
          return;
        }
      }

      while (!remainingVolumes.isEmpty()) {
        HddsVolume targetVolume = chooseNextVolume(remainingVolumes,
            spaceToReserve);
        try {
          lastException = tryImportContainerToVolume(containerID, tarFilePath,
              targetVolume, compression);
          if (lastException == null) {
            return;
          }
        } finally {
          targetVolume.incCommittedBytes(-spaceToReserve);
        }
        remainingVolumes.remove(targetVolume);
      }

      throw new StorageContainerException(
          "Container import failed because container " + containerID +
              " already exists on all candidate volumes",
          lastException, ContainerProtos.Result.CONTAINER_ALREADY_EXISTS);
    } finally {
      importContainerProgress.remove(containerID);
      FileUtils.deleteQuietly(tarFilePath.toFile());
    }
  }

  private void markContainerImportInProgress(long containerID, Path tarFilePath)
      throws StorageContainerException {
    if (!importContainerProgress.add(containerID)) {
      FileUtils.deleteQuietly(tarFilePath.toFile());
      String log = "Container import in progress with container Id " + containerID;
      LOG.warn(log);
      throw new StorageContainerException(log,
          ContainerProtos.Result.CONTAINER_EXISTS);
    }
  }

  private List<HddsVolume> getCandidateVolumesExcluding(
      HddsVolume excludedVolume) {
    volumeSet.readLock();
    try {
      List<HddsVolume> volumes =
          StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList());
      volumes.remove(excludedVolume);
      return volumes;
    } finally {
      volumeSet.readUnlock();
    }
  }

  private IOException tryImportContainerToVolume(long containerID,
      Path tarFilePath, HddsVolume targetVolume,
      CopyContainerCompression compression) throws IOException {
    try {
      importContainerToSelectedVolume(containerID, tarFilePath, targetVolume,
          compression);
      return null;
    } catch (IOException ex) {
      if (!isContainerAlreadyExistsException(ex)) {
        throw ex;
      }
      return ex;
    }
  }

  private void importContainerToSelectedVolume(long containerID,
      Path tarFilePath, HddsVolume targetVolume,
      CopyContainerCompression compression) throws IOException {
    if (containerDirExists(targetVolume, containerID)) {
      throw new StorageContainerException(
          "Container " + containerID + " already exists on selected volume " +
              targetVolume.getHddsRootDir(),
          ContainerProtos.Result.CONTAINER_ALREADY_EXISTS);
    }
    doImportContainer(containerID, tarFilePath, targetVolume, compression);
  }

  private void doImportContainer(long containerID, Path tarFilePath,
      HddsVolume targetVolume, CopyContainerCompression compression)
      throws IOException {
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
      if (!isContainerAlreadyExistsException(e)) {
        // Trigger a volume scan if the import failed.
        StorageVolumeUtil.onFailure(containerData.getVolume());
      }
      throw e;
    }
  }

  private void checkContainerCanBeImported(long containerID)
      throws StorageContainerException {
    if (containerSet.getContainer(containerID) != null) {
      String log = "Container already exists with container Id " + containerID;
      LOG.warn(log);
      throw new StorageContainerException(log,
          ContainerProtos.Result.CONTAINER_EXISTS);
    }
  }

  private boolean containerDirExists(HddsVolume targetVolume, long containerID)
      throws IOException {
    String idDir = VersionedDatanodeFeatures.ScmHA.chooseContainerPathID(
        targetVolume, targetVolume.getClusterID());
    Path containerPath = Paths.get(KeyValueContainerLocationUtil
        .getBaseContainerLocation(targetVolume.getHddsRootDir().toString(),
            idDir, containerID));
    return Files.exists(containerPath);
  }

  private boolean isContainerAlreadyExistsException(Throwable ex) {
    return ex instanceof StorageContainerException &&
        ((StorageContainerException) ex).getResult() ==
            ContainerProtos.Result.CONTAINER_ALREADY_EXISTS;
  }

  HddsVolume chooseNextVolume(long spaceToReserve) throws IOException {
    // Choose volume that can hold both container in tmp and dest directory
    LOG.debug("Choosing volume to reserve space : {}", spaceToReserve);
    return volumeChoosingPolicy.chooseVolume(
        StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList()),
        spaceToReserve);
  }

  HddsVolume chooseNextVolume(List<HddsVolume> volumes, long spaceToReserve)
      throws IOException {
    // Choose volume that can hold both container in tmp and dest directory
    LOG.debug("Choosing volume to reserve space : {}", spaceToReserve);
    return volumeChoosingPolicy.chooseVolume(new ArrayList<>(volumes),
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
