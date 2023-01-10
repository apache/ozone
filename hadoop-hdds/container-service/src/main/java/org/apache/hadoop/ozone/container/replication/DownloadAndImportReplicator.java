/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.replication;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.replication.ReplicationTask.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_VOLUME_CHOOSING_POLICY;

/**
 * Default replication implementation.
 * <p>
 * This class does the real job. Executes the download and import the container
 * to the container set.
 */
public class DownloadAndImportReplicator implements ContainerReplicator {

  public static final Logger LOG =
      LoggerFactory.getLogger(DownloadAndImportReplicator.class);

  public static final String CONTAINER_COPY_DIR = "container-copy";
  public static final String CONTAINER_COPY_TMP_DIR = "tmp";

  private final ContainerSet containerSet;
  private final ContainerController controller;
  private final ContainerDownloader downloader;
  private final TarContainerPacker packer;
  private final MutableVolumeSet volumeSet;
  private final VolumeChoosingPolicy volumeChoosingPolicy;
  private final long containerSize;

  public DownloadAndImportReplicator(
      ConfigurationSource conf,
      ContainerSet containerSet,
      ContainerController controller,
      ContainerDownloader downloader,
      TarContainerPacker packer,
      MutableVolumeSet volumeSet) {
    this.containerSet = containerSet;
    this.controller = controller;
    this.downloader = downloader;
    this.packer = packer;
    this.volumeSet = volumeSet;
    try {
      this.volumeChoosingPolicy = conf.getClass(
          HDDS_DATANODE_VOLUME_CHOOSING_POLICY, RoundRobinVolumeChoosingPolicy
              .class, VolumeChoosingPolicy.class).newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    this.containerSize = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);

  }

  public void importContainer(long containerID, Path tarFilePath,
      HddsVolume hddsVolume) throws IOException {

    HddsVolume targetVolume = hddsVolume;
    if (targetVolume == null) {
      targetVolume = chooseNextVolume();
    }
    KeyValueContainerData originalContainerData;
    try {
      try (FileInputStream tmpContainerTarStream = new FileInputStream(
          tarFilePath.toFile())) {
        byte[] containerDescriptorYaml =
            packer.unpackContainerDescriptor(tmpContainerTarStream);
        originalContainerData = (KeyValueContainerData) ContainerDataYaml
            .readContainer(containerDescriptorYaml);
      }
      originalContainerData.setVolume(targetVolume);

      try (FileInputStream tempContainerTarStream = new FileInputStream(
          tarFilePath.toFile())) {

        Container container = controller.importContainer(
            originalContainerData, tempContainerTarStream, packer);

        containerSet.addContainer(container);
      }

    } finally {
      try {
        Files.delete(tarFilePath);
      } catch (Exception ex) {
        LOG.error("Got exception while deleting downloaded container file: "
            + tarFilePath.toAbsolutePath().toString(), ex);
      }
    }
  }

  @Override
  public void replicate(ReplicationTask task) {
    long containerID = task.getContainerId();

    List<DatanodeDetails> sourceDatanodes = task.getSources();

    LOG.info("Starting replication of container {} from {}", containerID,
        sourceDatanodes);

    try {
      HddsVolume targetVolume = chooseNextVolume();
      // Wait for the download. This thread pool is limiting the parallel
      // downloads, so it's ok to block here and wait for the full download.
      Path tarFilePath =
          downloader.getContainerDataFromReplicas(containerID, sourceDatanodes,
              getUntarDirectory(targetVolume));
      if (tarFilePath == null) {
        task.setStatus(Status.FAILED);
        return;
      }
      long bytes = Files.size(tarFilePath);
      LOG.info("Container {} is downloaded with size {}, starting to import.",
              containerID, bytes);
      task.setTransferredBytes(bytes);

      importContainer(containerID, tarFilePath, targetVolume);

      LOG.info("Container {} is replicated successfully", containerID);
      task.setStatus(Status.DONE);
    } catch (IOException e) {
      LOG.error("Container {} replication was unsuccessful.", containerID, e);
      task.setStatus(Status.FAILED);
    }
  }

  private HddsVolume chooseNextVolume() throws IOException {
    // Choose volume that can hold both container in tmp and dest directory
    return volumeChoosingPolicy.chooseVolume(
        StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList()),
        containerSize * 2);
  }

  public static Path getUntarDirectory(HddsVolume hddsVolume)
      throws IOException {
    return Paths.get(hddsVolume.getVolumeRootDir())
        .resolve(CONTAINER_COPY_TMP_DIR).resolve(CONTAINER_COPY_DIR);
  }

  private List<HddsVolume> getHddsVolumesList() {
    return StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList());
  }
}
