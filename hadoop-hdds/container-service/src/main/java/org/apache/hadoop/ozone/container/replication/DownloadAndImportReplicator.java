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
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.replication.ReplicationTask.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default replication implementation.
 * <p>
 * This class does the real job. Executes the download and import the container
 * to the container set.
 */
public class DownloadAndImportReplicator implements ContainerReplicator {

  public static final Logger LOG =
      LoggerFactory.getLogger(DownloadAndImportReplicator.class);

  private final ContainerSet containerSet;

  private final ContainerController controller;

  private final ContainerDownloader downloader;

  private final TarContainerPacker packer;

  public DownloadAndImportReplicator(
      ContainerSet containerSet,
      ContainerController controller,
      ContainerDownloader downloader,
      TarContainerPacker packer) {
    this.containerSet = containerSet;
    this.controller = controller;
    this.downloader = downloader;
    this.packer = packer;
  }

  public void importContainer(long containerID, Path tarFilePath)
      throws IOException {
    try {
      ContainerData originalContainerData;
      try (FileInputStream tempContainerTarStream = new FileInputStream(
          tarFilePath.toFile())) {
        byte[] containerDescriptorYaml =
            packer.unpackContainerDescriptor(tempContainerTarStream);
        originalContainerData = ContainerDataYaml.readContainer(
            containerDescriptorYaml);
      }

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

    CompletableFuture<Path> tempTarFile = downloader
        .getContainerDataFromReplicas(containerID,
            sourceDatanodes);
    if (tempTarFile == null) {
      task.setStatus(Status.FAILED);
    } else {
      try {
        // Wait for the download. This thread pool is limiting the parallel
        // downloads, so it's ok to block here and wait for the full download.
        Path path = tempTarFile.get();
        long bytes = Files.size(path);

        LOG.info("Container {} is downloaded with size {}, starting to import.",
                containerID, bytes);
        task.setTransferredBytes(bytes);

        importContainer(containerID, path);
        LOG.info("Container {} is replicated successfully", containerID);
        task.setStatus(Status.DONE);
      } catch (Exception e) {
        LOG.error("Container {} replication was unsuccessful.", containerID, e);
        task.setStatus(Status.FAILED);
      }
    }
  }
}
