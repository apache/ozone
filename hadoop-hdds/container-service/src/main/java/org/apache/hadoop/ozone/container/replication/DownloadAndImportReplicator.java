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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.replication.AbstractReplicationTask.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default replication implementation.
 * <p>
 * This class does the real job. Executes the download and import the container
 * to the container set.
 */
public class DownloadAndImportReplicator implements ContainerReplicator {

  private static final Logger LOG =
      LoggerFactory.getLogger(DownloadAndImportReplicator.class);

  private final ConfigurationSource conf;
  private final ContainerDownloader downloader;
  private final ContainerImporter containerImporter;
  private final ContainerSet containerSet;

  public DownloadAndImportReplicator(
      ConfigurationSource conf, ContainerSet containerSet,
      ContainerImporter containerImporter,
      ContainerDownloader downloader) {
    this.conf = conf;
    this.containerSet = containerSet;
    this.downloader = downloader;
    this.containerImporter = containerImporter;
  }

  @Override
  public void replicate(ReplicationTask task) {
    long containerID = task.getContainerId();
    if (containerSet.getContainer(containerID) != null) {
      LOG.debug("Container {} has already been downloaded.", containerID);
      task.setStatus(Status.SKIPPED);
      return;
    }

    List<DatanodeDetails> sourceDatanodes = task.getSources();
    CopyContainerCompression compression =
        CopyContainerCompression.getConf(conf);

    LOG.info("Starting replication of container {} from {} using {}",
        containerID, sourceDatanodes, compression);
    HddsVolume targetVolume = null;

    try {
      targetVolume = containerImporter.chooseNextVolume(
          containerImporter.getDefaultReplicationSpace());

      // Wait for the download. This thread pool is limiting the parallel
      // downloads, so it's ok to block here and wait for the full download.
      Path tarFilePath =
          downloader.getContainerDataFromReplicas(containerID, sourceDatanodes,
              ContainerImporter.getUntarDirectory(targetVolume), compression);
      if (tarFilePath == null) {
        task.setStatus(Status.FAILED);
        return;
      }
      long bytes = Files.size(tarFilePath);
      LOG.info("Container {} is downloaded with size {}, starting to import.",
              containerID, bytes);
      task.setTransferredBytes(bytes);

      containerImporter.importContainer(containerID, tarFilePath, targetVolume,
          compression);

      LOG.info("Container {} is replicated successfully", containerID);
      task.setStatus(Status.DONE);
    } catch (IOException e) {
      LOG.error("Container {} replication was unsuccessful.", containerID, e);
      task.setStatus(Status.FAILED);
    } finally {
      if (targetVolume != null) {
        targetVolume.incCommittedBytes(-containerImporter.getDefaultReplicationSpace());
      }
    }
  }

}
