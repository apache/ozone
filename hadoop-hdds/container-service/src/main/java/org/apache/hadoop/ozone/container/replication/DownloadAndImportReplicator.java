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

import io.netty.handler.ssl.SslContext;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.replication.ReplicationTask.Status;
import org.apache.hadoop.ozone.container.stream.StreamingClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

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

  private final ContainerSet containerSet;

  private final ConfigurationSource config;
  private final Supplier<String> clusterId;
  private VolumeChoosingPolicy volumeChoosingPolicy;
  private VolumeSet volumeSet;
  private SslContext sslContext;

  public DownloadAndImportReplicator(
      ConfigurationSource config,
      Supplier<String> clusterId,
      ContainerSet containerSet,
      VolumeSet volumeSet,
      SslContext sslContext
  ) {
    this.containerSet = containerSet;
    this.config = config;
    this.clusterId = clusterId;
    this.volumeSet = volumeSet;
    this.sslContext = sslContext;
    Class<? extends VolumeChoosingPolicy> volumeChoosingPolicyType = null;
    try {
      volumeChoosingPolicyType =
          config.getClass(
              HDDS_DATANODE_VOLUME_CHOOSING_POLICY,
              RoundRobinVolumeChoosingPolicy
                  .class, VolumeChoosingPolicy.class);

      this.volumeChoosingPolicy = volumeChoosingPolicyType.newInstance();

    } catch (InstantiationException ex) {
      throw new IllegalArgumentException(
          "Couldn't create volume choosing policy: " + volumeChoosingPolicyType,
          ex);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }

  }

  @Override
  public void replicate(ReplicationTask task) {
    long containerID = task.getContainerId();
    if (clusterId.get() == null) {
      LOG.error("Replication task is called before first SCM call");
      task.setStatus(Status.FAILED);
    }
    List<DatanodeDetails> sourceDatanodes = task.getSources();

    LOG.info("Starting replication of container {} from {}", containerID,
        sourceDatanodes);

    try {

      long maxContainerSize = (long) config.getStorageSize(
          ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
          ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);

      KeyValueContainerData containerData =
          new KeyValueContainerData(containerID,
              ChunkLayOutVersion.FILE_PER_BLOCK, maxContainerSize, "", "");

      //choose a volume
      final HddsVolume volume = volumeChoosingPolicy
          .chooseVolume(
              StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList()),
              maxContainerSize);

      //fill the path fields
      containerData.assignToVolume(clusterId.get(), volume);

      Collections.shuffle(sourceDatanodes);

      //download data
      final DatanodeDetails datanode = sourceDatanodes.get(0);

      try (StreamingClient client =
               new StreamingClient(datanode.getIpAddress(),
                   datanode.getPort(Name.REPLICATION).getValue(),
                   new ContainerStreamingDestination(containerData),
                   sslContext)
      ) {
        client.stream("" + containerData.getContainerID());

        LOG.info("Container " + containerData.getContainerID()
            + " is downloaded successfully");
        KeyValueContainerData loadedContainerData =
            updateContainerData(containerData);
        LOG.info("Container {} is downloaded, starting to import.",
            containerID);
        importContainer(loadedContainerData);
        LOG.info("Container {} is replicated successfully", containerID);
        task.setStatus(Status.DONE);

      }
    } catch (IOException | RuntimeException ex) {
      LOG.error("Error on replicating container " + containerID, ex);
      task.setStatus(Status.FAILED);
    }
  }

  private void importContainer(
      KeyValueContainerData loadedContainerData
  ) throws IOException {

    //write out container descriptor
    KeyValueContainer keyValueContainer =
        new KeyValueContainer(loadedContainerData, config);

    //rewriting the yaml file with new checksum calculation.
    keyValueContainer.update(loadedContainerData.getMetadata(), true);

    //fill in memory stat counter (keycount, byte usage)
    KeyValueContainerUtil.parseKVContainerData(loadedContainerData, config);

    //load container
    containerSet.addContainer(keyValueContainer);

  }

  private KeyValueContainerData updateContainerData(
      KeyValueContainerData preCreated)
      throws IOException {
    try (FileInputStream fis = new FileInputStream(
        preCreated.getContainerFile().toString() + ".original")) {
      //parse descriptor
      //now, we have extracted the container descriptor from the previous
      //datanode. We can load it and upload it with the current data
      // (original metadata + current filepath fields)
      KeyValueContainerData replicated =
          (KeyValueContainerData) ContainerDataYaml.readContainer(fis);

      KeyValueContainerData updated = new KeyValueContainerData(
          replicated.getContainerID(),
          replicated.getLayOutVersion(),
          replicated.getMaxSize(),
          replicated.getOriginPipelineId(),
          replicated.getOriginNodeId());

      //inherited from the replicated
      updated
          .setState(replicated.getState());
      updated
          .setContainerDBType(replicated.getContainerDBType());
      updated
          .updateBlockCommitSequenceId(replicated
              .getBlockCommitSequenceId());
      updated
          .setSchemaVersion(replicated.getSchemaVersion());

      //inherited from the pre-created seed container
      updated.setMetadataPath(preCreated.getMetadataPath());
      updated.setDbFile(preCreated.getDbFile());
      updated.setChunksPath(preCreated.getChunksPath());
      updated.setVolume(preCreated.getVolume());

      return updated;

    }
  }
}