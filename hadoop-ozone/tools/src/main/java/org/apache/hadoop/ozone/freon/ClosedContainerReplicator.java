/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.replication.ContainerReplicator;
import org.apache.hadoop.ozone.container.replication.DownloadAndImportReplicator;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisor;
import org.apache.hadoop.ozone.container.replication.ReplicationTask;
import org.jetbrains.annotations.NotNull;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * Utility to replicated closed container with datanode code.
 */
@Command(name = "cr",
    aliases = "container-replicator",
    description = "Replicate / download closed containers.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class ClosedContainerReplicator extends BaseFreonGenerator implements
    Callable<Void> {

  @Option(names = {"--datanode"},
      description = "Replicate only containers on this specific datanode.",
      defaultValue = "")
  private String datanode;

  private ReplicationSupervisor supervisor;

  private Timer timer;

  private List<ReplicationTask> replicationTasks;

  @Override
  public Void call() throws Exception {

    OzoneConfiguration conf = createOzoneConfiguration();

    final Collection<String> datanodeStorageDirs =
        HddsServerUtil.getDatanodeStorageDirs(conf);

    for (String dir : datanodeStorageDirs) {
      checkDestinationDirectory(dir);
    }

    //logic same as the download+import on the destination datanode
    initializeReplicationSupervisor(conf);

    final ContainerOperationClient containerOperationClient =
        new ContainerOperationClient(conf);

    final List<ContainerInfo> containerInfos =
        containerOperationClient.listContainer(0L, 1_000_000);

    replicationTasks = new ArrayList<>();

    for (ContainerInfo container : containerInfos) {

      final ContainerWithPipeline containerWithPipeline =
          containerOperationClient
              .getContainerWithPipeline(
                  container.containerID().getId());

      if (container.getState() == LifeCycleState.CLOSED) {
        final List<DatanodeDetails> datanodesWithContainer =
            containerWithPipeline.getPipeline().getNodes();

        final List<String> datanodeUUIDs =
            datanodesWithContainer
                .stream().map(DatanodeDetails::getUuidString)
                .collect(Collectors.toList());

        //if datanode is specified, replicate only container if it has a
        //replica.
        if (datanode.isEmpty() || datanodeUUIDs.contains(datanode)) {
          replicationTasks.add(new ReplicationTask(container.getContainerID(),
              datanodesWithContainer));
        }
      }
      if (replicationTasks.size() >= getTestNo()) {
        break;
      }
    }

    //important: override the max number of tasks.
    setTestNo(replicationTasks.size());

    init();

    timer = getMetrics().timer("replicate-container");
    runTests(this::replicateContainer);
    return null;
  }

  /**
   * Check id target directory is not re-used.
   */
  private void checkDestinationDirectory(String dirUrl) throws IOException {
    final StorageLocation storageLocation = StorageLocation.parse(dirUrl);
    final Path dirPath = Paths.get(storageLocation.getUri().getPath());

    if (Files.notExists(dirPath)) {
      return;
    }

    if (Files.list(dirPath).count() == 0) {
      return;
    }

    throw new IllegalArgumentException(
        "Configured storage directory " + dirUrl
            + " (used as destination) should be empty");
  }

  @NotNull
  private void initializeReplicationSupervisor(ConfigurationSource conf)
      throws IOException {
    String fakeDatanodeUuid = datanode;

    if (fakeDatanodeUuid.isEmpty()) {
      fakeDatanodeUuid = UUID.randomUUID().toString();
    }

    String fakeClusterId = UUID.randomUUID().toString();

    ContainerSet containerSet = new ContainerSet();

    MutableVolumeSet volumeSet = new MutableVolumeSet(fakeDatanodeUuid, conf,
        null, StorageVolume.VolumeType.DATA_VOLUME, null);

    ContainerReplicator replicator =
        new DownloadAndImportReplicator(conf,
            () -> fakeClusterId,
            containerSet,
            volumeSet,
            null);

    supervisor = new ReplicationSupervisor(containerSet, replicator, 10);
  }

  private void replicateContainer(long counter) throws Exception {
    timer.time(() -> {
      final ReplicationTask replicationTask =
          replicationTasks.get((int) counter);
      supervisor.new TaskRunner(replicationTask).run();
      return null;
    });
  }
}