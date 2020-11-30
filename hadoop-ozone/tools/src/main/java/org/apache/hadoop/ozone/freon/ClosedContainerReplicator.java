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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.replication.ContainerReplicator;
import org.apache.hadoop.ozone.container.replication.DownloadAndImportReplicator;
import org.apache.hadoop.ozone.container.replication.MeasuredReplicator;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisor;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisor.TaskRunner;
import org.apache.hadoop.ozone.container.replication.ReplicationTask;
import org.apache.hadoop.ozone.container.replication.SimpleContainerDownloader;

import com.codahale.metrics.Timer;
import org.jetbrains.annotations.NotNull;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Utility to replicated closed container with datanode code.
 */
@Command(name = "cr",
    aliases = "container-replicator",
    description = "Replicate / download containers in the name of a namenode.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class ClosedContainerReplicator extends BaseFreonGenerator implements
    Callable<Void> {

  @Option(names = {"--datanode"},
      description = "Replicate only containers on this specific datanode.",
      defaultValue = "")
  private String dataNode;

  private ReplicationSupervisor supervisor;

  private Timer timer;

  private List<ReplicationTask> replicationTasks;

  @Override
  public Void call() throws Exception {

    //logic same as the download+import on the destination datanode
    OzoneConfiguration conf = initializeReplicationSupervisor();

    final ContainerOperationClient containerOperationClient =
        new ContainerOperationClient(conf);

    final List<ContainerInfo> containerInfos =
        containerOperationClient.listContainer(0L, 1_000_000);

    replicationTasks = new ArrayList<>();

    for (ContainerInfo container : containerInfos) {

      final ContainerWithPipeline containerWithPipeline =
          containerOperationClient
              .getContainerWithPipeline(container.getContainerID());

      if (container.getState() == LifeCycleState.CLOSED) {

        final List<DatanodeDetails> datanodesWithContainer =
            containerWithPipeline.getPipeline().getNodes();

        final List<String> datanodeUUIDs =
            datanodesWithContainer
                .stream().map(DatanodeDetails::getUuidString)
                .collect(Collectors.toList());

        //if datanode is specific replicate only container if has a replica.
        if (dataNode.equals("") || datanodeUUIDs.contains(dataNode)) {
          replicationTasks.add(new ReplicationTask(container.getContainerID(),
              datanodesWithContainer));
        }
      }

    }

    //important: override the max number of tasks.
    setTestNo(replicationTasks.size());

    init();

    timer = getMetrics().timer("replicate-container");
    runTests(this::replicateContainer);
    return null;
  }

  @NotNull
  private OzoneConfiguration initializeReplicationSupervisor()
      throws IOException {
    String fakeDatanodeUuid = dataNode;

    if (fakeDatanodeUuid.equals("")) {
      UUID.randomUUID().toString();
    }

    OzoneConfiguration conf = createOzoneConfiguration();

    ContainerSet containerSet = new ContainerSet();

    ContainerMetrics metrics = ContainerMetrics.create(conf);

    MutableVolumeSet volumeSet = new MutableVolumeSet(fakeDatanodeUuid, conf);

    Map<ContainerType, Handler> handlers = new HashMap<>();

    for (ContainerType containerType : ContainerType.values()) {
      final Handler handler =
          Handler.getHandlerForContainerType(
              containerType,
              conf,
              fakeDatanodeUuid,
              containerSet,
              volumeSet,
              metrics,
              containerReplicaProto -> {
              });
      handler.setScmID(UUID.randomUUID().toString());
      handlers.put(containerType, handler);
    }

    ContainerController controller =
        new ContainerController(containerSet, handlers);

    ContainerReplicator replicator =
        new MeasuredReplicator(
            new DownloadAndImportReplicator(containerSet,
                controller,
                new SimpleContainerDownloader(conf, null),
                new TarContainerPacker()));

    supervisor = new ReplicationSupervisor(containerSet, replicator, 10);
    return conf;
  }

  private void replicateContainer(long counter) throws Exception {
    timer.time(() -> {
      final ReplicationTask replicationTask =
          replicationTasks.get((int) counter);
      final TaskRunner taskRunner = supervisor.new TaskRunner(replicationTask);
      taskRunner.run();
      return null;
    });
  }
}