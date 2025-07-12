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

package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.metadata.WitnessedContainerMetadataStore;
import org.apache.hadoop.ozone.container.metadata.WitnessedContainerMetadataStoreImpl;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.replication.ContainerImporter;
import org.apache.hadoop.ozone.container.replication.ContainerReplicator;
import org.apache.hadoop.ozone.container.replication.DownloadAndImportReplicator;
import org.apache.hadoop.ozone.container.replication.ReplicationServer;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisor;
import org.apache.hadoop.ozone.container.replication.ReplicationTask;
import org.apache.hadoop.ozone.container.replication.SimpleContainerDownloader;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Utility to replicated closed container with datanode code.
 */
@Command(name = "cr",
    aliases = "container-replicator",
    description = "Replicate / download closed containers.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class ClosedContainerReplicator extends BaseFreonGenerator implements
    Callable<Void> {

  @Option(names = {"--datanode"},
      description = "Replicate only containers on this specific datanode.",
      defaultValue = "")
  private String datanode;

  private ReplicationSupervisor supervisor;

  private ContainerReplicator replicator;

  private Timer timer;
  private WitnessedContainerMetadataStore witnessedContainerMetadataStore;

  private List<ReplicationTask> replicationTasks;

  private static final Logger LOG =
      LoggerFactory.getLogger(ClosedContainerReplicator.class);

  @Override
  public Void call() throws Exception {
    try {
      return replicate();
    } finally {
      if (witnessedContainerMetadataStore != null) {
        witnessedContainerMetadataStore.close();
      }
    }
  }

  public Void replicate() throws Exception {

    OzoneConfiguration conf = createOzoneConfiguration();

    final Collection<String> datanodeStorageDirs =
        HddsServerUtil.getDatanodeStorageDirs(conf);

    for (String dir : datanodeStorageDirs) {
      checkDestinationDirectory(dir);
    }

    final ContainerOperationClient containerOperationClient =
        new ContainerOperationClient(conf);

    final List<ContainerInfo> containerInfos =
        containerOperationClient.listContainer(0L, 1_000_000).getContainerInfoList();

    //logic same as the download+import on the destination datanode
    initializeReplicationSupervisor(conf, containerInfos.size() * 2);

    replicationTasks = new ArrayList<>();

    for (ContainerInfo container : containerInfos) {
      if (container.getState() == LifeCycleState.CLOSED) {
        final Pipeline pipeline = containerOperationClient.getPipeline(container.getPipelineID().getProtobuf());
        final List<DatanodeDetails> datanodesWithContainer = pipeline.getNodes();
        final List<String> datanodeUUIDs =
            datanodesWithContainer
                .stream().map(DatanodeDetails::getUuidString)
                .collect(Collectors.toList());

        //if datanode is specified, replicate only container if it has a
        //replica.
        if (datanode.isEmpty() || datanodeUUIDs.contains(datanode)) {
          replicationTasks.add(new ReplicationTask(
              ReplicateContainerCommand.fromSources(container.getContainerID(),
                  datanodesWithContainer), replicator));
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

  /**
   * Check id target directory is not re-used.
   */
  private void checkDestinationDirectory(String dirUrl) throws IOException {
    final StorageLocation storageLocation = StorageLocation.parse(dirUrl);
    final Path dirPath = Paths.get(storageLocation.getUri().getPath());

    if (Files.notExists(dirPath)) {
      return;
    }
    try (Stream<Path> stream = Files.list(dirPath)) {
      if (stream.count() == 0) {
        return;
      }
    }

    throw new IllegalArgumentException(
        "Configured storage directory " + dirUrl
            + " (used as destination) should be empty");
  }

  private void initializeReplicationSupervisor(
      ConfigurationSource conf, int queueSize) throws IOException {
    String scmID = UUID.randomUUID().toString();
    String clusterID = UUID.randomUUID().toString();
    String fakeDatanodeUuid = datanode;

    if (fakeDatanodeUuid.isEmpty()) {
      fakeDatanodeUuid = UUID.randomUUID().toString();
    }
    WitnessedContainerMetadataStore referenceCountedDS =
        WitnessedContainerMetadataStoreImpl.get(conf);
    this.witnessedContainerMetadataStore = referenceCountedDS;
    ContainerSet containerSet = ContainerSet.newRwContainerSet(referenceCountedDS, 1000);

    ContainerMetrics metrics = ContainerMetrics.create(conf);

    MutableVolumeSet volumeSet = new MutableVolumeSet(fakeDatanodeUuid, conf,
        null, StorageVolume.VolumeType.DATA_VOLUME, null);
    VolumeChoosingPolicy volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(conf);

    if (VersionedDatanodeFeatures.SchemaV3.isFinalizedAndEnabled(conf)) {
      MutableVolumeSet dbVolumeSet =
          HddsServerUtil.getDatanodeDbDirs(conf).isEmpty() ? null :
          new MutableVolumeSet(fakeDatanodeUuid, conf, null,
              StorageVolume.VolumeType.DB_VOLUME, null);
      // load rocksDB with readOnly mode, otherwise it will fail.
      HddsVolumeUtil.loadAllHddsVolumeDbStore(
          volumeSet, dbVolumeSet, false, LOG);

      for (StorageVolume volume : volumeSet.getVolumesList()) {
        StorageVolumeUtil.checkVolume(volume, scmID, clusterID, conf, LOG, dbVolumeSet);
      }
    }

    Map<ContainerType, Handler> handlers = new HashMap<>();

    for (ContainerType containerType : ContainerType.values()) {
      final Handler handler =
          Handler.getHandlerForContainerType(
              containerType,
              conf,
              fakeDatanodeUuid,
              containerSet,
              volumeSet,
              volumeChoosingPolicy,
              metrics,
              containerReplicaProto -> {
              },
              // Since this a Freon tool, this instance is not part of a running datanode.
              new ContainerChecksumTreeManager(conf));
      handler.setClusterID(UUID.randomUUID().toString());
      handlers.put(containerType, handler);
    }

    ContainerController controller =
        new ContainerController(containerSet, handlers);

    ContainerImporter importer = new ContainerImporter(conf, containerSet,
        controller, volumeSet, volumeChoosingPolicy);
    replicator = new DownloadAndImportReplicator(conf, containerSet, importer,
        new SimpleContainerDownloader(conf, null));

    DatanodeConfiguration datanodeConfig =
        conf.getObject(DatanodeConfiguration.class);
    datanodeConfig.setCommandQueueLimit(queueSize);
    ReplicationServer.ReplicationConfig replicationConfig
        = conf.getObject(ReplicationServer.ReplicationConfig.class);
    supervisor = ReplicationSupervisor.newBuilder()
        .datanodeConfig(datanodeConfig)
        .replicationConfig(replicationConfig)
        .build();
  }

  private void replicateContainer(long counter) throws Exception {
    timer.time(() -> {
      final ReplicationTask replicationTask =
          replicationTasks.get((int) counter);
      supervisor.initCounters(replicationTask);
      supervisor.new TaskRunner(replicationTask).run();
      return null;
    });
  }
}
