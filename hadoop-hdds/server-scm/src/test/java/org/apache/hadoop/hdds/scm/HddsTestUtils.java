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

package org.apache.hadoop.hdds.scm;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ClosePipelineInfo;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineActionsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.PipelineActionsFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.PipelineReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

/**
 * Stateless helper functions for Hdds tests.
 */
public final class HddsTestUtils {

  private static ThreadLocalRandom random = ThreadLocalRandom.current();
  private static PipelineID randomPipelineID = PipelineID.randomId();

  public static final long CONTAINER_USED_BYTES_DEFAULT = 100L;
  public static final long CONTAINER_NUM_KEYS_DEFAULT = 2L;

  private HddsTestUtils() {
  }

  /**
   * Generates DatanodeDetails from RegisteredCommand.
   *
   * @param registeredCommand registration response from SCM
   *
   * @return DatanodeDetails
   */
  public static DatanodeDetails getDatanodeDetails(
      RegisteredCommand registeredCommand) {
    return MockDatanodeDetails.createDatanodeDetails(
        registeredCommand.getDatanode().getID(),
        registeredCommand.getDatanode().getHostName(),
        registeredCommand.getDatanode().getIpAddress(),
        null);
  }

  /**
   * Creates a random DatanodeDetails and register it with the given
   * NodeManager.
   *
   * @param nodeManager NodeManager
   *
   * @return DatanodeDetails
   */
  public static DatanodeDetails createRandomDatanodeAndRegister(
      SCMNodeManager nodeManager) {
    return getDatanodeDetails(
        nodeManager.register(MockDatanodeDetails.randomDatanodeDetails(), null,
                getRandomPipelineReports()));
  }

  /**
   * Get specified number of DatanodeDetails and register them with node
   * manager.
   *
   * @param nodeManager node manager to register the datanode ids.
   * @param count       number of DatanodeDetails needed.
   *
   * @return list of DatanodeDetails
   */
  public static List<DatanodeDetails> getListOfRegisteredDatanodeDetails(
      SCMNodeManager nodeManager, int count) {
    ArrayList<DatanodeDetails> datanodes = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      datanodes.add(createRandomDatanodeAndRegister(nodeManager));
    }
    return datanodes;
  }

  /**
   * Generates a random NodeReport.
   *
   * @return NodeReportProto
   */
  public static NodeReportProto getRandomNodeReport() {
    return getRandomNodeReport(1, 1);
  }

  /**
   * Generates random NodeReport with the given number of storage report in it.
   *
   * @param numberOfStorageReport number of storage report this node report
   *                              should have
   * @param numberOfMetadataStorageReport number of metadata storage report
   *                                      this node report should have
   * @return NodeReportProto
   */
  public static NodeReportProto getRandomNodeReport(int numberOfStorageReport,
      int numberOfMetadataStorageReport) {
    DatanodeID nodeId = DatanodeID.randomID();
    return getRandomNodeReport(nodeId, File.separator + nodeId.getID(),
        numberOfStorageReport, numberOfMetadataStorageReport);
  }

  /**
   * Generates random NodeReport for the given nodeId with the given
   * base path and number of storage report in it.
   *
   * @param nodeId                datanode id
   * @param basePath              base path of storage directory
   * @param numberOfStorageReport number of storage report
   * @param numberOfMetadataStorageReport number of metadata storage report
   *
   * @return NodeReportProto
   */
  public static NodeReportProto getRandomNodeReport(DatanodeID nodeId,
      String basePath, int numberOfStorageReport,
      int numberOfMetadataStorageReport) {
    List<StorageReportProto> storageReports = new ArrayList<>();
    for (int i = 0; i < numberOfStorageReport; i++) {
      storageReports.add(getRandomStorageReport(nodeId,
          basePath + File.separator + "data-" + i));
    }
    List<MetadataStorageReportProto> metadataStorageReports =
        new ArrayList<>();
    for (int i = 0; i < numberOfMetadataStorageReport; i++) {
      metadataStorageReports.add(getRandomMetadataStorageReport(
          basePath + File.separator + "metadata-" + i));
    }
    return createNodeReport(storageReports, metadataStorageReports);
  }

  /**
   * Creates NodeReport with the given storage reports.
   *
   * @param reports storage reports to be included in the node report.
   * @param metaReports metadata storage reports to be included
   *                    in the node report.
   * @return NodeReportProto
   */
  public static NodeReportProto createNodeReport(
      List<StorageReportProto> reports,
      List<MetadataStorageReportProto> metaReports) {
    NodeReportProto.Builder nodeReport = NodeReportProto.newBuilder();
    nodeReport.addAllStorageReport(reports);
    nodeReport.addAllMetadataStorageReport(metaReports);
    return nodeReport.build();
  }

  /**
   * Generates random storage report.
   *
   * @param nodeId datanode id for which the storage report belongs to
   * @param path   path of the storage
   *
   * @return StorageReportProto
   */
  public static StorageReportProto getRandomStorageReport(DatanodeID nodeId,
      String path) {
    return createStorageReport(nodeId, path,
        random.nextInt(1000),
        random.nextInt(500),
        random.nextInt(500),
        StorageTypeProto.DISK);
  }

  /**
   * Generates random metadata storage report.
   *
   * @param path path of the storage
   *
   * @return MetadataStorageReportProto
   */
  public static MetadataStorageReportProto getRandomMetadataStorageReport(
      String path) {
    return createMetadataStorageReport(path,
        random.nextInt(1000),
        random.nextInt(500),
        random.nextInt(500),
        StorageTypeProto.DISK);
  }

  public static StorageReportProto createStorageReport(DatanodeID nodeId, String path,
      long capacity) {
    return createStorageReport(nodeId, path,
        capacity,
        0,
        capacity,
        StorageTypeProto.DISK);
  }

  public static List<StorageReportProto> createStorageReports(DatanodeID nodeID, long capacity, long remaining,
                                                              long committed) {
    return Collections.singletonList(
        StorageReportProto.newBuilder()
            .setStorageUuid(nodeID.toString())
            .setStorageLocation("test")
            .setCapacity(capacity)
            .setRemaining(remaining)
            .setCommitted(committed)
            .setScmUsed(200L - remaining)
            .build());
  }

  public static StorageReportProto createStorageReport(DatanodeID nodeId, String path,
       long capacity, long used, long remaining, StorageTypeProto type) {
    return createStorageReport(nodeId, path, capacity, used, remaining,
            type, false);
  }

  /**
   * Creates storage report with the given information.
   *
   * @param nodeId    datanode id
   * @param path      storage dir
   * @param capacity  storage size
   * @param used      space used
   * @param remaining space remaining
   * @param type      type of storage
   *
   * @return StorageReportProto
   */
  public static StorageReportProto createStorageReport(DatanodeID nodeId, String path,
      long capacity, long used, long remaining, StorageTypeProto type,
                                                       boolean failed) {
    Objects.requireNonNull(nodeId, "nodeId == null");
    Objects.requireNonNull(path, "path == null");
    StorageReportProto.Builder srb = StorageReportProto.newBuilder();
    srb.setStorageUuid(nodeId.toString())
        .setStorageLocation(path)
        .setCapacity(capacity)
        .setScmUsed(used)
        .setFailed(failed)
        .setRemaining(remaining);
    StorageTypeProto storageTypeProto =
        type == null ? StorageTypeProto.DISK : type;
    srb.setStorageType(storageTypeProto);
    return srb.build();
  }

  public static MetadataStorageReportProto createMetadataStorageReport(
      String path, long capacity) {
    return createMetadataStorageReport(path,
        capacity,
        0,
        capacity,
        StorageTypeProto.DISK, false);
  }

  public static MetadataStorageReportProto createMetadataStorageReport(
      String path, long capacity, long used, long remaining,
      StorageTypeProto type) {
    return createMetadataStorageReport(path, capacity, used, remaining,
        type, false);
  }

  /**
   * Creates metadata storage report with the given information.
   *
   * @param path      storage dir
   * @param capacity  storage size
   * @param used      space used
   * @param remaining space remaining
   * @param type      type of storage
   *
   * @return StorageReportProto
   */
  public static MetadataStorageReportProto createMetadataStorageReport(
      String path, long capacity, long used, long remaining,
      StorageTypeProto type, boolean failed) {
    Objects.requireNonNull(path, "path == null");
    MetadataStorageReportProto.Builder srb = MetadataStorageReportProto
        .newBuilder();
    srb.setStorageLocation(path)
        .setCapacity(capacity)
        .setScmUsed(used)
        .setFailed(failed)
        .setRemaining(remaining);
    StorageTypeProto storageTypeProto =
        type == null ? StorageTypeProto.DISK : type;
    srb.setStorageType(storageTypeProto);
    return srb.build();
  }

  /**
   * Generates random container reports.
   *
   * @return ContainerReportsProto
   */
  public static ContainerReportsProto getRandomContainerReports() {
    return getRandomContainerReports(1);
  }

  /**
   * Generates random container report with the given number of containers.
   *
   * @param numberOfContainers number of containers to be in container report
   *
   * @return ContainerReportsProto
   */
  public static ContainerReportsProto getRandomContainerReports(
      int numberOfContainers) {
    List<ContainerReplicaProto> containerInfos = new ArrayList<>();
    for (int i = 0; i < numberOfContainers; i++) {
      containerInfos.add(getRandomContainerInfo(i));
    }
    return getContainerReports(containerInfos);
  }

  public static PipelineReportsProto getRandomPipelineReports() {
    return PipelineReportsProto.newBuilder().build();
  }

  public static PipelineReportFromDatanode getPipelineReportFromDatanode(
      DatanodeDetails dn, PipelineID... pipelineIDs) {
    PipelineReportsProto.Builder reportBuilder =
        PipelineReportsProto.newBuilder();
    for (PipelineID pipelineID : pipelineIDs) {
      reportBuilder.addPipelineReport(
          PipelineReport.newBuilder()
              .setPipelineID(pipelineID.getProtobuf())
              .setIsLeader(false));
    }
    return new PipelineReportFromDatanode(dn, reportBuilder.build());
  }

  public static PipelineReportFromDatanode getPipelineReportFromDatanode(
      DatanodeDetails dn, PipelineID pipelineID, boolean isLeader) {
    PipelineReportsProto.Builder reportBuilder =
        PipelineReportsProto.newBuilder();
    reportBuilder.addPipelineReport(PipelineReport.newBuilder()
        .setPipelineID(pipelineID.getProtobuf()).setIsLeader(isLeader));
    return new PipelineReportFromDatanode(dn, reportBuilder.build());
  }

  public static void openAllRatisPipelines(PipelineManager pipelineManager)
      throws IOException, TimeoutException {
    // Pipeline is created by background thread
    for (ReplicationFactor factor : ReplicationFactor.values()) {
      // Trigger the processed pipeline report event
      for (Pipeline pipeline : pipelineManager
          .getPipelines(RatisReplicationConfig.getInstance(factor))) {
        pipelineManager.openPipeline(pipeline.getId());
      }
    }
  }

  public static PipelineActionsFromDatanode getPipelineActionFromDatanode(
      DatanodeDetails dn, PipelineID... pipelineIDs) {
    PipelineActionsProto.Builder actionsProtoBuilder =
        PipelineActionsProto.newBuilder();
    for (PipelineID pipelineID : pipelineIDs) {
      ClosePipelineInfo closePipelineInfo =
          ClosePipelineInfo.newBuilder().setPipelineID(pipelineID.getProtobuf())
              .setReason(ClosePipelineInfo.Reason.PIPELINE_FAILED)
              .setDetailedReason("").build();
      actionsProtoBuilder.addPipelineActions(PipelineAction.newBuilder()
          .setClosePipeline(closePipelineInfo)
          .setAction(PipelineAction.Action.CLOSE)
          .build());
    }
    return new PipelineActionsFromDatanode(dn, actionsProtoBuilder.build());
  }

  /**
   * Creates container report with the given ContainerInfo(s).
   *
   * @param containerInfos one or more ContainerInfo
   *
   * @return ContainerReportsProto
   */
  public static ContainerReportsProto getContainerReports(
      ContainerReplicaProto... containerInfos) {
    return getContainerReports(Arrays.asList(containerInfos));
  }

  /**
   * Creates container report with the given ContainerInfo(s).
   *
   * @param containerInfos list of ContainerInfo
   *
   * @return ContainerReportsProto
   */
  public static ContainerReportsProto getContainerReports(
      List<ContainerReplicaProto> containerInfos) {
    ContainerReportsProto.Builder
        reportsBuilder = ContainerReportsProto.newBuilder();
    for (ContainerReplicaProto containerInfo : containerInfos) {
      reportsBuilder.addReports(containerInfo);
    }
    return reportsBuilder.build();
  }

  /**
   * Generates random ContainerInfo.
   *
   * @param containerId container id of the ContainerInfo
   *
   * @return ContainerInfo
   */
  public static ContainerReplicaProto getRandomContainerInfo(
      long containerId) {
    return createContainerInfo(containerId,
        OzoneConsts.GB * 5,
        random.nextLong(1000),
        OzoneConsts.GB * random.nextInt(5),
        random.nextLong(1000),
        OzoneConsts.GB * random.nextInt(2),
        random.nextLong(1000),
        OzoneConsts.GB * random.nextInt(5));
  }

  /**
   * Creates ContainerInfo with the given details.
   *
   * @param containerId id of the container
   * @param size        size of container
   * @param keyCount    number of keys
   * @param bytesUsed   bytes used by the container
   * @param readCount   number of reads
   * @param readBytes   bytes read
   * @param writeCount  number of writes
   * @param writeBytes  bytes written
   *
   * @return ContainerInfo
   */
  @SuppressWarnings("parameternumber")
  public static ContainerReplicaProto createContainerInfo(
      long containerId, long size, long keyCount, long bytesUsed,
      long readCount, long readBytes, long writeCount, long writeBytes) {
    return ContainerReplicaProto.newBuilder()
        .setContainerID(containerId)
        .setState(ContainerReplicaProto.State.OPEN)
        .setSize(size)
        .setKeyCount(keyCount)
        .setUsed(bytesUsed)
        .setReadCount(readCount)
        .setReadBytes(readBytes)
        .setWriteCount(writeCount)
        .setWriteBytes(writeBytes)
        .build();
  }

  /**
   * Create Command Status report object.
   * @return CommandStatusReportsProto
   */
  public static CommandStatusReportsProto createCommandStatusReport(
      List<CommandStatus> reports) {
    CommandStatusReportsProto.Builder report = CommandStatusReportsProto
        .newBuilder();
    report.addAllCmdStatus(reports);
    return report.build();
  }

  public static org.apache.hadoop.hdds.scm.container.ContainerInfo
      allocateContainer(ContainerManager containerManager)
      throws IOException, TimeoutException {
    return containerManager
        .allocateContainer(RatisReplicationConfig
                .getInstance(ReplicationFactor.THREE),
            "root");

  }

  public static void closeContainer(ContainerManager containerManager,
        ContainerID id) throws IOException,
      InvalidStateTransitionException, TimeoutException {
    containerManager.updateContainerState(
        id, HddsProtos.LifeCycleEvent.FINALIZE);
    containerManager.updateContainerState(
        id, HddsProtos.LifeCycleEvent.CLOSE);

  }

  /**
   * Move the container to Quaise close state.
   * @param containerManager
   * @param id
   * @throws IOException
   */
  public static void quasiCloseContainer(ContainerManager containerManager,
       ContainerID id) throws IOException,
      InvalidStateTransitionException, TimeoutException {
    containerManager.updateContainerState(
        id, HddsProtos.LifeCycleEvent.FINALIZE);
    containerManager.updateContainerState(
        id, HddsProtos.LifeCycleEvent.QUASI_CLOSE);

  }

  /**
   * Construct and returns StorageContainerManager instance using the given
   * configuration.
   *
   * @param conf OzoneConfiguration
   * @return StorageContainerManager instance
   * @throws IOException
   * @throws AuthenticationException
   */
  public static StorageContainerManager getScmSimple(OzoneConfiguration conf)
      throws IOException, AuthenticationException {
    // The default behaviour whether ratis will be enabled or not
    // in SCM will be inferred from ozone-default.xml.
    return getScmSimple(conf, new SCMConfigurator());
  }

  /**
   * Construct and returns StorageContainerManager instance using the given
   * configuration and service configurator.
   *
   * @param conf OzoneConfiguration
   * @return StorageContainerManager instance
   * @throws IOException
   * @throws AuthenticationException
   */
  public static StorageContainerManager getScmSimple(OzoneConfiguration conf,
      SCMConfigurator configurator) throws IOException,
      AuthenticationException {
    return StorageContainerManager.createSCM(conf, configurator);
  }

  /**
   * Construct and returns StorageContainerManager instance using the given
   * configuration. The ports used by this StorageContainerManager are
   * randomly selected from free ports available.
   *
   * @param conf OzoneConfiguration
   * @return StorageContainerManager instance
   * @throws IOException
   * @throws AuthenticationException
   */
  public static StorageContainerManager getScm(OzoneConfiguration conf)
      throws IOException, AuthenticationException {
    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setSCMHAManager(SCMHAManagerStub.getInstance(true));
    configurator.setScmContext(SCMContext.emptyContext());
    return getScm(conf, configurator);
  }

  /**
   * Construct and returns StorageContainerManager instance using the given
   * configuration and the configurator. The ports used by this
   * StorageContainerManager are randomly selected from free ports available.
   *
   * @param conf OzoneConfiguration
   * @param configurator SCMConfigurator
   * @return StorageContainerManager instance
   * @throws IOException
   * @throws AuthenticationException
   */
  public static StorageContainerManager getScm(OzoneConfiguration conf,
                                               SCMConfigurator configurator)
      throws IOException, AuthenticationException {
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY, "127.0.0.1:0");
    SCMStorageConfig scmStore = new SCMStorageConfig(conf);
    if (scmStore.getState() != Storage.StorageState.INITIALIZED) {
      String clusterId = UUID.randomUUID().toString();
      String scmId = UUID.randomUUID().toString();
      scmStore.setClusterId(clusterId);
      scmStore.setScmId(scmId);
      scmStore.setSCMHAFlag(true);
      // writes the version file properties
      scmStore.initialize();
    }
    return StorageContainerManager.createSCM(conf, configurator);
  }

  private static ContainerInfo.Builder getDefaultContainerInfoBuilder(
      final HddsProtos.LifeCycleState state) {
    return new ContainerInfo.Builder()
        .setContainerID(RandomUtils.secure().randomLong())
        .setReplicationConfig(
            RatisReplicationConfig
                .getInstance(ReplicationFactor.THREE))
        .setState(state)
        .setSequenceId(10000L)
        .setOwner("TEST");
  }

  public static ContainerInfo getContainer(
      final HddsProtos.LifeCycleState state) {
    return getDefaultContainerInfoBuilder(state)
        .setPipelineID(randomPipelineID)
        .build();
  }

  public static ContainerInfo getContainer(
      final HddsProtos.LifeCycleState state, PipelineID pipelineID) {
    return getDefaultContainerInfoBuilder(state)
        .setPipelineID(pipelineID)
        .build();
  }

  public static ContainerInfo getECContainer(
      final HddsProtos.LifeCycleState state, PipelineID pipelineID,
      ECReplicationConfig replicationConfig) {
    return getDefaultContainerInfoBuilder(state)
        .setReplicationConfig(replicationConfig)
        .setPipelineID(pipelineID)
        .build();
  }

  public static Set<ContainerReplica> getReplicas(
      final ContainerID containerId,
      final ContainerReplicaProto.State state,
      final DatanodeDetails... datanodeDetails) {
    return getReplicas(containerId, state, 10000L, datanodeDetails);
  }

  public static Set<ContainerReplica> getReplicas(
          final ContainerID containerId,
          final ContainerReplicaProto.State state,
          final long sequenceId,
          final DatanodeDetails... datanodeDetails) {
    return Sets.newHashSet(getReplicas(containerId, state, sequenceId,
            Arrays.asList(datanodeDetails)));
  }

  public static List<ContainerReplica> getReplicas(
      final ContainerID containerId,
      final ContainerReplicaProto.State state,
      final long sequenceId,
      final Iterable<DatanodeDetails> datanodeDetails) {
    List<ContainerReplica> replicas = new ArrayList<>();
    for (DatanodeDetails datanode : datanodeDetails) {
      replicas.add(getReplicas(containerId, state,
          sequenceId, datanode.getID(), datanode));
    }
    return replicas;
  }

  public static ContainerReplica getReplicas(
      final ContainerID containerId,
      final ContainerReplicaProto.State state,
      final long sequenceId,
      final DatanodeID originNodeId,
      final DatanodeDetails datanodeDetails) {
    return getReplicaBuilder(containerId, state, CONTAINER_USED_BYTES_DEFAULT,
            CONTAINER_NUM_KEYS_DEFAULT, sequenceId, originNodeId,
            datanodeDetails).build();
  }

  public static ContainerReplica.ContainerReplicaBuilder getReplicaBuilder(
          final ContainerID containerId,
          final ContainerReplicaProto.State state,
          final long usedBytes,
          final long keyCount,
          final long sequenceId,
          final DatanodeID originNodeId,
          final DatanodeDetails datanodeDetails) {
    return ContainerReplica.newBuilder()
            .setContainerID(containerId).setContainerState(state)
            .setDatanodeDetails(datanodeDetails)
            .setOriginNodeId(originNodeId).setSequenceId(sequenceId)
            .setBytesUsed(usedBytes)
            .setKeyCount(keyCount)
            .setEmpty(keyCount == 0);
  }

  public static List<ContainerReplica> getReplicasWithReplicaIndex(
          final ContainerID containerId,
          final ContainerReplicaProto.State state,
          final long usedBytes,
          final long keyCount,
          final long sequenceId,
          final Iterable<DatanodeDetails> datanodeDetails) {
    List<ContainerReplica> replicas = new ArrayList<>();
    int replicaIndex = 1;
    for (DatanodeDetails datanode : datanodeDetails) {
      replicas.add(getReplicaBuilder(containerId, state,
              usedBytes, keyCount, sequenceId, datanode.getID(), datanode)
              .setReplicaIndex(replicaIndex).build());
      replicaIndex += 1;
    }
    return replicas;
  }

  public static Set<ContainerReplica> getReplicasWithReplicaIndex(
          final ContainerID containerId,
          final ContainerReplicaProto.State state,
          final long usedBytes,
          final long keyCount,
          final long sequenceId,
          final DatanodeDetails... datanodeDetails) {
    return Sets.newHashSet(getReplicasWithReplicaIndex(containerId, state,
            usedBytes, keyCount, sequenceId, Arrays.asList(datanodeDetails)));
  }

  public static Pipeline getRandomPipeline() {
    List<DatanodeDetails> nodes = new ArrayList<>();
    nodes.add(MockDatanodeDetails.randomDatanodeDetails());
    nodes.add(MockDatanodeDetails.randomDatanodeDetails());
    nodes.add(MockDatanodeDetails.randomDatanodeDetails());
    return Pipeline.newBuilder()
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setId(PipelineID.randomId())
        .setNodes(nodes)
        .setState(Pipeline.PipelineState.OPEN)
        .build();
  }

  /**
   * Create Command Status report object.
   *
   * @param numOfContainers number of containers to be included in report.
   * @return CommandStatusReportsProto
   */
  public static SCMDatanodeProtocolServer.NodeRegistrationContainerReport
      createNodeRegistrationContainerReport(int numOfContainers) {
    return new SCMDatanodeProtocolServer.NodeRegistrationContainerReport(
        MockDatanodeDetails.randomDatanodeDetails(),
        getRandomContainerReports(numOfContainers));
  }

  /**
   * Create NodeRegistrationContainerReport object.
   *
   * @param dnContainers List of containers to be included in report
   * @return NodeRegistrationContainerReport
   */
  public static SCMDatanodeProtocolServer.NodeRegistrationContainerReport
      createNodeRegistrationContainerReport(List<ContainerInfo> dnContainers) {
    List<ContainerReplicaProto>
        containers = new ArrayList<>();
    dnContainers.forEach(c -> {
      containers.add(getRandomContainerInfo(c.getContainerID()));
    });
    return new SCMDatanodeProtocolServer.NodeRegistrationContainerReport(
        MockDatanodeDetails.randomDatanodeDetails(),
        getContainerReports(containers));
  }

  /**
   * Creates list of ContainerInfo.
   *
   * @param numContainers number of ContainerInfo to be included in list.
   * @return {@literal List<ContainerInfo>}
   */
  public static List<ContainerInfo> getContainerInfo(int numContainers) {
    List<ContainerInfo> containerInfoList = new ArrayList<>();
    RatisReplicationConfig ratisReplicationConfig =
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE);
    for (int i = 0; i < numContainers; i++) {
      ContainerInfo.Builder builder = new ContainerInfo.Builder();
      containerInfoList.add(builder
          .setContainerID(RandomUtils.secure().randomLong())
          .setReplicationConfig(ratisReplicationConfig)
          .build());
    }
    return containerInfoList;
  }

  /**
   * Generate EC Container data.
   *
   * @param numContainers number of ContainerInfo to be included in list.
   * @param data Data block Num.
   * @param parity Parity block Num.
   * @return {@literal List<ContainerInfo>}
   */
  public static List<ContainerInfo> getECContainerInfo(int numContainers, int data, int parity) {
    List<ContainerInfo> containerInfoList = new ArrayList<>();
    ECReplicationConfig eCReplicationConfig = new ECReplicationConfig(data, parity);
    for (int i = 0; i < numContainers; i++) {
      ContainerInfo.Builder builder = new ContainerInfo.Builder();
      containerInfoList.add(builder
          .setContainerID(RandomUtils.secure().randomLong())
          .setOwner("test-owner")
          .setPipelineID(PipelineID.randomId())
          .setReplicationConfig(eCReplicationConfig)
          .build());
    }
    return containerInfoList;
  }

  public static ContainerReplicaProto createContainerReplica(
          ContainerID containerId, ContainerReplicaProto.State state,
          String originNodeId, long usedBytes, long keyCount,
          int replicaIndex) {

    return ContainerReplicaProto.newBuilder()
                    .setContainerID(containerId.getId())
                    .setState(state)
                    .setOriginNodeId(originNodeId)
                    .setFinalhash("e16cc9d6024365750ed8dbd194ea46d2")
                    .setSize(5368709120L)
                    .setUsed(usedBytes)
                    .setKeyCount(keyCount)
                    .setReadCount(100000000L)
                    .setWriteCount(100000000L)
                    .setReadBytes(2000000000L)
                    .setWriteBytes(2000000000L)
                    .setBlockCommitSequenceId(10000L)
                    .setDeleteTransactionId(0)
                    .setReplicaIndex(replicaIndex)
                    .build();
  }

  public static void mockRemoteUser(UserGroupInformation ugi) {
    Server.Call call = spy(new Server.Call(1, 1, null, null,
        RPC.RpcKind.RPC_BUILTIN, new byte[] {1, 2, 3}));
    when(call.getRemoteUser()).thenReturn(ugi);
    Server.getCurCall().set(call);
  }
}
