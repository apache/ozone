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

package org.apache.hadoop.hdds.scm.container.balancer;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.DELETED;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Container2BCSIDMapProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.XceiverClientSpi.Validator;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.OzoneTestHelper;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.ContainerStateMachine;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext.Op;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext.WriteChunkStage;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.metadata.ContainerCreateInfo;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.StatemachineImplTestUtil;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A container replica deleted from a datanode must not come back, whether a stale write from the Ratis pipeline arrives
 * after a balancer move or the datanode restarts while its Ratis snapshot still lists the container.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TestDeletedContainerNotRecreated {
  private static final Logger LOG = LoggerFactory.getLogger(TestDeletedContainerNotRecreated.class);
  private static final ReplicationConfig RATIS_THREE = RatisReplicationConfig.getInstance(
      HddsProtos.ReplicationFactor.THREE);

  private static MiniOzoneCluster cluster;
  private static StorageContainerManager scm;
  private static OzoneClient client;
  private static OzoneBucket bucket;
  private static XceiverClientManager clientManager;

  @BeforeAll
  static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 200, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 200, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 200, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 6, TimeUnit.SECONDS);
    conf.setInt(OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    RatisClientConfig ratisClientConfig = conf.getObject(RatisClientConfig.class);
    ratisClientConfig.setWriteRequestTimeout(Duration.ofSeconds(10));
    ratisClientConfig.setWatchRequestTimeout(Duration.ofSeconds(10));
    conf.setFromObject(ratisClientConfig);
    DatanodeRatisServerConfig ratisServerConfig = conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(10));
    conf.setFromObject(ratisServerConfig);

    // Four datanodes: the Ratis pipeline has three, so one datanode is outside it and can receive the moved replica.
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(4).build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.THREE, 60000);
    scm = cluster.getStorageContainerManager();
    client = cluster.newClient();
    client.getObjectStore().createVolume("vol");
    client.getObjectStore().getVolume("vol").createBucket("bucket");
    bucket = client.getObjectStore().getVolume("vol").getBucket("bucket");
    clientManager = new XceiverClientManager(conf);
  }

  @AfterAll
  static void shutdown() {
    IOUtils.closeQuietly(clientManager, client, cluster);
  }

  /**
   * A balancer move deletes the replica on the leader of the container's pipeline while the pipeline stays open. A
   * write for a block that SCM allocated before the container closed must then not make that datanode create the
   * replica again.
   */
  @Test
  @Order(1)
  void staleWriteAfterMoveDoesNotRecreateReplica() throws Exception {
    // Container C on Ratis pipeline P. DN1 is the leader of P, DN4 is the datanode outside P.
    byte[] data = writeKey("moved");
    OzoneKeyLocation location = keyLocation("moved");
    long containerId = location.getContainerID();
    ContainerInfo info = scm.getContainerManager().getContainer(ContainerID.valueOf(containerId));
    Pipeline pipeline = scm.getPipelineManager().getPipeline(info.getPipelineID());
    DatanodeDetails dn1 = waitForLeader(pipeline);
    DatanodeDetails dn4 = cluster.getHddsDatanodes().stream().map(HddsDatanodeService::getDatanodeDetails)
        .filter(dn -> !pipeline.getNodes().contains(dn)).findFirst().get();

    // While C is open, take a second block of C from SCM and build the write that a client holding it would send.
    ContainerCommandRequestProto staleWrite = ContainerTestHelper.getWriteChunkRequest(
        pipeline, allocateBlockIn(info), 1024);

    assertTrue(hasContainerDir(dn1, containerId), "no directory for the container on DN1 before the move");
    // Close C but not P, then move the replica from DN1 to DN4.
    closeContainerAndWait(info);
    assertTrue(scm.getPipelineManager().getPipeline(pipeline.getId()).isOpen(), "pipeline closed with its container");
    CompletableFuture<MoveManager.MoveResult> move = scm.getMoveManager().move(info.containerID(), dn1, dn4);
    GenericTestUtils.waitFor(() -> container(dn1).getContainerSet().getContainer(containerId) == null, 100, 60000);
    assertEquals(MoveManager.MoveResult.COMPLETED, move.get(60, TimeUnit.SECONDS));

    // DN1 has deleted the replica but still witnesses the container.
    assertNoReplicaNotMissing(dn1, containerId);
    assertDeletedEntry(dn1, containerId);
    assertFalse(hasContainerDir(dn1, containerId), "directory of the deleted container is left on DN1");

    // The stale write reaches DN1 as the leader of P. It must not succeed and must not create anything on DN1. Capture
    // the dispatcher log around this one write: DN1 itself must refuse it, in its chunk-writer thread.
    assertTrue(isLeader(dn1, pipeline), "DN1 is no longer the leader of the pipeline");
    boolean acknowledged;
    String log;
    org.apache.log4j.Logger dispatcherLog = org.apache.log4j.Logger.getLogger(HddsDispatcher.class);
    Level priorLevel = dispatcherLog.getLevel();
    dispatcherLog.setLevel(Level.DEBUG);
    try (LogCapturer logs = LogCapturer.captureLogs(dispatcherLog, new PatternLayout("[%t] %m%n"))) {
      acknowledged = writeSucceeds(pipeline, staleWrite);
      log = logs.getOutput();
    } finally {
      dispatcherLog.setLevel(priorLevel);
    }
    long refusals = countRefusals(log, dn1, containerId);
    LOG.info("DN1 {} refused the stale WriteChunk of container {} in {} log lines", dn1, containerId, refusals);
    assertAll(
        () -> assertTrue(refusals > 0, "DN1 did not refuse the stale WriteChunk with CLOSED_CONTAINER_IO"),
        () -> assertFalse(acknowledged, "the stale WriteChunk was acknowledged"),
        // Both stages are refused as for a closed container, so the write does not fail DN1's state machine.
        () -> assertTrue(((ContainerStateMachine) OzoneTestHelper.getStateMachine(
            cluster.getHddsDatanodes().get(cluster.getHddsDatanodeIndex(dn1)), pipeline)).isStateMachineHealthy(),
            "the stale WriteChunk made DN1's state machine for the pipeline unhealthy"),
        () -> assertNull(container(dn1).getContainerSet().getContainer(containerId), "DN1 recreated the replica"),
        () -> assertFalse(hasContainerDir(dn1, containerId), "DN1 created a directory for the container"),
        () -> assertDeletedEntry(dn1, containerId),
        // The data written before the move is still on DN4, read from DN4 alone.
        () -> assertArrayEquals(data, readBlockFrom(dn4, new BlockID(containerId, location.getLocalID()))));
  }

  /**
   * The Ratis snapshot of a pipeline lists a container whose replica was deleted from a datanode. When that datanode
   * restarts it must not mark the container missing, and it must refuse a write for the container.
   */
  @Test
  @Order(2)
  void restartWithDeletedContainerInRatisSnapshotRefusesWrite() throws Exception {
    // Keep SCM from replicating the deleted replica back.
    scm.getReplicationManager().stop();

    // A container with all its writes finished.
    writeKey("snapshotted");
    long containerId = keyLocation("snapshotted").getContainerID();
    ContainerInfo info = scm.getContainerManager().getContainer(ContainerID.valueOf(containerId));
    Pipeline pipeline = scm.getPipelineManager().getPipeline(info.getPipelineID());
    DatanodeDetails dn = pipeline.getFirstNode();
    closeContainerAndWait(info);

    // Delete the closed replica from the datanode, then snapshot the state machine of the pipeline. The snapshot still
    // holds the container ID in its Container2BCSID map.
    assertTrue(hasContainerDir(dn, containerId), "no directory for the container before the delete");
    container(dn).getController().deleteContainer(containerId, true);
    assertNull(container(dn).getContainerSet().getContainer(containerId), "the replica was not deleted");
    ContainerStateMachine stateMachine = (ContainerStateMachine) OzoneTestHelper.getStateMachine(
        cluster.getHddsDatanodes().get(cluster.getHddsDatanodeIndex(dn)), pipeline);
    stateMachine.takeSnapshot();
    File snapshot = StatemachineImplTestUtil.findLatestSnapshot(
        (SimpleStateMachineStorage) stateMachine.getStateMachineStorage()).getFile().getPath().toFile();
    try (InputStream in = Files.newInputStream(snapshot.toPath())) {
      assertTrue(Container2BCSIDMapProto.parseFrom(in).containsContainer2BCSID(containerId),
          "the snapshot does not hold container " + containerId);
    }

    cluster.restartHddsDatanode(dn, true);

    // The Ratis snapshot names the container, but the datanode knows that it deleted it.
    assertNoReplicaNotMissing(dn, containerId);
    assertDeletedEntry(dn, containerId);

    // A write for the container is refused in both stages and creates nothing.
    ContainerCommandRequestProto write = ContainerTestHelper.getWriteChunkRequest(
        pipeline, ContainerTestHelper.getTestBlockID(containerId), 1024);
    ContainerCommandRequestProto commit = write.toBuilder()
        .setWriteChunk(write.getWriteChunk().toBuilder().clearData()).build();
    Map<Long, Long> container2BCSID = new ConcurrentHashMap<>();
    HddsDispatcher dispatcher = (HddsDispatcher) container(dn).getDispatcher();
    Result writeStage = dispatcher.dispatch(write, DispatcherContext.newBuilder(Op.WRITE_STATE_MACHINE_DATA)
        .setStage(WriteChunkStage.WRITE_DATA).setContainer2BCSIDMap(container2BCSID).build()).getResult();
    Result commitStage = dispatcher.dispatch(commit, DispatcherContext.newBuilder(Op.APPLY_TRANSACTION)
        .setStage(WriteChunkStage.COMMIT_DATA).setContainer2BCSIDMap(container2BCSID).build()).getResult();
    assertAll(
        () -> assertEquals(Result.CLOSED_CONTAINER_IO, writeStage, "write stage"),
        () -> assertEquals(Result.CLOSED_CONTAINER_IO, commitStage, "commit stage"),
        () -> assertNull(container(dn).getContainerSet().getContainer(containerId), "the replica was recreated"),
        () -> assertFalse(hasContainerDir(dn, containerId), "a directory was created for the container"),
        () -> assertDeletedEntry(dn, containerId));
  }

  /** Writes a small RATIS/THREE key and returns what was written. */
  private static byte[] writeKey(String keyName) throws IOException {
    byte[] data = new byte[2048];
    ThreadLocalRandom.current().nextBytes(data);
    try (OzoneOutputStream out = bucket.createKey(keyName, data.length, RATIS_THREE, new HashMap<>())) {
      out.write(data);
    }
    return data;
  }

  private static OzoneKeyLocation keyLocation(String keyName) throws IOException {
    List<OzoneKeyLocation> locations = bucket.getKey(keyName).getOzoneKeyLocations();
    assertEquals(1, locations.size());
    return locations.get(0);
  }

  /** The service currently running as the datanode, which changes when the datanode is restarted. */
  private static OzoneContainer container(DatanodeDetails dn) {
    try {
      return cluster.getHddsDatanodes().get(cluster.getHddsDatanodeIndex(dn)).getDatanodeStateMachine().getContainer();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Checks the witnessed container DB entry of a deleted container: kept, with state DELETED and no replica index. */
  private static void assertDeletedEntry(DatanodeDetails dn, long containerId) throws IOException {
    ContainerCreateInfo entry = container(dn).getWitnessedContainerMetadataStore().getContainerCreateInfoTable()
        .get(ContainerID.valueOf(containerId));
    assertNotNull(entry, "the witnessed entry of deleted container " + containerId + " is gone");
    assertEquals(DELETED, entry.getState(), "state of the witnessed entry of container " + containerId);
    assertEquals(ContainerCreateInfo.INVALID_REPLICA_INDEX, entry.getReplicaIndex(),
        "replica index of the witnessed entry of container " + containerId);
  }

  /** Checks that the datanode holds no replica of the container and has not marked it missing. */
  private static void assertNoReplicaNotMissing(DatanodeDetails dn, long containerId) throws IOException {
    ContainerSet containerSet = container(dn).getContainerSet();
    assertNull(containerSet.getContainer(containerId), "datanode holds a replica of container " + containerId);
    assertFalse(containerSet.getMissingContainerSet().contains(containerId),
        "container " + containerId + " is in the missing container set");
  }

  /**
   * Counts the log lines of the datanode's chunk-writer threads that show a WriteChunk for the container failing to
   * create it with CLOSED_CONTAINER_IO. The log must use the layout "[%t] %m%n".
   */
  private static long countRefusals(String log, DatanodeDetails dn, long containerId) {
    String thread = "[" + dn.threadNamePrefix() + "ChunkWriter-";
    return log.lines().filter(line -> line.startsWith(thread) && line.contains("Operation: WriteChunk")
        && line.contains("Message: ContainerID " + containerId + " creation failed")
        && line.contains("CLOSED_CONTAINER_IO")).count();
  }

  /** True if any volume of the datanode has a directory for the container. */
  private static boolean hasContainerDir(DatanodeDetails dn, long containerId) throws IOException {
    for (StorageVolume volume : container(dn).getVolumeSet().getVolumesList()) {
      String base = KeyValueContainerLocationUtil.getBaseContainerLocation(
          ((HddsVolume) volume).getHddsRootDir().getPath(), volume.getClusterID(), containerId);
      if (new File(base).exists()) {
        return true;
      }
    }
    return false;
  }

  private static boolean isLeader(DatanodeDetails dn, Pipeline pipeline) {
    try {
      XceiverServerRatis server = (XceiverServerRatis) container(dn).getWriteChannel();
      return server.getServerDivision(RaftGroupId.valueOf(pipeline.getId().getId())).getInfo().isLeader();
    } catch (IOException e) {
      return false;
    }
  }

  /** Waits until a datanode of the pipeline is the Ratis leader of it and returns that datanode. */
  private static DatanodeDetails waitForLeader(Pipeline pipeline) throws Exception {
    GenericTestUtils.waitFor(() -> pipeline.getNodes().stream().anyMatch(dn -> isLeader(dn, pipeline)), 100, 30000);
    return pipeline.getNodes().stream().filter(dn -> isLeader(dn, pipeline)).findFirst().get();
  }

  /** Closes the container in SCM without closing its pipeline and waits until its replicas are closed. */
  private static void closeContainerAndWait(ContainerInfo info) throws Exception {
    ContainerID id = info.containerID();
    scm.getEventQueue().fireEvent(SCMEvents.CLOSE_CONTAINER, id);
    OzoneTestHelper.waitForContainerStateInSCM(scm, id, HddsProtos.LifeCycleState.CLOSED);
    OzoneTestHelper.waitForReplicaState(scm.getContainerManager(), id, 3, ContainerReplicaProto.State.CLOSED);
    for (DatanodeDetails dn : scm.getPipelineManager().getPipeline(info.getPipelineID()).getNodes()) {
      GenericTestUtils.waitFor(() -> OzoneTestHelper.isContainerClosed(cluster, info.getContainerID(), dn), 100, 30000);
    }
  }

  /** Gets another block of the container from SCM, as a client holding an old allocation would have. */
  private static BlockID allocateBlockIn(ContainerInfo info) throws Exception {
    for (int i = 0; i < 30; i++) {
      AllocatedBlock block = scm.getScmBlockManager().allocateBlock(1, RATIS_THREE, info.getOwner(), new ExcludeList());
      if (block != null && block.getBlockID().getContainerID() == info.getContainerID()) {
        return new BlockID(block.getBlockID());
      }
    }
    throw new AssertionError("SCM did not allocate another block in container " + info.getContainerID());
  }

  /** Reads a block from one datanode, with a client that talks to that datanode only. */
  private static byte[] readBlockFrom(DatanodeDetails dn, BlockID blockID) throws IOException {
    Pipeline pipeline = MockPipeline.createPipeline(Collections.singleton(dn));
    Validator validator = (request, response) -> ContainerProtocolCalls.validateContainerResponse(response);
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    try (XceiverClientSpi direct = clientManager.acquireClient(pipeline)) {
      BlockData block = ContainerProtocolCalls.getBlockFromDatanode(direct, blockID, null, dn, pipeline).getBlockData();
      for (ChunkInfo chunk : block.getChunksList()) {
        ReadChunkResponseProto response = ContainerProtocolCalls.readChunk(
            direct, chunk, block.getBlockID(), Collections.singletonList(validator), null);
        if (response.hasData()) {
          data.write(response.getData().toByteArray());
        } else {
          for (ByteString buffer : response.getDataBuffers().getBuffersList()) {
            data.write(buffer.toByteArray());
          }
        }
      }
    }
    return data.toByteArray();
  }

  /** Sends the request through the pipeline; false if the client gets an error response or an exception. */
  private static boolean writeSucceeds(Pipeline pipeline, ContainerCommandRequestProto request) throws IOException {
    try (XceiverClientSpi ratisClient = clientManager.acquireClient(pipeline)) {
      try {
        ContainerCommandResponseProto response = ratisClient.sendCommand(request);
        LOG.info("{} on container {} gave {}", request.getCmdType(), request.getContainerID(), response.getResult());
        return response.getResult() == Result.SUCCESS;
      } catch (IOException e) {
        LOG.info("{} on container {} failed", request.getCmdType(), request.getContainerID(), e);
        return false;
      }
    }
  }
}
