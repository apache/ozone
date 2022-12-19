/**
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
package org.apache.hadoop.ozone.container;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCoordinator;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionSupervisor;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_RECOVERING_CONTAINER_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_RECOVERING_CONTAINER_TIMEOUT_DEFAULT;

/**
 * Tests the EC recovery and over replication processing.
 */
public class TestECContainerRecovery {
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf = new OzoneConfiguration();
  private static OzoneClient client;
  private static ObjectStore objectStore;
  private static int chunkSize;
  private static int flushSize;
  private static int maxFlushSize;
  private static int blockSize;
  private static String volumeName;
  private static String bucketName;
  private static int dataBlocks = 3;
  private static byte[][] inputChunks = new byte[dataBlocks][chunkSize];

  private static final Logger LOG =
          LoggerFactory.getLogger(TestECContainerRecovery.class);
  /**
   * Create a MiniDFSCluster for testing.
   */
  @BeforeAll
  public static void init() throws Exception {
    chunkSize = 1024;
    flushSize = 2 * chunkSize;
    maxFlushSize = 2 * flushSize;
    blockSize = 2 * maxFlushSize;

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumType(ContainerProtos.ChecksumType.NONE);
    clientConfig.setStreamBufferFlushDelay(false);
    conf.setFromObject(clientConfig);
    DatanodeConfiguration datanodeConfiguration =
            conf.getObject(DatanodeConfiguration.class);
    datanodeConfiguration.setRecoveringContainerScrubInterval(
            Duration.of(10, ChronoUnit.SECONDS));
    conf.setFromObject(datanodeConfiguration);
    ReplicationManager.ReplicationManagerConfiguration rmConfig = conf
            .getObject(
                    ReplicationManager.ReplicationManagerConfiguration.class);
    //Setting all the intervals to 10 seconds current tests have timeout
    // of 100s.
    rmConfig.setUnderReplicatedInterval(Duration.of(10,
            ChronoUnit.SECONDS));
    rmConfig.setOverReplicatedInterval(Duration.of(10,
            ChronoUnit.SECONDS));
    rmConfig.setInterval(Duration.of(10, ChronoUnit.SECONDS));
    conf.setFromObject(rmConfig);
    conf.set(ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL, "1s");
    conf.set(ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL, "1s");
    conf.set(HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL, "1s");
    conf.set(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, "1s");
    conf.setTimeDuration("hdds.ratis.raft.server.rpc.slowness.timeout", 300,
        TimeUnit.SECONDS);
    conf.setTimeDuration(
        "hdds.ratis.raft.server.notification.no-leader.timeout", 300,
        TimeUnit.SECONDS);
    conf.setQuietMode(false);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 4,
        StorageUnit.MB);
    conf.setTimeDuration(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, 500,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL, 1,
        TimeUnit.SECONDS);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(10)
        .setTotalPipelineNumLimit(10).setBlockSize(blockSize)
        .setChunkSize(chunkSize).setStreamBufferFlushSize(flushSize)
        .setStreamBufferMaxSize(maxFlushSize)
        .setStreamBufferSizeUnit(StorageUnit.BYTES).build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    volumeName = UUID.randomUUID().toString();
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
    initInputChunks();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private OzoneBucket getOzoneBucket() throws IOException {
    String myBucket = UUID.randomUUID().toString();
    OzoneVolume volume = objectStore.getVolume(volumeName);
    final BucketArgs.Builder bucketArgs = BucketArgs.newBuilder();
    bucketArgs.setDefaultReplicationConfig(
        new DefaultReplicationConfig(ReplicationType.EC,
            new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
                chunkSize)));

    volume.createBucket(myBucket, bucketArgs.build());
    return volume.getBucket(myBucket);
  }

  private static void initInputChunks() {
    for (int i = 0; i < dataBlocks; i++) {
      inputChunks[i] = getBytesWith(i + 1, chunkSize);
    }
  }

  private static byte[] getBytesWith(int singleDigitNumber, int total) {
    StringBuilder builder = new StringBuilder(singleDigitNumber);
    for (int i = 1; i <= total; i++) {
      builder.append(singleDigitNumber);
    }
    return builder.toString().getBytes(UTF_8);
  }

  @Test
  public void testContainerRecoveryOverReplicationProcessing()
      throws Exception {
    byte[] inputData = getInputBytes(3);
    final OzoneBucket bucket = getOzoneBucket();
    String keyName = UUID.randomUUID().toString();
    final Pipeline pipeline;
    ECReplicationConfig repConfig =
        new ECReplicationConfig(3, 2,
            ECReplicationConfig.EcCodec.RS, chunkSize);
    try (OzoneOutputStream out = bucket
        .createKey(keyName, 1024, repConfig, new HashMap<>())) {
      out.write(inputData);
      pipeline =
          ((ECKeyOutputStream) out.getOutputStream()).getStreamEntries().get(0)
              .getPipeline();
    }

    List<ContainerInfo> containers =
        cluster.getStorageContainerManager().getContainerManager()
            .getContainers();
    ContainerInfo container = null;
    for (ContainerInfo info : containers) {
      if (info.getPipelineID().getId().equals(pipeline.getId().getId())) {
        container = info;
      }
    }
    StorageContainerManager scm = cluster.getStorageContainerManager();

    // Shutting down DN triggers close pipeline and close container.
    cluster.shutdownHddsDatanode(pipeline.getFirstNode());

    // Make sure container closed.
    waitForSCMContainerState(StorageContainerDatanodeProtocolProtos
        .ContainerReplicaProto.State.CLOSED, container.containerID());
    //Temporarily stop the RM process.
    scm.getReplicationManager().stop();

    // Wait for the lower replication.
    waitForContainerCount(4, container.containerID(), scm);

    // Start the RM to resume the replication process and wait for the
    // reconstruction.
    scm.getReplicationManager().start();
    waitForContainerCount(5, container.containerID(), scm);

    // Let's verify for Over replications now.
    //Temporarily stop the RM process.
    scm.getReplicationManager().stop();

    // Restart the DN to make the over replication and expect replication to be
    // increased.
    cluster.restartHddsDatanode(pipeline.getFirstNode(), true);
    // Check container is over replicated.
    waitForContainerCount(6, container.containerID(), scm);
    // Wait for all the replicas to be closed.
    container = scm.getContainerInfo(container.getContainerID());
    waitForDNContainerState(container, scm);

    // Resume RM and wait the over replicated replica deleted.
    scm.getReplicationManager().start();
    waitForContainerCount(5, container.containerID(), scm);
  }

  @Flaky("HDDS-7617")
  @Test
  public void testECContainerRecoveryWithTimedOutRecovery() throws Exception {
    byte[] inputData = getInputBytes(3);
    final OzoneBucket bucket = getOzoneBucket();
    String keyName = UUID.randomUUID().toString();
    final Pipeline pipeline;
    ECReplicationConfig repConfig =
            new ECReplicationConfig(3, 2,
                    ECReplicationConfig.EcCodec.RS, chunkSize);
    try (OzoneOutputStream out = bucket
            .createKey(keyName, 1024, repConfig, new HashMap<>())) {
      out.write(inputData);
      pipeline = ((ECKeyOutputStream) out.getOutputStream())
              .getStreamEntries().get(0).getPipeline();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    List<ContainerInfo> containers =
            cluster.getStorageContainerManager().getContainerManager()
                    .getContainers();
    ContainerInfo container = null;
    for (ContainerInfo info : containers) {
      if (info.getPipelineID().getId().equals(pipeline.getId().getId())) {
        container = info;
      }
    }
    StorageContainerManager scm = cluster.getStorageContainerManager();
    AtomicReference<HddsDatanodeService> reconstructedDN =
            new AtomicReference<>();
    ContainerInfo finalContainer = container;
    Map<HddsDatanodeService, Long> recoveryTimeoutMap = new HashMap<>();
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      recoveryTimeoutMap.put(dn, dn.getDatanodeStateMachine().getConf()
              .getTimeDuration(OZONE_RECOVERING_CONTAINER_TIMEOUT,
              OZONE_RECOVERING_CONTAINER_TIMEOUT_DEFAULT,
              TimeUnit.MILLISECONDS));
      dn.getDatanodeStateMachine().getContainer()
              .getContainerSet().setRecoveringTimeout(100);

      ECReconstructionSupervisor ecReconstructionSupervisor =
              GenericTestUtils.getFieldReflection(dn.getDatanodeStateMachine(),
                      "ecReconstructionSupervisor");
      ECReconstructionCoordinator coordinator = GenericTestUtils
              .mockFieldReflection(ecReconstructionSupervisor,
                      "reconstructionCoordinator");

      Mockito.doAnswer(invocation -> {
        GenericTestUtils.waitFor(() ->
                        dn.getDatanodeStateMachine()
                                .getContainer()
                                .getContainerSet()
                                .getContainer(finalContainer.getContainerID())
                                .getContainerState() ==
                        ContainerProtos.ContainerDataProto.State.UNHEALTHY,
                1000, 100000);
        reconstructedDN.set(dn);
        invocation.callRealMethod();
        return null;
      }).when(coordinator).reconstructECBlockGroup(Mockito.any(), Mockito.any(),
              Mockito.any(), Mockito.any());
    }

    // Shutting down DN triggers close pipeline and close container.
    cluster.shutdownHddsDatanode(pipeline.getFirstNode());



    // Make sure container closed.
    waitForSCMContainerState(StorageContainerDatanodeProtocolProtos
            .ContainerReplicaProto.State.CLOSED, container.containerID());
    //Temporarily stop the RM process.
    scm.getReplicationManager().stop();

    // Wait for the lower replication.
    waitForContainerCount(4, container.containerID(), scm);

    // Start the RM to resume the replication process and wait for the
    // reconstruction.
    scm.getReplicationManager().start();
    GenericTestUtils.waitFor(() -> reconstructedDN.get() != null, 10000,
            100000);
    GenericTestUtils.waitFor(() -> reconstructedDN.get()
            .getDatanodeStateMachine().getContainer().getContainerSet()
            .getContainer(finalContainer.getContainerID()) == null,
            10000, 100000);
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      dn.getDatanodeStateMachine().getContainer().getContainerSet()
              .setRecoveringTimeout(recoveryTimeoutMap.get(dn));
    }
  }

  private void waitForDNContainerState(ContainerInfo container,
      StorageContainerManager scm) throws InterruptedException,
      TimeoutException {
    GenericTestUtils.waitFor(() -> {
      try {
        List<ContainerReplica> unhealthyReplicas = scm.getContainerManager()
            .getContainerReplicas(container.containerID()).stream()
            .filter(r -> !ReplicationManager
                .compareState(container.getState(), r.getState()))
            .collect(Collectors.toList());
        return unhealthyReplicas.size() == 0;
      } catch (ContainerNotFoundException e) {
        return false;
      }
    }, 100, 100000);
  }

  private void waitForSCMContainerState(
      StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State state,
      ContainerID containerID) throws TimeoutException, InterruptedException {
    //Wait until container closed at SCM
    GenericTestUtils.waitFor(() -> {
      try {
        HddsProtos.LifeCycleState containerState = cluster
            .getStorageContainerManager().getContainerManager()
            .getContainer(containerID).getState();
        return ReplicationManager.compareState(containerState, state);
      } catch (IOException e) {
        return false;
      }
    }, 100, 100000);
  }

  private void waitForContainerCount(int count, ContainerID containerID,
      StorageContainerManager scm)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      try {
        return scm.getContainerManager()
            .getContainerReplicas(containerID)
            .size() == count;
      } catch (ContainerNotFoundException e) {
        return false;
      }
    }, 100, 100000);
  }

  private byte[] getInputBytes(int numChunks) {
    byte[] inputData = new byte[numChunks * chunkSize];
    for (int i = 0; i < numChunks; i++) {
      int start = (i * chunkSize);
      Arrays.fill(inputData, start, start + chunkSize - 1,
          String.valueOf(i % 9).getBytes(UTF_8)[0]);
    }
    return inputData;
  }

}
