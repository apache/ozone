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

package org.apache.hadoop.ozone.client.rpc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_PER_METADATA_VOLUME;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the containerStateMachine failure handling.
 */
public class TestClientRetryContainerStateMachineFailures {
  private OzoneConfiguration  conf;
  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private ObjectStore objectStore;
  private String volumeName;
  private String bucketName;
  private XceiverClientManager xceiverClientManager;

  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();

    // ensure only 1 pipeline is created
    conf.setLong(OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setLong(OZONE_SCM_PIPELINE_PER_METADATA_VOLUME, 1);
    conf.set(OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION, "150s");
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 30, TimeUnit.SECONDS);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamBufferFlushDelay(false);
    conf.setFromObject(clientConfig);

    // update watch timeout to 10 second to finish test for client
    RatisClientConfig ratisClientConfig = conf.getObject(RatisClientConfig.class);
    ratisClientConfig.setWatchRequestTimeout(Duration.ofSeconds(10));
    conf.setFromObject(ratisClientConfig);
    RatisClientConfig.RaftConfig raftClientConfig = conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(10));
    conf.setFromObject(raftClientConfig);

    conf.setLong(OzoneConfigKeys.HDDS_RATIS_SNAPSHOT_THRESHOLD_KEY, 1);
    conf.setQuietMode(false);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.ONE, 60000);
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    xceiverClientManager = new XceiverClientManager(conf);
    volumeName = "testcontainerstatemachinefailures";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  @AfterEach
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (xceiverClientManager != null) {
      xceiverClientManager.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  void testContainerStateMachineLeaderFailure() throws Exception {
    // 1. ensure pipeline is ready
    ReplicationConfig replicationConfig = ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
        ReplicationFactor.THREE);
    try (OzoneOutputStream key = objectStore.getVolume(volumeName).getBucket(bucketName).createKey(
        "firstKey1", 1024, replicationConfig, new HashMap<>())) {
      key.write("ratis".getBytes(UTF_8));
      key.flush();
    } catch (IOException ex) {
      Assertions.fail("write key failed with exception: " + ex.getMessage());
    }

    // 2. mark leader pipeline dn's volume as full to induce failure
    List<Pair<StorageVolume, Long>> increasedVolumeSpace = new ArrayList<>();
    cluster.getHddsDatanodes().forEach(dn -> {
          AtomicBoolean isLeader = new AtomicBoolean(false);
          OzoneContainer container = dn.getDatanodeStateMachine().getContainer();
          checkDnPipelineIfLeader(container, isLeader);
          if (isLeader.get()) {
            List<StorageVolume> volumesList = container.getVolumeSet().getVolumesList();
            volumesList.forEach(sv -> {
              increasedVolumeSpace.add(Pair.of(sv, sv.getCurrentUsage().getAvailable()));
              sv.incrementUsedSpace(sv.getCurrentUsage().getAvailable());
            });
          }
        }
    );

    AtomicLong cnt = new AtomicLong();
    long startTime = Time.monotonicNow();
    try {
      // 3. create parallel key writes with leader failure and ensure they succeed with client retry
      for (int i = 0; i < 10; ++i) {
        int idx = i;
        cnt.getAndIncrement();
        CompletableFuture.runAsync(() -> {
          try (OzoneOutputStream key = objectStore.getVolume(volumeName).getBucket(bucketName).createKey(
              "testkey1" + idx, 1024, replicationConfig, new HashMap<>())) {
            key.write("ratis".getBytes(UTF_8));
            key.flush();
          } catch (IOException ex) {
            fail(ex.getMessage());
          }
          cnt.decrementAndGet();
        });
      }
      GenericTestUtils.waitFor(() -> cnt.get() == 0, 1000, 120000);
    } finally {
      increasedVolumeSpace.forEach(e -> e.getLeft().decrementUsedSpace(e.getRight()));
      System.out.println("Time taken: " + (Time.monotonicNow() - startTime));
    }
  }

  @Test
  void testContainerStateMachine5MBLeaderFailure() throws Exception {
    // 1. ensure pipeline is ready
    ReplicationConfig replicationConfig = ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
        ReplicationFactor.THREE);
    try (OzoneOutputStream key = objectStore.getVolume(volumeName).getBucket(bucketName).createKey(
        "firstKey1", 1024, replicationConfig, new HashMap<>())) {
      key.write("ratis".getBytes(UTF_8));
      key.flush();
    } catch (IOException ex) {
      Assertions.fail("write key failed with exception: " + ex.getMessage());
    }

    // 2. mark leader pipeline dn's volume as full to induce failure
    List<Pair<StorageVolume, Long>> increasedVolumeSpace = new ArrayList<>();
    cluster.getHddsDatanodes().forEach(dn -> {
          AtomicBoolean isLeader = new AtomicBoolean(false);
          OzoneContainer container = dn.getDatanodeStateMachine().getContainer();
          checkDnPipelineIfLeader(container, isLeader);
          if (isLeader.get()) {
            List<StorageVolume> volumesList = container.getVolumeSet().getVolumesList();
            volumesList.forEach(sv -> {
              increasedVolumeSpace.add(Pair.of(sv, sv.getCurrentUsage().getAvailable()));
              sv.incrementUsedSpace(sv.getCurrentUsage().getAvailable());
            });
          }
        }
    );

    int size = 5 * 1024 * 1024;
    long startTime = Time.monotonicNow();
    try {
      // 3. key writes with leader failure and ensure they succeed with client retry
      try (OzoneOutputStream key = objectStore.getVolume(volumeName).getBucket(bucketName).createKey(
          "testkey123", size, replicationConfig, new HashMap<>())) {
        key.write(generateData(size));
        key.flush();
      } catch (IOException ex) {
        fail(ex.getMessage());
      }
    } finally {
      increasedVolumeSpace.forEach(e -> e.getLeft().decrementUsedSpace(e.getRight()));
      System.out.println("Time taken: " + (Time.monotonicNow() - startTime));
    }
    validateBlockData("testkey123", 2, true);
  }

  @Test
  void testContainerStateMachineWriteLeaderNextChunkFailure() throws Exception {
    ReplicationConfig replicationConfig = ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
        ReplicationFactor.THREE);
    int chunkSize = (int) conf.getStorageSize(OZONE_SCM_CHUNK_SIZE_KEY, OZONE_SCM_CHUNK_SIZE_DEFAULT,
        StorageUnit.BYTES);
    int size = chunkSize +  1024;
    // 1. mark leader pipeline dn's volume as full to induce failure
    List<Pair<StorageVolume, Long>> increasedVolumeSpace = new ArrayList<>();
    cluster.getHddsDatanodes().forEach(dn -> {
      AtomicBoolean isLeader = new AtomicBoolean(false);
      OzoneContainer container = dn.getDatanodeStateMachine().getContainer();
      checkDnPipelineIfLeader(container, isLeader);
      if (isLeader.get()) {
            List<StorageVolume> volumesList = container.getVolumeSet().getVolumesList();
            volumesList.forEach(sv -> {
              increasedVolumeSpace.add(Pair.of(sv, sv.getCurrentUsage().getAvailable()));
            });
          }
        }
    );

    long startTime = Time.monotonicNow();
    try {
    // 2. create parallel key writes with leader failure and ensure they succeed with client retry
      try (OzoneOutputStream key = objectStore.getVolume(volumeName).getBucket(bucketName).createKey(
          "testkey1", size, replicationConfig, new HashMap<>())) {
        key.write(generateData(chunkSize));
        key.flush();
        // Fail writing second chunk
        increasedVolumeSpace.forEach(e -> e.getLeft().incrementUsedSpace(e.getRight()));
        key.write(generateData(1024));
        key.flush();
      } catch (IOException ex) {
        fail(ex.getMessage());
      }
    } finally {
      increasedVolumeSpace.forEach(e -> e.getLeft().decrementUsedSpace(e.getRight()));
      System.out.println("Time taken: " + (Time.monotonicNow() - startTime));
    }
    validateBlockData("testkey1", 2, false);
  }

  @Test
  void testContainerStateMachineWriteFollowerFailure() throws Exception {
    // 1. ensure pipeline is ready
    ReplicationConfig replicationConfig = ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
        ReplicationFactor.THREE);
    try (OzoneOutputStream key = objectStore.getVolume(volumeName).getBucket(bucketName).createKey(
        "firstKey1", 1024, replicationConfig, new HashMap<>())) {
      key.write("ratis".getBytes(UTF_8));
      key.flush();
    } catch (IOException ex) {
      Assertions.fail("write key failed with exception: " + ex.getMessage());
    }

    // 2. mark leader pipeline dn's volume as full to induce failure
    List<Pair<StorageVolume, Long>> increasedVolumeSpace = new ArrayList<>();
    for (HddsDatanodeService dn: cluster.getHddsDatanodes()) {
      AtomicBoolean isLeader = new AtomicBoolean(false);
      OzoneContainer container = dn.getDatanodeStateMachine().getContainer();
      checkDnPipelineIfLeader(container, isLeader);
      if (!isLeader.get()) {
        List<StorageVolume> volumesList = container.getVolumeSet().getVolumesList();
        volumesList.forEach(sv -> {
          increasedVolumeSpace.add(Pair.of(sv, sv.getCurrentUsage().getAvailable()));
          sv.incrementUsedSpace(sv.getCurrentUsage().getAvailable());
        });
        break;
      }
    }

    long startTime = Time.monotonicNow();
    try {
      // 3. create parallel key writes with leader failure and ensure they succeed with client retry
      try (OzoneOutputStream key = objectStore.getVolume(volumeName).getBucket(bucketName).createKey(
          "testkey1", 1024, replicationConfig, new HashMap<>())) {
        key.write(generateData(1024));
        key.flush();
      } catch (IOException ex) {
        fail(ex.getMessage());
      }
    } finally {
      increasedVolumeSpace.forEach(e -> e.getLeft().decrementUsedSpace(e.getRight()));
      System.out.println("Time taken: " + (Time.monotonicNow() - startTime));
    }
    validateBlockData("testkey1", 2, true);
  }

  @Test
  void testContainerStateMachineWriteFollowerNextChunkFailure() throws Exception {
    // 1. ensure pipeline is ready
    ReplicationConfig replicationConfig = ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
        ReplicationFactor.THREE);
    int chunkSize = (int) conf.getStorageSize(OZONE_SCM_CHUNK_SIZE_KEY, OZONE_SCM_CHUNK_SIZE_DEFAULT,
        StorageUnit.BYTES);
    int size = chunkSize +  1024;
    // 2. mark leader pipeline dn's volume as full to induce failure
    List<Pair<StorageVolume, Long>> increasedVolumeSpace = new ArrayList<>();
    for (HddsDatanodeService dn: cluster.getHddsDatanodes()) {
      AtomicBoolean isLeader = new AtomicBoolean(false);
      OzoneContainer container = dn.getDatanodeStateMachine().getContainer();
      checkDnPipelineIfLeader(container, isLeader);
      if (isLeader.get()) {
        List<StorageVolume> volumesList = container.getVolumeSet().getVolumesList();
        volumesList.forEach(sv -> {
          increasedVolumeSpace.add(Pair.of(sv, sv.getCurrentUsage().getAvailable()));
        });
      }
    }

    long startTime = Time.monotonicNow();
    try {
      // 3. create parallel key writes with leader failure and ensure they succeed with client retry
      try (OzoneOutputStream key = objectStore.getVolume(volumeName).getBucket(bucketName).createKey(
          "testkey1", size, replicationConfig, new HashMap<>())) {
        key.write(generateData(chunkSize));
        key.flush();
        // Fail writing second chunk
        increasedVolumeSpace.forEach(e -> e.getLeft().incrementUsedSpace(e.getRight()));
        key.write(generateData(1024));
        key.flush();
      } catch (IOException ex) {
        fail(ex.getMessage());
      }
    } finally {
      increasedVolumeSpace.forEach(e -> e.getLeft().decrementUsedSpace(e.getRight()));
      System.out.println("Time taken: " + (Time.monotonicNow() - startTime));
    }
    validateBlockData("testkey1", 2, false);
  }

  private byte[] generateData(int size) {
    byte[] data = new byte[size];
    Arrays.fill(data, (byte) ('a'));
    data[size - 1] = 0;
    return data;
  }

  private static void checkDnPipelineIfLeader(OzoneContainer container, AtomicBoolean isLeader) {
    RaftServer server = ((XceiverServerRatis) container.getWriteChannel()).getServer();
    try {
      server.getGroups().forEach(gid -> {
        if (gid.getPeers().size() < 3) {
          return;
        }
        try {
          if (server.getDivision(gid.getGroupId()).getInfo().isLeader()) {
            isLeader.set(true);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void validateBlockData(String keyName, int matchCount, boolean missingContainer) throws IOException {
    OzoneKeyDetails key = objectStore.getVolume(volumeName).getBucket(bucketName).getKey(keyName);
    Map<Long, List<Long>> containerBlockListMap = new HashMap<>();
    cluster.getHddsDatanodes().forEach(dn -> {
      OzoneContainer container = dn.getDatanodeStateMachine().getContainer();
      container.getContainerSet().getContainerMap().forEach((key1, value) -> {
        List<Long> blockList = containerBlockListMap.getOrDefault(key1, new ArrayList<>());
        blockList.add(value.getBlockCommitSequenceId());
        containerBlockListMap.put(key1, blockList);
      });
    });
    key.getOzoneKeyLocations().forEach(location -> {
      List<Long> blockList = containerBlockListMap.getOrDefault(location.getContainerID(), Collections.emptyList());
      if (missingContainer) {
        Assertions.assertEquals(blockList.size(), matchCount, "Block list: " + blockList.size());
        System.out.println("Block list: " + blockList.size());
      } else {
        long max = Collections.max(blockList);
        long count = blockList.stream().filter(num -> num == max).count();
        Assertions.assertTrue(count >= matchCount, "Count: " + count);
        System.out.println("Block max bcsid: " + max + ", count: " + count + ", block list: " + blockList);
      }
    });
  }
}
