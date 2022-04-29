/*
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

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Tests for {@link GrpcReplicationClient} and {@link GrpcReplicationService}.
 */
public class TestReplicationService {

  private static MiniOzoneCluster cluster;
  private static OzoneManager om;
  private static StorageContainerManager scm;
  private static OzoneClient client;
  private static ObjectStore store;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static final String SCM_ID = UUID.randomUUID().toString();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static final int NUM_KEYS = 2;
  private static final byte[] VALUE =
      UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
  private static long containerID;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    conf.setBoolean(OzoneConfigKeys.OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS,
        OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    startCluster(conf);
    prepareData(NUM_KEYS);
  }

  @AfterAll
  public static void stop() throws IOException {
    stopCluster();
  }

  @Test
  public void testReadContainer() throws Exception {
    DatanodeDetails dn = cluster.getHddsDatanodes().get(0).getDatanodeDetails();
    GrpcReplicationClient rc = cluster.getReplicationClient(dn);
    Optional<ContainerDataProto> maybeProto =
        rc.readContainer(containerID, dn.getUuidString()).get();
    Assertions.assertTrue(maybeProto.isPresent());
    ContainerDataProto proto = maybeProto.get();
    Assertions.assertEquals(containerID, proto.getContainerID());
    Assertions.assertEquals(NUM_KEYS * VALUE.length, proto.getBytesUsed());
    Assertions.assertEquals(NUM_KEYS, proto.getBlockCount());
    rc.close();
  }

  @Test
  public void testListBlock() throws Exception {
    DatanodeDetails dn = cluster.getHddsDatanodes().get(0).getDatanodeDetails();
    GrpcReplicationClient rc = cluster.getReplicationClient(dn);
    List<BlockData> blocks =
        rc.listBlock(containerID, dn.getUuidString(), NUM_KEYS + 1).get();
    Assertions.assertEquals(NUM_KEYS, blocks.size());
    Assertions.assertEquals(VALUE.length, blocks.get(0).getSize());
    rc.close();
  }

  @Test
  public void testReadChunk() throws Exception {
    DatanodeDetails dn = cluster.getHddsDatanodes().get(0).getDatanodeDetails();
    GrpcReplicationClient rc = cluster.getReplicationClient(dn);
    List<BlockData> blocks =
        rc.listBlock(containerID, dn.getUuidString(), NUM_KEYS).get();

    for (BlockData block : blocks) {
      Assertions.assertEquals(1, block.getChunksCount());
      Optional<ByteBuffer> maybeChunk = rc.readChunk(containerID,
          dn.getUuidString(), block.getBlockID(), block.getChunks(0)).get();
      Assertions.assertTrue(maybeChunk.isPresent());
      ByteBuffer chunk = maybeChunk.get();
      byte[] chunkData = new byte[chunk.remaining()];
      chunk.get(chunkData);
      Assertions.assertArrayEquals(VALUE, chunkData);
    }
    rc.close();
  }

  public static void startCluster(OzoneConfiguration conf) throws Exception {
    // Reduce long wait time in MiniOzoneClusterImpl#waitForHddsDatanodesStop
    //  for testZReadKeyWithUnhealthyContainerReplica.
    conf.set("ozone.scm.stale.node.interval", "10s");
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setTotalPipelineNumLimit(10)
        .setScmId(SCM_ID)
        .setClusterId(CLUSTER_ID)
        .build();
    cluster.waitForClusterToBeReady();
    om = cluster.getOzoneManager();
    scm = cluster.getStorageContainerManager();
    client = OzoneClientFactory.getRpcClient(conf);
    store = client.getObjectStore();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
  }

  public static void prepareData(int numKeys) throws Exception {
    final String volumeName = UUID.randomUUID().toString();
    final String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    final ReplicationConfig repConfig = ReplicationConfig
        .fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.THREE);
    for (int i = 0; i < numKeys; i++) {
      final String keyName = UUID.randomUUID().toString();
      try (OutputStream out = bucket.createKey(
          keyName, VALUE.length, repConfig, new HashMap<>())) {
        out.write(VALUE);
      }
    }
    Optional<Long> maybeContainerID = scm.getContainerManager()
        .getContainerIDs().stream().findAny().map(ContainerID::getId);
    Assertions.assertTrue(maybeContainerID.isPresent());
    containerID = maybeContainerID.get();
  }

  public static void stopCluster() throws IOException {
    if (client != null) {
      client.close();
    }

    if (storageContainerLocationClient != null) {
      storageContainerLocationClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }

}
