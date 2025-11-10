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
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests Hybrid Pipeline Creation and IO on same set of Datanodes.
 */
public class TestHybridPipelineOnDatanode {
  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static ObjectStore objectStore;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 5);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
  }

  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Tests reading a corrputed chunk file throws checksum exception.
   * @throws IOException
   */
  @Test
  public void testHybridPipelineOnDatanode() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = UUID.randomUUID().toString();
    byte[] data = value.getBytes(UTF_8);
    objectStore.createVolume(volumeName);
    OzoneVolume volume = objectStore.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName1 = UUID.randomUUID().toString();

    // Write data into a key
    TestDataUtil.createKey(bucket, keyName1,
        ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
            ReplicationFactor.ONE), value.getBytes(UTF_8));

    String keyName2 = UUID.randomUUID().toString();

    // Write data into a key
    TestDataUtil.createKey(bucket, keyName2,
        ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
            ReplicationFactor.THREE), value.getBytes(UTF_8));

    // We need to find the location of the chunk file corresponding to the
    // data we just wrote.
    OzoneKey key1 = bucket.getKey(keyName1);
    long containerID1 =
        ((OzoneKeyDetails) key1).getOzoneKeyLocations().get(0).getContainerID();

    OzoneKey key2 = bucket.getKey(keyName2);
    long containerID2 =
        ((OzoneKeyDetails) key2).getOzoneKeyLocations().get(0).getContainerID();

    PipelineID pipelineID1 =
        cluster.getStorageContainerManager().getContainerInfo(containerID1)
            .getPipelineID();
    PipelineID pipelineID2 =
        cluster.getStorageContainerManager().getContainerInfo(containerID2)
            .getPipelineID();
    Pipeline pipeline1 =
        cluster.getStorageContainerManager().getPipelineManager()
            .getPipeline(pipelineID1);
    List<DatanodeDetails> dns = pipeline1.getNodes();
    assertEquals(1, dns.size());

    Pipeline pipeline2 =
        cluster.getStorageContainerManager().getPipelineManager()
            .getPipeline(pipelineID2);
    assertNotEquals(pipeline1, pipeline2);
    assertSame(pipeline1.getType(),
        HddsProtos.ReplicationType.RATIS);
    assertSame(pipeline1.getType(), pipeline2.getType());
    // assert that the pipeline Id1 and pipelineId2 are on the same node
    // but different replication factor
    assertThat(pipeline2.getNodes()).contains(dns.get(0));
    byte[] b1 = new byte[data.length];
    byte[] b2 = new byte[data.length];
    // now try to read both the keys
    try (InputStream is = bucket.readKey(keyName1)) {
      IOUtils.readFully(is, b1);
    }

    try (InputStream is = bucket.readKey(keyName2)) {
      IOUtils.readFully(is, b2);
    }
    assertArrayEquals(b1, data);
    assertArrayEquals(b1, b2);
  }
}

