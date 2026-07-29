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

package org.apache.hadoop.ozone.client.rpc.read;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.RatisDataStreamBlockInputStream;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.BlockDataStreamOutputEntry;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.om.BucketForTesting;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Test;

/**
 * Tests reading a RATIS block through the Ratis data stream transport.
 */
public class TestRatisDataStreamReadBlock {
  private static final SizeInBytes KEY_SIZE = SizeInBytes.valueOf("16M");
  private static final SizeInBytes BUFFER_SIZE = SizeInBytes.valueOf("1M");

  @Test
  void readLargeBlockWithRatisDataStream() throws Exception {
    try (MiniOzoneCluster cluster = TestStreamRead.newCluster(16 << 10)) {
      cluster.waitForClusterToBeReady();

      final OzoneConfiguration conf = cluster.getConf();
      final OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
      clientConfig.setStreamReadBlock(false);
      clientConfig.setRatisStreamReadBlock(true);
      conf.setFromObject(clientConfig);

      try (OzoneClient client = OzoneClientFactory.getRpcClient(conf)) {
        final BucketForTesting testBucket =
            BucketForTesting.newBuilder(client).build();
        final String keyName = "ratis-datastream-read-block";
        final String expectedMd5 = createKey(testBucket.delegate(), keyName);

        try (KeyInputStream in = testBucket.getKeyInputStream(keyName)) {
          final List<RatisDataStreamBlockInputStream> streams =
              assertRatisStreams(in);
          assertEquals(0, streams.get(0).read(ByteBuffer.allocate(0)));
          assertEquals(expectedMd5, readMd5(in));
        }
      }
    }
  }

  @Test
  void readClosedContainerAfterRatisGroupRemovalWithRatisDataStream()
      throws Exception {
    try (MiniOzoneCluster cluster = TestStreamRead.newCluster(16 << 10)) {
      cluster.waitForClusterToBeReady();

      final OzoneConfiguration conf = cluster.getConf();
      final OzoneClientConfig clientConfig =
          conf.getObject(OzoneClientConfig.class);
      clientConfig.setStreamReadBlock(false);
      clientConfig.setRatisStreamReadBlock(true);
      conf.setFromObject(clientConfig);

      try (OzoneClient client = OzoneClientFactory.getRpcClient(conf)) {
        final BucketForTesting testBucket =
            BucketForTesting.newBuilder(client).build();
        final String keyName = "ratis-datastream-read-closed-container";
        final String expectedMd5 = createKeyAndCloseContainersAndRemoveGroups(
            cluster, testBucket.delegate(), keyName);

        try (KeyInputStream in = testBucket.getKeyInputStream(keyName)) {
          assertRatisStreams(in);
          assertEquals(expectedMd5, readMd5(in));
        }
      }
    }
  }

  private static List<RatisDataStreamBlockInputStream> assertRatisStreams(
      KeyInputStream in) {
    assertTrue(in.isStreamBlockInputStream());
    final List<RatisDataStreamBlockInputStream> streams = new ArrayList<>();
    for (BlockExtendedInputStream stream : in.getPartStreams()) {
      streams.add(assertInstanceOf(RatisDataStreamBlockInputStream.class,
          stream, "Unexpected stream classes: " + in.getPartStreams()));
    }
    assertFalse(streams.isEmpty());
    return streams;
  }

  private static String createKey(OzoneBucket bucket, String keyName) throws Exception {
    try (OutputStream out = bucket.createStreamKey(keyName, KEY_SIZE.getSize(),
        RatisReplicationConfig.getInstance(ONE), Collections.emptyMap())) {
      return writeKey(out);
    }
  }

  private static String createKeyAndCloseContainersAndRemoveGroups(
      MiniOzoneCluster cluster, OzoneBucket bucket, String keyName)
      throws Exception {
    final String expectedMd5;
    final List<Long> containerIds;
    try (OzoneDataStreamOutput out = bucket.createStreamKey(keyName,
        KEY_SIZE.getSize(), RatisReplicationConfig.getInstance(ONE),
        Collections.emptyMap())) {
      expectedMd5 = writeKey(out);
      containerIds = getContainerIds(out);
    }
    final List<Pipeline> pipelines = getPipelines(cluster, containerIds);
    TestHelper.waitForContainerClose(cluster, containerIds.toArray(new Long[0]));
    removeRatisGroups(cluster, pipelines);
    return expectedMd5;
  }

  private static List<Pipeline> getPipelines(MiniOzoneCluster cluster,
      List<Long> containerIds) throws Exception {
    final List<Pipeline> pipelines = new ArrayList<>();
    for (long containerId : containerIds) {
      final Pipeline pipeline = cluster.getStorageContainerManager()
          .getPipelineManager().getPipeline(cluster.getStorageContainerManager()
              .getContainerManager().getContainer(
                  ContainerID.valueOf(containerId)).getPipelineID());
      if (!pipelines.contains(pipeline)) {
        pipelines.add(pipeline);
      }
    }
    return pipelines;
  }

  private static void removeRatisGroups(MiniOzoneCluster cluster,
      List<Pipeline> pipelines) throws Exception {
    for (Pipeline pipeline : pipelines) {
      for (DatanodeDetails dn : pipeline.getNodes()) {
        final XceiverServerSpi server = cluster.getHddsDatanodes()
            .get(cluster.getHddsDatanodeIndex(dn)).getDatanodeStateMachine()
            .getContainer().getWriteChannel();
        if (server.isExist(pipeline.getId().getProtobuf())) {
          server.removeGroup(pipeline.getId().getProtobuf());
        }
        assertFalse(server.isExist(pipeline.getId().getProtobuf()));
      }
    }
  }

  private static List<Long> getContainerIds(OzoneDataStreamOutput out) {
    final List<Long> containerIds = new ArrayList<>();
    for (BlockDataStreamOutputEntry entry :
        out.getKeyDataStreamOutput().getStreamEntries()) {
      final long containerId = entry.getBlockID().getContainerID();
      if (!containerIds.contains(containerId)) {
        containerIds.add(containerId);
      }
    }
    assertFalse(containerIds.isEmpty());
    return containerIds;
  }

  private static String writeKey(OutputStream out) throws Exception {
    final MessageDigest md5 = MessageDigest.getInstance("MD5");
    final byte[] buffer = new byte[BUFFER_SIZE.getSizeInt()];
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = (byte) (i & 0xff);
    }

    for (long pos = 0; pos < KEY_SIZE.getSize();) {
      final int writeSize =
          Math.toIntExact(Math.min(buffer.length, KEY_SIZE.getSize() - pos));
      out.write(buffer, 0, writeSize);
      md5.update(buffer, 0, writeSize);
      pos += writeSize;
    }
    return StringUtils.bytes2Hex(md5.digest());
  }

  private static String readMd5(InputStream in) throws Exception {
    final MessageDigest md5 = MessageDigest.getInstance("MD5");
    final byte[] buffer = new byte[BUFFER_SIZE.getSizeInt()];
    for (;;) {
      final int read = in.read(buffer);
      if (read < 0) {
        break;
      }
      md5.update(buffer, 0, read);
    }
    return StringUtils.bytes2Hex(md5.digest());
  }
}
