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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for TestXceiverClientGrpc, to ensure topology aware reads work
 * select the closest node, and connections are re-used after a getBlock call.
 */
public class TestXceiverClientGrpc {

  private Pipeline pipeline;
  private List<DatanodeDetails> dns;
  private List<DatanodeDetails> dnsInOrder;
  private OzoneConfiguration conf = new OzoneConfiguration();

  @BeforeEach
  public void setup() {
    dns = new ArrayList<>();
    dns.add(MockDatanodeDetails.randomDatanodeDetails());
    dns.add(MockDatanodeDetails.randomDatanodeDetails());
    dns.add(MockDatanodeDetails.randomDatanodeDetails());

    dnsInOrder = new ArrayList<>();
    for (int i = 2; i >= 0; i--) {
      dnsInOrder.add(dns.get(i));
    }

    pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setState(Pipeline.PipelineState.CLOSED)
        .setNodes(dns)
        .setNodesInOrder(dnsInOrder)
        .build();
  }

  @Test
  public void testCorrectDnsReturnedFromPipeline() throws IOException {
    assertEquals(dnsInOrder.get(0), pipeline.getClosestNode());
    assertEquals(dns.get(0), pipeline.getFirstNode());
    assertNotEquals(dns.get(0), dnsInOrder.get(0));
  }

  @Test
  public void testLeaderNodeIsCommandTarget() throws IOException {
    final Set<DatanodeDetails> seenDN = new HashSet<>();
    conf.setBoolean(
            OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY, false);
    // Using a new Xceiver Client, make 100 calls and ensure leader node is used
    // each time. The logic should always use the leader node, so we can check
    // only a single DN is ever seen after 100 calls.
    for (int i = 0; i < 100; i++) {
      try (XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf) {
        @Override
        public XceiverClientReply sendCommandAsync(
            ContainerProtos.ContainerCommandRequestProto request,
            DatanodeDetails dn) {
          seenDN.add(dn);
          return buildValidResponse();
        }
      }) {
        invokeXceiverClientGetBlock(client);
      }
    }
    assertEquals(1, seenDN.size());
  }

  @Test
  public void testGetBlockRetryAlNodes() {
    final ArrayList<DatanodeDetails> allDNs = new ArrayList<>(dns);
    assertThat(allDNs.size()).isGreaterThan(1);
    try (XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf) {
      @Override
      public XceiverClientReply sendCommandAsync(
          ContainerProtos.ContainerCommandRequestProto request,
          DatanodeDetails dn) throws IOException {
        allDNs.remove(dn);
        throw new IOException("Failed " + dn);
      }
    }) {
      invokeXceiverClientGetBlock(client);
    } catch (IOException e) {
      e.printStackTrace();
    }
    assertEquals(0, allDNs.size());
  }

  @Test
  public void testReadChunkRetryAllNodes() {
    final ArrayList<DatanodeDetails> allDNs = new ArrayList<>(dns);
    assertThat(allDNs.size()).isGreaterThan(1);
    try (XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf) {
      @Override
      public XceiverClientReply sendCommandAsync(
          ContainerProtos.ContainerCommandRequestProto request,
          DatanodeDetails dn) throws IOException {
        allDNs.remove(dn);
        throw new IOException("Failed " + dn);
      }
    }) {
      invokeXceiverClientReadChunk(client);
    } catch (IOException e) {
      e.printStackTrace();
    }
    assertEquals(0, allDNs.size());
  }

  @Test
  public void testFirstNodeIsCorrectWithTopologyForCommandTarget()
      throws IOException {
    final Set<DatanodeDetails> seenDNs = new HashSet<>();
    conf.setBoolean(
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY, true);
    // With a new Client, make 100 calls and ensure the first sortedDN is used
    // each time. The logic should always use the sorted node, so we can check
    // only a single DN is ever seen after 100 calls.
    for (int i = 0; i < 100; i++) {
      try (XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf) {
        @Override
        public XceiverClientReply sendCommandAsync(
            ContainerProtos.ContainerCommandRequestProto request,
            DatanodeDetails dn) {
          seenDNs.add(dn);
          return buildValidResponse();
        }
      }) {
        invokeXceiverClientGetBlock(client);
      }
    }
    assertEquals(1, seenDNs.size());
  }

  @Test
  public void testPrimaryReadFromNormalDatanode()
      throws IOException {
    final List<DatanodeDetails> seenDNs = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Pipeline randomPipeline = MockPipeline.createRatisPipeline();
      int nodeCount = randomPipeline.getNodes().size();
      assertThat(nodeCount).isGreaterThan(1);
      randomPipeline.getNodes().forEach(
          node -> assertEquals(NodeOperationalState.IN_SERVICE, node.getPersistedOpState()));

      randomPipeline.getNodes().get(
          RandomUtils.secure().randomInt(0, nodeCount)).
          setPersistedOpState(NodeOperationalState.IN_MAINTENANCE);
      randomPipeline.getNodes().get(
          RandomUtils.secure().randomInt(0, nodeCount)).
          setPersistedOpState(NodeOperationalState.IN_MAINTENANCE);
      try (XceiverClientGrpc client = new XceiverClientGrpc(randomPipeline, conf) {
        @Override
        public XceiverClientReply sendCommandAsync(
            ContainerProtos.ContainerCommandRequestProto request,
            DatanodeDetails dn) {
          seenDNs.add(dn);
          return buildValidResponse();
        }
      }) {
        invokeXceiverClientGetBlock(client);
      } catch (IOException e) {
        e.printStackTrace();
      }
      // Always the IN_SERVICE datanode will be read first
      assertEquals(NodeOperationalState.IN_SERVICE, seenDNs.get(0).getPersistedOpState());
    }
  }

  @Test
  public void testConnectionReusedAfterGetBlock() throws IOException {
    // With a new Client, make 100 calls. On each call, ensure that only one
    // DN is seen, indicating the same DN connection is reused.
    for (int i = 0; i < 100; i++) {
      final Set<DatanodeDetails> seenDNs = new HashSet<>();
      try (XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf) {
        @Override
        public XceiverClientReply sendCommandAsync(
            ContainerProtos.ContainerCommandRequestProto request,
            DatanodeDetails dn) {
          seenDNs.add(dn);
          return buildValidResponse();
        }
      }) {
        invokeXceiverClientGetBlock(client);
        invokeXceiverClientGetBlock(client);
        invokeXceiverClientReadChunk(client);
        invokeXceiverClientReadSmallFile(client);
      }
      assertEquals(1, seenDNs.size());
    }
  }

  private void invokeXceiverClientGetBlock(XceiverClientSpi client)
      throws IOException {
    ContainerProtocolCalls.getBlock(client,
        BlockID.getFromProtobuf(ContainerProtos.DatanodeBlockID.newBuilder()
            .setContainerID(1)
            .setLocalID(1)
            .setBlockCommitSequenceId(1)
            .build()), null, client.getPipeline().getReplicaIndexes());
  }

  private void invokeXceiverClientReadChunk(XceiverClientSpi client)
      throws IOException {
    BlockID bid = new BlockID(1, 1);
    bid.setBlockCommitSequenceId(1);
    ContainerProtocolCalls.readChunk(client,
        ContainerProtos.ChunkInfo.newBuilder()
            .setChunkName("Anything")
            .setChecksumData(ContainerProtos.ChecksumData.newBuilder()
                .setBytesPerChecksum(512)
                .setType(ContainerProtos.ChecksumType.CRC32)
                .build())
            .setLen(-1)
            .setOffset(0)
            .build(),
        bid.getDatanodeBlockIDProtobuf(),
        null, null);
  }

  private void invokeXceiverClientReadSmallFile(XceiverClientSpi client)
      throws IOException {
    BlockID bid = new BlockID(1, 1);
    bid.setBlockCommitSequenceId(1);
    ContainerProtocolCalls.readSmallFile(client, bid, null);
  }

  private XceiverClientReply buildValidResponse() {
    ContainerProtos.ContainerCommandResponseProto resp =
        ContainerProtos.ContainerCommandResponseProto.newBuilder()
            .setCmdType(ContainerProtos.Type.GetBlock)
            .setResult(ContainerProtos.Result.SUCCESS).build();
    final CompletableFuture<ContainerProtos.ContainerCommandResponseProto>
        replyFuture = new CompletableFuture<>();
    replyFuture.complete(resp);
    return new XceiverClientReply(replyFuture);
  }

}
