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

package org.apache.hadoop.hdds.scm.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.ratis.ContainerCommandRequestMessage;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.ratis.client.api.DataStreamApi;
import org.apache.ratis.client.api.DataStreamInput;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamReplyHeader;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ReferenceCountedObject;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

/**
 * Tests for {@link RatisDataStreamBlockInputStream}.
 */
class TestRatisDataStreamBlockInputStream {
  private static final long ONE_GB = 1L << 30;
  private static final long READ_WINDOW = 256L << 20;
  private static final long PRE_READ = 32L << 20;
  private static final byte[] DATA = {1, 2, 3, 4};

  private final BlockID blockID = new BlockID(1L, 1L);
  private final DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
  private final DatanodeDetails dn2 = MockDatanodeDetails.randomDatanodeDetails();
  private final DatanodeDetails dn3 = MockDatanodeDetails.randomDatanodeDetails();
  private final PipelineID pipelineID = PipelineID.randomId();

  @Test
  void readLengthUsesLargeWindowOnlyAfterSequentialStream() {
    final int smallRead = 4 << 10;

    assertEquals(smallRead,
        RatisDataStreamBlockInputStream.computeReadLength(
            ONE_GB, 0, smallRead, true, false, PRE_READ, READ_WINDOW));
    assertEquals(READ_WINDOW,
        RatisDataStreamBlockInputStream.computeReadLength(
            ONE_GB, PRE_READ + smallRead, smallRead, true, true, PRE_READ,
            READ_WINDOW));
    assertEquals(smallRead,
        RatisDataStreamBlockInputStream.computeReadLength(
            ONE_GB, 0, smallRead, false, true, PRE_READ, READ_WINDOW));
    assertEquals(1024,
        RatisDataStreamBlockInputStream.computeReadLength(
            ONE_GB, ONE_GB - 1024, smallRead, true, true, PRE_READ,
            READ_WINDOW));
  }

  /**
   * The request must name the datanode the client streams to (the closest node of the client's pipeline), even
   * when the block's pipeline orders the nodes differently; otherwise a closed-container read is not resolved.
   */
  @Test
  void requestNamesTheDatanodeTheClientStreamsTo() throws Exception {
    final Pipeline blockPipeline = pipeline(dn2, dn1, dn3);
    final Pipeline clientPipeline = pipeline(dn1, dn2, dn3);
    final List<ByteBuffer> headers = new ArrayList<>();
    final XceiverClientRatis client = client(clientPipeline, headers, dataReply());
    final XceiverClientFactory factory = factory(invocation -> client);

    try (RatisDataStreamBlockInputStream in = newStream(blockPipeline, factory)) {
      assertArrayEquals(DATA, readAll(in));
    }

    assertEquals(1, headers.size());
    assertEquals(dn1.getUuidString(), toRequest(headers.get(0)).getDatanodeUuid());
  }

  /** A follower refuses reads of an open container; the read must be retried on the suggested leader. */
  @Test
  void redirectsToSuggestedLeader() throws Exception {
    final Pipeline blockPipeline = pipeline(dn1, dn2, dn3);
    final List<ByteBuffer> followerHeaders = new ArrayList<>();
    final List<ByteBuffer> leaderHeaders = new ArrayList<>();
    final List<XceiverClientRatis> followers = new ArrayList<>();
    final XceiverClientFactory factory = factory(invocation -> {
      final Pipeline p = invocation.getArgument(0);
      if (p.getClosestNode().equals(dn3)) {
        return client(p, leaderHeaders, dataReply());
      }
      final XceiverClientRatis follower = client(p, followerHeaders, notLeaderReply(p.getClosestNode(), dn3));
      followers.add(follower);
      return follower;
    });

    try (RatisDataStreamBlockInputStream in = newStream(blockPipeline, factory)) {
      assertArrayEquals(DATA, readAll(in));
    }

    assertEquals(1, followerHeaders.size());
    assertEquals(dn1.getUuidString(), toRequest(followerHeaders.get(0)).getDatanodeUuid());
    assertEquals(1, leaderHeaders.size());
    assertEquals(dn3.getUuidString(), toRequest(leaderHeaders.get(0)).getDatanodeUuid());
    assertEquals(1, followers.size());
    verify(factory).releaseClient(followers.get(0), false, true);
  }

  /** A factory that answers every way of acquiring a read client with {@code answer}. */
  private static XceiverClientFactory factory(Answer<XceiverClientRatis> answer) throws Exception {
    final XceiverClientFactory factory = mock(XceiverClientFactory.class);
    when(factory.acquireClient(any(Pipeline.class), eq(true))).thenAnswer(answer);
    when(factory.acquireClientForReadData(any(Pipeline.class))).thenAnswer(answer);
    return factory;
  }

  private Pipeline pipeline(DatanodeDetails... nodesInOrder) {
    return Pipeline.newBuilder()
        .setId(pipelineID)
        .setState(Pipeline.PipelineState.CLOSED)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setNodes(Arrays.asList(dn1, dn2, dn3))
        .setNodesInOrder(Arrays.asList(nodesInOrder))
        .build();
  }

  private RatisDataStreamBlockInputStream newStream(Pipeline pipeline, XceiverClientFactory factory) {
    final OzoneClientConfig config = new OzoneClientConfig();
    config.setChecksumVerify(false);
    return new RatisDataStreamBlockInputStream(blockID, DATA.length, pipeline, null, factory, config);
  }

  private static byte[] readAll(RatisDataStreamBlockInputStream in) throws Exception {
    final ByteBuffer buffer = ByteBuffer.allocate(DATA.length);
    assertEquals(DATA.length, in.read(buffer));
    return buffer.array();
  }

  /** A client of {@code pipeline} whose read-only stream returns {@code replies} and records its request header. */
  private static XceiverClientRatis client(Pipeline pipeline, List<ByteBuffer> headers, DataStreamReply... replies) {
    final Deque<DataStreamReply> queue = new ArrayDeque<>(Arrays.asList(replies));
    final DataStreamInput input = mock(DataStreamInput.class);
    when(input.readAsync()).thenAnswer(invocation -> CompletableFuture.completedFuture(retained(queue.remove())));
    final DataStreamApi api = mock(DataStreamApi.class);
    when(api.streamReadOnly(any(ByteBuffer.class))).thenAnswer(invocation -> {
      headers.add(invocation.getArgument(0));
      return input;
    });
    final XceiverClientRatis client = mock(XceiverClientRatis.class);
    when(client.getPipeline()).thenReturn(pipeline);
    when(client.getDataStreamApi()).thenReturn(api);
    return client;
  }

  private static ReferenceCountedObject<DataStreamReply> retained(DataStreamReply reply) {
    final ReferenceCountedObject<DataStreamReply> ref =
        ReferenceCountedObject.<DataStreamReply>newBuilder().setValue(reply).build();
    ref.retain();
    return ref;
  }

  private DataStreamReply dataReply() {
    final ContainerCommandResponseProto response = ContainerCommandResponseProto.newBuilder()
        .setCmdType(Type.ReadBlock)
        .setResult(Result.SUCCESS)
        .setReadBlock(ReadBlockResponseProto.newBuilder()
            .setOffset(0)
            .setData(ByteString.EMPTY)
            .setChecksumData(ChecksumData.newBuilder().setType(ChecksumType.NONE).setBytesPerChecksum(0)))
        .build();
    final byte[] metadata = response.toByteArray();
    final ByteBuffer frame = ByteBuffer.allocate(Integer.BYTES + metadata.length + DATA.length);
    frame.putInt(metadata.length).put(metadata).put(DATA).flip();
    return reply(DataStreamPacketHeaderProto.Type.STREAM_DATA, frame, true);
  }

  private DataStreamReply notLeaderReply(DatanodeDetails follower, DatanodeDetails leader) {
    final RaftGroupMemberId member = RaftGroupMemberId.valueOf(
        RatisHelper.toRaftPeerId(follower), RaftGroupId.valueOf(pipelineID.getId()));
    final RaftClientReply reply = RaftClientReply.newBuilder()
        .setClientId(ClientId.randomId())
        .setServerId(member)
        .setCallId(1)
        .setException(new NotLeaderException(member, RatisHelper.toRaftPeer(leader), Collections.emptyList()))
        .build();
    return reply(DataStreamPacketHeaderProto.Type.STREAM_HEADER,
        ClientProtoUtils.toRaftClientReplyProto(reply).toByteString().asReadOnlyByteBuffer(), false);
  }

  private static DataStreamReply reply(DataStreamPacketHeaderProto.Type type, ByteBuffer buffer, boolean success) {
    return DataStreamReplyByteBuffer.newBuilder()
        .setDataStreamReplyHeader(new DataStreamReplyHeader(ClientId.randomId(), type, 1, 0, buffer.remaining(), 0,
            success, Collections.emptyList()))
        .setBuffer(buffer)
        .build();
  }

  private static ContainerCommandRequestProto toRequest(ByteBuffer header) throws Exception {
    return ContainerCommandRequestMessage.toProto(ByteString.copyFrom(header.duplicate()), null);
  }
}
