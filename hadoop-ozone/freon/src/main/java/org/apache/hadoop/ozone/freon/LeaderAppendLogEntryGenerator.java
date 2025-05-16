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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.CreateContainerRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceStub;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.NegotiationType;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Test isolated LEADER datanodes.
 * <p>
 * This test configures a Raft group, in a single standalone datanode and use
 * it as a leader. To move out followers from the picture
 * dev-support/byteman/ratis-mock-followers.btm byteman script can be used.
 */
@Command(name = "lalg",
    aliases = "leader-append-log-generator",
    description = "Generate append log entries to a leader server",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
@SuppressWarnings("java:S2245") // no need for secure random
public class LeaderAppendLogEntryGenerator extends BaseAppendLogGenerator
    implements
    Callable<Void> {

  public static final String FAKE_LEADER_ADDDRESS1 = "localhost:1234";
  public static final String FAKE_LEADER_ADDDRESS2 = "localhost:1235";

  private static final Logger LOG =
      LoggerFactory.getLogger(LeaderAppendLogEntryGenerator.class);

  private static final String FAKE_FOLLOWER_ID1 =
      "ffffffff-df33-4a20-8e1f-ffffffff6be5";

  @Option(names = {"-l", "--pipeline"},
      description = "Pipeline to use. By default the first RATIS/THREE "
          + "pipeline will be used.",
      defaultValue = "96714307-4bd7-42b5-a65d-e1b13b4ca5c0")
  private String pipelineId = "96714307-4bd7-42b5-a65d-e1b13b4ca5c0";

  @Option(names = {"-s", "--size"},
      description = "Size of the generated chunks (in bytes)",
      defaultValue = "1024")
  private int chunkSize;

  @Option(names = {"-i", "--next-index"},
      description = "The next index in the term 2 to continue a test. (If "
          + "zero, a new ratis ring will be initialized with configureGroup "
          + "call and vote)",
      defaultValue = "0")
  private long nextIndex;

  private RaftPeerProto requestor;

  private long term = 2L;

  private RaftServerProtocolServiceStub stub;

  private Random callIdRandom = new Random();

  private ByteString dataToWrite;

  private Timer timer;

  @Override
  public Void call() throws Exception {

    inFlightMessages = new LinkedBlockingQueue<>(inflightLimit);

    OzoneConfiguration conf = createOzoneConfiguration();

    byte[] data = RandomStringUtils.secure().nextAscii(chunkSize)
        .getBytes(StandardCharsets.UTF_8);
    dataToWrite = ByteString.copyFrom(data);

    setServerIdFromFile(conf);

    requestor = RaftPeerProto.newBuilder()
        .setId(RaftPeerId.valueOf(FAKE_FOLLOWER_ID1).toByteString())
        .setAddress(FAKE_LEADER_ADDDRESS1)
        .build();

    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forTarget(serverAddress).proxyDetector(uri -> null);
    channelBuilder.negotiationType(NegotiationType.PLAINTEXT);
    ManagedChannel build = channelBuilder.build();
    stub = RaftServerProtocolServiceGrpc.newStub(build);

    init();

    if (nextIndex == 0) {
      configureGroup();
    }

    Thread.sleep(3000L);

    XceiverClientRatis client = createXceiverClient(conf);

    client.connect();

    long containerId = 1L;

    System.out.println(client.sendCommand(createContainerRequest(containerId)));

    timer = getMetrics().timer("append-entry");

    runTests(step -> timer.time(() -> {
      inFlightMessages.put(step);
      XceiverClientReply xceiverClientReply =
          client.sendCommandAsync(createChunkWriteRequest(containerId, step));
      xceiverClientReply.getResponse()
          .thenApply(response -> inFlightMessages.remove(step));
      return null;
    }));

    return null;
  }

  private XceiverClientRatis createXceiverClient(OzoneConfiguration conf) {
    List<DatanodeDetails> datanodes = new ArrayList<>();

    datanodes.add(DatanodeDetails.newBuilder()
        .setID(DatanodeID.fromUuidString(serverId))
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .addPort(DatanodeDetails.newPort(Name.RATIS, 9858))
        .build());

    Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.valueOf(pipelineId))
        .setState(PipelineState.OPEN)
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setLeaderId(DatanodeID.fromUuidString(serverId))
        .setNodes(datanodes)
        .build();

    return XceiverClientRatis
        .newXceiverClientRatis(pipeline, conf);
  }

  private ContainerCommandRequestProto createContainerRequest(
      long containerId) {
    return ContainerCommandRequestProto.newBuilder()
        .setContainerID(containerId)
        .setCmdType(Type.CreateContainer)
        .setDatanodeUuid(serverId)
        .setCreateContainer(CreateContainerRequestProto.newBuilder()
            .setContainerType(ContainerType.KeyValueContainer)
            .build())
        .build();

  }

  private ContainerCommandRequestProto createChunkWriteRequest(long containerId,
      long chunkId) {

    long blockId = getPrefix().hashCode() + chunkId / 1000;
    return ContainerCommandRequestProto.newBuilder()
        .setContainerID(containerId)
        .setCmdType(Type.WriteChunk)
        .setDatanodeUuid(serverId)
        .setWriteChunk(WriteChunkRequestProto.newBuilder()
            .setData(dataToWrite)
            .setBlockID(DatanodeBlockID.newBuilder()
                .setContainerID(containerId)
                .setLocalID(blockId)
                .build())
            .setChunkData(ChunkInfo.newBuilder()
                .setChunkName("chunk" + chunkId)
                .setLen(dataToWrite.size())
                .setOffset(0)
                .setChecksumData(ChecksumData.newBuilder()
                    .setBytesPerChecksum(0)
                    .setType(ChecksumType.NONE)
                    .build())
                .build())
            .build())
        .build();

  }

  private void configureGroup() throws IOException {
    ClientId clientId = ClientId.randomId();

    RaftGroupId groupId = RaftGroupId
        .valueOf(UUID.fromString(pipelineId));
    RaftPeerId peerId =
        RaftPeerId.getRaftPeerId(serverId);

    RaftGroup group = RaftGroup.valueOf(groupId,
        RaftPeer.newBuilder()
            .setId(serverId)
            .setAddress(serverAddress)
            .build(),
        RaftPeer.newBuilder()
            .setId(RaftPeerId.valueOf(FAKE_FOLLOWER_ID1))
            .setAddress(FAKE_LEADER_ADDDRESS1)
            .build(),
        RaftPeer.newBuilder()
            .setId(RaftPeerId.valueOf(FAKE_FOLLOWER_ID1))
            .setAddress(FAKE_LEADER_ADDDRESS2)
            .build());
    RaftClient client = RaftClient.newBuilder()
        .setClientId(clientId)
        .setProperties(new RaftProperties())
        .setRaftGroup(group)
        .build();

    RaftClientReply raftClientReply = client.getGroupManagementApi(peerId)
        .add(group);

    LOG.info(
        "Group is configured in the RAFT server (with two fake leader leader)"
            + ": {}",
        raftClientReply);
  }

}
