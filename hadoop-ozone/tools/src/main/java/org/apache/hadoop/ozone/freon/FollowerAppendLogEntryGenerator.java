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
package org.apache.hadoop.ozone.freon;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;

import com.codahale.metrics.Timer;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.concurrent.TimedSemaphore;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.proto.RaftProtos.RaftGroupIdProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.RaftProtos.StateMachineEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto.Builder;
import org.apache.ratis.proto.RaftProtos.TermIndexProto;
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
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Freon test to test one single datanode with a fake leader (this test).
 * <p>
 * To use this test, start one datanode, but set
 * OZONE_DATANODE_STANDALONE_TEST=follower first.
 * <p>
 * After that this test can be started. All the metadata/storage should be
 * cleaned up before restarting the test.
 */
@Command(name = "falg",
    aliases = "follower-append-log-generator",
    description = "Generate append log entries to a follower server",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class FollowerAppendLogEntryGenerator extends BaseAppendLogGenerator
    implements Callable<Void>, StreamObserver<AppendEntriesReplyProto> {

  public static final String FAKE_LEADER_ADDDRESS = "localhost:1234";

  private static final Logger LOG =
      LoggerFactory.getLogger(FollowerAppendLogEntryGenerator.class);

  private static final String FAKE_LEADER_ID =
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

  @Option(names = {"-b", "--batching"},
      description = "Number of write chunks requests in one AppendLogEntry",
      defaultValue = "2")
  private int batching;

  @Option(names = {"-i", "--next-index"},
      description = "The next index in the term 2 to continue a test. (If "
          + "zero, a new ratis ring will be intialized with configureGroup "
          + "call and vote)",
      defaultValue = "0")
  private long nextIndex;

  @Option(names = {"--rate-limit"},
      description = "Maximum number of requests per second (if bigger than 0)",
      defaultValue = "0")
  private int rateLimit;

  private TimedSemaphore rateLimiter;

  private RaftPeerProto requestor;

  private long term = 2L;

  private RaftServerProtocolServiceStub stub;

  private Random callIdRandom = new Random();

  private ByteString dataToWrite;

  private Timer timer;

  private StreamObserver<AppendEntriesRequestProto> sender;

  @Override
  public Void call() throws Exception {
    inFlightMessages = new LinkedBlockingQueue<>(inflightLimit);

    timer = getMetrics().timer("append-entry");
    byte[] data = RandomStringUtils.randomAscii(chunkSize)
        .getBytes(StandardCharsets.UTF_8);

    dataToWrite = ByteString.copyFrom(data);

    OzoneConfiguration conf = createOzoneConfiguration();

    setServerIdFromFile(conf);

    Preconditions.assertTrue(getThreadNo() == 1,
        "This test should be executed from one thread");

    //the raft identifier which is used by the freon
    requestor = RaftPeerProto.newBuilder()
        .setId(RaftPeerId.valueOf(FAKE_LEADER_ID).toByteString())
        .setAddress(FAKE_LEADER_ADDDRESS)
        .build();

    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forTarget(serverAddress);
    channelBuilder.negotiationType(NegotiationType.PLAINTEXT);
    ManagedChannel build = channelBuilder.build();
    stub = RaftServerProtocolServiceGrpc.newStub(build);

    if (rateLimit != 0) {
      rateLimiter = new TimedSemaphore(1, TimeUnit.SECONDS, rateLimit);
    }

    init();

    sender = stub.appendEntries(this);

    if (nextIndex == 0) {
      //first: configure a new ratis group (one follower, one fake leader
      // (freon))
      configureGroup();

      RequestVoteReplyProto vote = requestVote().get(1000, TimeUnit.SECONDS);
      LOG.info("Datanode answered to the vote request: {}", vote);
      if (!vote.getServerReply().getSuccess()) {
        throw new RuntimeException(
            "Datanode didn't vote to the fake freon leader.");
      }

      //send the first appendEntry. This one is special as it initialized the
      // log.
      long callId = callIdRandom.nextLong();
      inFlightMessages.put(callId);
      sender.onNext(createInitialLogEntry(callId));

      nextIndex = 1L;
    }

    //We can generate as mach entry as we need.
    runTests(this::sendAppendLogEntryRequest);

    if (rateLimiter != null) {
      rateLimiter.shutdown();
    }

    return null;
  }

  /**
   * Seend a new HB and record the call id to handle response.
   */
  private void sendAppendLogEntryRequest(long sequence) {
    timer.time(() -> {
      try {
        long callId = callIdRandom.nextLong();
        inFlightMessages.put(callId);
        sender.onNext(createAppendLogEntry(sequence, callId));
      } catch (Exception e) {
        LOG.error(
            "Error while sending new append entry request (HB) to the "
                + "follower", e);
      }
    });
  }

  /**
   * Create the appendLogEntry message (Raft envelope + ozone payload).
   */
  private AppendEntriesRequestProto createAppendLogEntry(long sequence,
      long callId) {
    AppendEntriesRequestProto.Builder requestBuilder =
        AppendEntriesRequestProto.newBuilder();

    if (rateLimiter != null) {
      try {
        rateLimiter.acquire();
      } catch (InterruptedException e) {
        LOG.error("Rate limiter acquire has been interrupted", e);
      }
    }
    long previousLog = nextIndex - 1;
    for (int i = 0; i < batching; i++) {
      long index = nextIndex++;

      long chunkId = batching * sequence + i;
      long blockId = chunkId / 1000;
      long containerId = blockId / 1000;
      //ozone specific
      ByteString payload = ContainerCommandRequestProto.newBuilder()
          .setContainerID(containerId)
          .setCmdType(Type.WriteChunk)
          .setDatanodeUuid(serverId)
          .setWriteChunk(WriteChunkRequestProto.newBuilder()
              .setData(ByteString.EMPTY)
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
          .build().toByteString();

      //ratis specific
      Builder stateMachinelogEntry = StateMachineLogEntryProto.newBuilder()
          .setCallId(callId)
          .setClientId(ClientId.randomId().toByteString())
          .setLogData(payload)
          .setStateMachineEntry(
              StateMachineEntryProto.newBuilder()
                  .setStateMachineData(dataToWrite)
                  .build());

      //ratis specific
      LogEntryProto logEntry = LogEntryProto.newBuilder()
          .setTerm(term)
          .setIndex(index)
          .setStateMachineLogEntry(stateMachinelogEntry)
          .build();

      requestBuilder.addEntries(logEntry);
    }

    //the final AppendEntriesRequest includes all the previous chunk writes
    // (log entries).
    requestBuilder
        .setPreviousLog(
            TermIndexProto.newBuilder().setTerm(term)
                .setIndex(previousLog).build())
        .setLeaderCommit(Math.max(0, previousLog - batching))
        .addCommitInfos(CommitInfoProto
            .newBuilder()
            .setServer(requestor)
            .setCommitIndex(Math.max(0, previousLog - batching))
            .build())
        .setLeaderTerm(term)
        .setServerRequest(createServerRequest(callId))
        .build();

    return requestBuilder.build();

  }

  /**
   * Configure raft group before using raft service.
   */
  private void configureGroup() throws IOException {

    ClientId clientId = ClientId.randomId();

    RaftGroupId groupId = RaftGroupId
        .valueOf(UUID.fromString(pipelineId));

    RaftPeerId peerId =
        RaftPeerId.getRaftPeerId(serverId);

    RaftGroup group = RaftGroup.valueOf(groupId,
        RaftPeer.newBuilder().setId(serverId).setAddress(serverAddress).build(),
        RaftPeer.newBuilder()
            .setId(RaftPeerId.valueOf(FAKE_LEADER_ID))
            .setAddress(FAKE_LEADER_ADDDRESS)
            .build());
    RaftClient client = RaftClient.newBuilder()
        .setClientId(clientId)
        .setProperties(new RaftProperties(true))
        .setRaftGroup(group)
        .build();

    RaftClientReply raftClientReply = client.getGroupManagementApi(peerId)
        .add(group);

    LOG.info(
        "Group is configured in the RAFT server (one follower, one fake "
            + "leader): {}", raftClientReply);
  }

  /**
   * Handle async response.
   */
  @Override
  public void onNext(AppendEntriesReplyProto reply) {
    long callId = reply.getServerReply().getCallId();
    if (!inFlightMessages.remove(callId)) {
      LOG.warn(
          "Received message with callId which was not used to send message: {}",
          callId);
      LOG.info("{}", reply);
    }
    long lastCommit = reply.getFollowerCommit();
    if (lastCommit % 1000 == 0) {
      long currentIndex = getAttemptCounter().get();
      if (currentIndex - lastCommit > batching * 3) {
        LOG.warn(
            "Last committed index ({}) is behind the current index ({}) on "
                + "the client side.",
            lastCommit, currentIndex);
      }
    }

  }

  @Override
  public void onError(Throwable t) {
    LOG.error("Error on sending message", t);
  }

  @Override
  public void onCompleted() {

  }

  /**
   * Create initial log entry to initialize the log.
   */
  private AppendEntriesRequestProto createInitialLogEntry(long callId) {

    RaftRpcRequestProto serverRequest = createServerRequest(callId);

    long index = 0L;
    LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(term)
        .setIndex(index)
        .setConfigurationEntry(
            RaftConfigurationProto.newBuilder()
                .addPeers(RaftPeerProto.newBuilder()
                    .setId(RaftPeerId.valueOf(serverAddress).toByteString())
                    .setAddress(serverAddress)
                    .build())
                .addPeers(requestor)
                .build()
        )
        .build();

    return AppendEntriesRequestProto.newBuilder()
        .setLeaderTerm(term)
        .addEntries(logEntry)
        .setServerRequest(serverRequest)
        .build();
  }

  /**
   * Pseudo sync call to request a vote.
   *
   */
  private CompletableFuture<RequestVoteReplyProto> requestVote() {
    CompletableFuture<RequestVoteReplyProto> response =
        new CompletableFuture<>();
    RequestVoteRequestProto voteRequest = RequestVoteRequestProto.newBuilder()
        .setServerRequest(createServerRequest(callIdRandom.nextLong()))
        .setCandidateLastEntry(
            TermIndexProto.newBuilder()
                .setIndex(0L)
                .setTerm(term)
                .build()
        )
        .build();

    stub.requestVote(voteRequest,
        new StreamObserver<RequestVoteReplyProto>() {
          @Override
          public void onNext(RequestVoteReplyProto value) {
            response.complete(value);
          }

          @Override
          public void onError(Throwable t) {
            response.completeExceptionally(t);
          }

          @Override
          public void onCompleted() {

          }
        });
    return response;
  }

  private RaftRpcRequestProto createServerRequest(long callId) {
    RaftGroupId raftGroupId = RaftGroupId
        .valueOf(UUID.fromString(pipelineId));

    return RaftRpcRequestProto.newBuilder()
        .setRaftGroupId(
            RaftGroupIdProto.newBuilder().setId(raftGroupId.toByteString())
                .build())
        .setRequestorId(requestor.getId())
        .setCallId(callId)
        .build();
  }

}
