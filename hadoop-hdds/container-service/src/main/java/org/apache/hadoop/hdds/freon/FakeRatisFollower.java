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

package org.apache.hadoop.hdds.freon;

import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto.AppendResult;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to use it to replace original Ratis GRPC outgoing calls.
 */
public final class FakeRatisFollower {

  private static final Logger LOG =
          LoggerFactory.getLogger(FakeRatisFollower.class);
  private static int simulatedLatency = 0;

  static {
    String ratisSimulatedLatency = System.getenv("RATIS_SIMULATED_LATENCY");
    if (ratisSimulatedLatency != null) {
      simulatedLatency = Integer.parseInt(ratisSimulatedLatency);
    }
  }

  private FakeRatisFollower() {
  }

  public static StreamObserver<AppendEntriesRequestProto> appendEntries(
      RaftPeerId raftPeerId,
      StreamObserver<AppendEntriesReplyProto> responseHandler) {
    return new StreamObserver<AppendEntriesRequestProto>() {
      private long maxIndex = -1L;

      @Override
      public void onNext(AppendEntriesRequestProto value) {

        for (LogEntryProto entry : value.getEntriesList()) {
          if (entry.getIndex() > maxIndex) {
            maxIndex = entry.getIndex();
          }
        }

        long maxCommitted = value.getCommitInfosList()
            .stream()
            .mapToLong(CommitInfoProto::getCommitIndex)
            .max().orElseGet(() -> 0L);

        maxCommitted = Math.min(maxCommitted, maxIndex);

        AppendEntriesReplyProto response = AppendEntriesReplyProto.newBuilder()
            .setNextIndex(maxIndex + 1)
            .setFollowerCommit(maxCommitted)
            .setResult(AppendResult.SUCCESS)
            .setTerm(value.getLeaderTerm())
            .setMatchIndex(maxIndex)
            .setServerReply(RaftRpcReplyProto.newBuilder()
                .setSuccess(true)
                .setRequestorId(value.getServerRequest().getRequestorId())
                .setReplyId(raftPeerId.toByteString())
                .setCallId(value.getServerRequest().getCallId())
                .setRaftGroupId(value.getServerRequest().getRaftGroupId()))
            .build();
        maxCommitted = Math.min(value.getLeaderCommit(), maxIndex);
        addLatency();
        responseHandler.onNext(response);
      }

      @Override
      public void onError(Throwable t) {

      }

      @Override
      public void onCompleted() {

      }
    };
  }

  public static RequestVoteReplyProto requestVote(RaftPeerId raftPeerId,
      RequestVoteRequestProto request) {
    addLatency();
    System.out.println("Request vote response");
    return RequestVoteReplyProto.newBuilder()
        .setServerReply(
            RaftRpcReplyProto.newBuilder()
                .setSuccess(true)
                .setRequestorId(request.getServerRequest().getRequestorId())
                .setReplyId(raftPeerId.toByteString())
                .setCallId(request.getServerRequest().getCallId())
                .setRaftGroupId(request.getServerRequest().getRaftGroupId())
        )
        .setTerm(request.getCandidateTerm())
        .build();
  }

  private static void addLatency() {
    if (simulatedLatency > 0) {
      try {
        Thread.sleep(simulatedLatency);
      } catch (InterruptedException e) {
        LOG.error("Interrupted exception while sleeping.", e);
        Thread.currentThread().interrupt();
      }
    }
  }
}
