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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AsyncApi;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.NotReplicatedException;
import org.apache.ratis.rpc.RpcType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/**
 * Tests for {@link XceiverClientRatis#watchForCommit}: the returned future must complete even when the client is
 * closed while the watch is pending (HDDS-17648).
 */
class TestXceiverClientRatis {

  private static final long INDEX = 10;

  private final RaftClient raftClient = mock(RaftClient.class);
  private final AsyncApi async = mock(AsyncApi.class);
  private final CompletableFuture<RaftClientReply> allCommitted = new CompletableFuture<>();
  private XceiverClientRatis client;

  @BeforeEach
  void setUp() throws Exception {
    when(raftClient.async()).thenReturn(async);
    when(async.watch(INDEX, ReplicationLevel.ALL_COMMITTED)).thenReturn(allCommitted);
    client = connect(new OzoneConfiguration(), null);
  }

  private XceiverClientRatis connect(OzoneConfiguration conf, AtomicReference<Boolean> skipSendForward)
      throws Exception {
    Pipeline pipeline = MockPipeline.createRatisPipeline();
    XceiverClientRatis xceiverClient = XceiverClientRatis.newXceiverClientRatis(pipeline, conf);
    try (MockedStatic<RatisHelper> ratisHelper = mockStatic(RatisHelper.class, CALLS_REAL_METHODS)) {
      ratisHelper.when(() -> RatisHelper.newRaftClient(
              any(RpcType.class), any(Pipeline.class), any(), any(), any(), anyBoolean()))
          .thenAnswer(invocation -> {
            if (skipSendForward != null) {
              skipSendForward.set(invocation.getArgument(5));
            }
            return raftClient;
          });
      xceiverClient.connect();
    }
    return xceiverClient;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void dataStreamSkipsSendForwardOnlyWithPutBlockWithoutRaft(boolean putBlockWithoutRaft) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setDatastreamPutBlockWithoutRaftEnabled(putBlockWithoutRaft);
    conf.setFromObject(clientConfig);
    AtomicReference<Boolean> skipSendForward = new AtomicReference<>();

    connect(conf, skipSendForward).close();
    assertEquals(putBlockWithoutRaft, skipSendForward.get());
  }

  /** The client is closed (for example evicted and released) while the ALL_COMMITTED watch is pending. */
  @Test
  void watchCompletesWhenClientIsClosedDuringWatch() {
    CompletableFuture<XceiverClientReply> reply = client.watchForCommit(INDEX);
    client.close();
    allCommitted.completeExceptionally(new IOException("watch timed out"));

    ExecutionException e = assertThrows(ExecutionException.class, () -> reply.get(10, TimeUnit.SECONDS));
    assertThat(e.getCause()).hasMessageContaining("watch timed out");
  }

  @Test
  void noMajorityWatchWhenClientIsClosed() {
    CompletableFuture<XceiverClientReply> reply = client.watchForCommit(INDEX);
    allCommitted.completeExceptionally(new AlreadyClosedException("client is closed"));

    ExecutionException e = assertThrows(ExecutionException.class, () -> reply.get(10, TimeUnit.SECONDS));
    assertThat(e.getCause()).isInstanceOf(AlreadyClosedException.class);
    verify(async, never()).watch(anyLong(), eq(ReplicationLevel.MAJORITY_COMMITTED));
  }

  /** The future completes even when handling the failed ALL_COMMITTED watch throws. */
  @Test
  void watchCompletesWhenFailureHandlingThrows() {
    CompletableFuture<XceiverClientReply> reply = client.watchForCommit(INDEX);
    // no commit infos: handleFailedAllCommit throws NullPointerException
    allCommitted.completeExceptionally(new NotReplicatedException(1, ReplicationLevel.ALL_COMMITTED, INDEX));

    ExecutionException e = assertThrows(ExecutionException.class, () -> reply.get(10, TimeUnit.SECONDS));
    assertThat(e.getCause()).isInstanceOf(NullPointerException.class);
  }

  /** Unchanged: when ALL_COMMITTED fails on an open client, a MAJORITY_COMMITTED watch is used instead. */
  @Test
  void fallsBackToMajorityWatch() throws Exception {
    RaftClientReply majorityReply = mock(RaftClientReply.class);
    when(majorityReply.getCommitInfos()).thenReturn(Collections.emptyList());
    when(async.watch(INDEX, ReplicationLevel.MAJORITY_COMMITTED))
        .thenReturn(CompletableFuture.completedFuture(majorityReply));

    CompletableFuture<XceiverClientReply> reply = client.watchForCommit(INDEX);
    allCommitted.completeExceptionally(new IOException("watch timed out"));

    assertEquals(INDEX, reply.get(10, TimeUnit.SECONDS).getLogIndex());
  }
}
