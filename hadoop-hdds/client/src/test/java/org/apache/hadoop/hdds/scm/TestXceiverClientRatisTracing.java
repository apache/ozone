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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.trace.SpanKind;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.tracing.TestSpanCollector;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AsyncApi;
import org.apache.ratis.protocol.RaftClientReply;
import org.junit.jupiter.api.Test;

/**
 * Tests the spans of {@link XceiverClientRatis}.
 */
class TestXceiverClientRatisTracing {

  private static final String SPAN = "XceiverClientRatis.PutBlock";

  @Test
  void spanEndsWhenReplyArrives() throws Exception {
    CompletableFuture<RaftClientReply> reply = new CompletableFuture<>();
    try (TestSpanCollector collector = TestSpanCollector.install("client")) {
      clientReturning(reply).sendCommandAsync(putBlockRequest());

      assertTrue(collector.named(SPAN).isEmpty(), "the span must stay open until the reply arrives");
      reply.complete(mock(RaftClientReply.class));

      collector.assertKind(SPAN, SpanKind.CLIENT);
      collector.assertNotError(SPAN);
    }
  }

  @Test
  void failedReplyIsRecordedOnSpan() throws Exception {
    CompletableFuture<RaftClientReply> reply = new CompletableFuture<>();
    try (TestSpanCollector collector = TestSpanCollector.install("client")) {
      clientReturning(reply).sendCommandAsync(putBlockRequest());

      reply.completeExceptionally(new IOException("datanode is down"));

      collector.assertError(SPAN, "datanode is down");
      collector.assertExceptionRecorded(SPAN, IOException.class);
    }
  }

  @SuppressWarnings("unchecked")
  private static XceiverClientRatis clientReturning(CompletableFuture<RaftClientReply> reply) throws Exception {
    AsyncApi async = mock(AsyncApi.class);
    when(async.send(any())).thenReturn(reply);
    RaftClient raftClient = mock(RaftClient.class);
    when(raftClient.async()).thenReturn(async);
    XceiverClientRatis client =
        XceiverClientRatis.newXceiverClientRatis(MockPipeline.createRatisPipeline(), new OzoneConfiguration());
    Field field = XceiverClientRatis.class.getDeclaredField("client");
    field.setAccessible(true);
    ((AtomicReference<RaftClient>) field.get(client)).set(raftClient);
    return client;
  }

  private static ContainerCommandRequestProto putBlockRequest() {
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.PutBlock)
        .setContainerID(1L)
        .setDatanodeUuid("dn")
        .setPutBlock(ContainerProtos.PutBlockRequestProto.newBuilder()
            .setBlockData(ContainerProtos.BlockData.newBuilder()
                .setBlockID(ContainerProtos.DatanodeBlockID.newBuilder().setContainerID(1L).setLocalID(1L))))
        .build();
  }
}
