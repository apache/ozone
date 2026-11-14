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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.tracing.TestSpanCollector;
import org.junit.jupiter.api.Test;

/**
 * Tests the spans of {@link XceiverClientGrpc}.
 */
class TestXceiverClientGrpcTracing {

  @Test
  void retryOnAnotherDatanodeIsRecordedOnSpan() throws IOException {
    Pipeline pipeline = MockPipeline.createPipeline(Arrays.asList(
        MockDatanodeDetails.randomDatanodeDetails(), MockDatanodeDetails.randomDatanodeDetails()));
    List<DatanodeDetails> tried = new ArrayList<>();
    ContainerCommandRequestProto request = ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.GetBlock)
        .setContainerID(1L)
        .setDatanodeUuid(pipeline.getFirstNode().getUuidString())
        .setGetBlock(ContainerProtos.GetBlockRequestProto.newBuilder()
            .setBlockID(ContainerProtos.DatanodeBlockID.newBuilder().setContainerID(1L).setLocalID(1L)))
        .build();

    try (TestSpanCollector collector = TestSpanCollector.install("client");
         XceiverClientGrpc client = new XceiverClientGrpc(pipeline, new OzoneConfiguration()) {
           @Override
           public XceiverClientReply sendCommandAsync(ContainerCommandRequestProto req, DatanodeDetails dn)
               throws IOException {
             tried.add(dn);
             if (tried.size() == 1) {
               throw new IOException("datanode is down");
             }
             return new XceiverClientReply(CompletableFuture.completedFuture(ContainerCommandResponseProto.newBuilder()
                 .setCmdType(req.getCmdType()).setResult(ContainerProtos.Result.SUCCESS).build()));
           }
         }) {
      assertEquals(ContainerProtos.Result.SUCCESS, client.sendCommand(request).getResult());

      assertEquals(2, tried.size());
      collector.assertAttribute("XceiverClientGrpc.GetBlock", ClientTracingAttributes.HDDS_DATANODE,
          tried.get(1).getHostName());
      collector.assertAttribute("XceiverClientGrpc.GetBlock", ClientTracingAttributes.HDDS_DATANODE_UUID,
          tried.get(1).getUuidString());
      collector.assertEvent("XceiverClientGrpc.GetBlock", ClientTracingAttributes.HDDS_DATANODE_FAILED,
          ClientTracingAttributes.HDDS_DATANODE, tried.get(0).getHostName());
    }
  }
}
