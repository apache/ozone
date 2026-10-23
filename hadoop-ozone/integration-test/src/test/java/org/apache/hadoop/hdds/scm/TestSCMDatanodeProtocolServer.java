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

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer;
import org.apache.hadoop.hdds.tracing.TestSpanCollector;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.junit.jupiter.api.Test;

/**
 * Test for StorageContainerDatanodeProtocolProtos.
 */
public class TestSCMDatanodeProtocolServer {

  @Test
  public void ensureTermAndDeadlineOnCommands()
      throws IOException, TimeoutException {
    OzoneStorageContainerManager scm =
        mock(OzoneStorageContainerManager.class);

    ReplicateContainerCommand command = ContainerTestUtils
        .getReplicateContainerCommand(1, randomDatanodeDetails());
    command.setTerm(5L);
    command.setDeadline(1234L);
    StorageContainerDatanodeProtocolProtos.SCMCommandProto proto =
        SCMDatanodeProtocolServer.getCommandResponse(command, scm);

    assertEquals(StorageContainerDatanodeProtocolProtos.SCMCommandProto
        .Type.replicateContainerCommand, proto.getCommandType());
    assertEquals(5L, proto.getTerm());
    assertEquals(1234L, proto.getDeadlineMsSinceEpoch());
  }

  @Test
  public void testTracing() throws IOException, TimeoutException {
    OzoneStorageContainerManager scm = mock(OzoneStorageContainerManager.class);

    try (TestSpanCollector spans = TestSpanCollector.install("TestSCMDatanodeProtocolServer")) {
      Span span = TracingUtil.createInternalSpan("ReplicationManager");
      ReplicateContainerCommand command;
      try (Scope ignored = span.makeCurrent()) {
        command = ContainerTestUtils.getReplicateContainerCommand(1, randomDatanodeDetails());
      } finally {
        span.end();
      }

      StorageContainerDatanodeProtocolProtos.SCMCommandProto proto =
          SCMDatanodeProtocolServer.getCommandResponse(command, scm);

      assertThat(proto.getTraceID()).contains(span.getSpanContext().getTraceId());
    }
  }
}
