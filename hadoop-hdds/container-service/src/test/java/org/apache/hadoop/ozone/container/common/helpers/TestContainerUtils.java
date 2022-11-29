/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.helpers;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.ByteStringConversion;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.ReadChunk;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getReadChunkResponse;
import static org.apache.hadoop.hdds.HddsUtils.processForDebug;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getDummyCommandRequestProto;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for {@link ContainerUtils}.
 */
public class TestContainerUtils {

  @Test
  public void redactsDataBuffers() {
    // GIVEN
    ContainerCommandRequestProto req = getDummyCommandRequestProto(ReadChunk);
    ChunkBuffer data = ChunkBuffer.wrap(ByteBuffer.wrap(
        "junk".getBytes(UTF_8)));
    ContainerCommandResponseProto resp = getReadChunkResponse(req, data,
        ByteStringConversion::safeWrap);

    // WHEN
    ContainerCommandResponseProto processed = processForDebug(resp);

    // THEN
    ContainerProtos.DataBuffers dataBuffers =
        processed.getReadChunk().getDataBuffers();
    assertEquals(1, dataBuffers.getBuffersCount());
    assertEquals("<redacted>", dataBuffers.getBuffers(0).toString(UTF_8));
  }

}
