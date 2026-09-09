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

package org.apache.hadoop.hdds.scm.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link ClientCommandsUtils}.
 */
public class TestClientCommandsUtils {

  private static ContainerCommandRequestProto.Builder baseRequest() {
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(Type.WriteChunk)
        .setContainerID(1)
        .setDatanodeUuid("dn");
  }

  @Test
  void returnsWritePipelineVersionWhenAtLeastZdu() {
    ContainerCommandRequestProto request = baseRequest()
        .setWritePipelineVersion(HDDSVersion.ZDU.serialize())
        .build();

    assertEquals(HDDSVersion.ZDU,
        ClientCommandsUtils.getWritePipelineVersion(request));
  }

  @Test
  void roundsUpToZduWhenBelowZdu() {
    // A recognized pre-ZDU version must be rounded up to ZDU (the write-versioning floor).
    ContainerCommandRequestProto request = baseRequest()
        .setWritePipelineVersion(HDDSVersion.SHORT_CIRCUIT_READS.serialize())
        .build();

    assertEquals(HDDSVersion.ZDU,
        ClientCommandsUtils.getWritePipelineVersion(request));
  }

  @Test
  void defaultsToZduWhenAbsent() {
    ContainerCommandRequestProto request = baseRequest().build();

    assertEquals(HDDSVersion.ZDU,
        ClientCommandsUtils.getWritePipelineVersion(request));
  }

  @Test
  void fallsBackToZduForUnknownVersion() {
    ContainerCommandRequestProto request = baseRequest()
        .setWritePipelineVersion(HDDSVersion.UNKNOWN_VERSION.serialize())
        .build();

    assertEquals(HDDSVersion.ZDU,
        ClientCommandsUtils.getWritePipelineVersion(request));
  }
}
