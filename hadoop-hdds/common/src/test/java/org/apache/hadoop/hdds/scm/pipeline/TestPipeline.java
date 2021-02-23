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
package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.ALL_PORTS;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.V0_PORTS;
import static org.apache.hadoop.hdds.protocol.TestDatanodeDetails.assertPorts;
import static org.apache.hadoop.ozone.ClientVersions.DEFAULT_VERSION;
import static org.apache.hadoop.ozone.ClientVersions.VERSION_HANDLES_UNKNOWN_DN_PORTS;

/**
 * Test for {@link Pipeline}.
 */
public class TestPipeline {

  @Test
  public void protoIncludesNewPortsOnlyForV1() throws IOException {
    Pipeline subject = MockPipeline.createPipeline(3);

    HddsProtos.Pipeline proto = subject.getProtobufMessage(DEFAULT_VERSION);
    for (HddsProtos.DatanodeDetailsProto dn : proto.getMembersList()) {
      assertPorts(dn, V0_PORTS);
    }

    HddsProtos.Pipeline protoV1 = subject.getProtobufMessage(
        VERSION_HANDLES_UNKNOWN_DN_PORTS);
    for (HddsProtos.DatanodeDetailsProto dn : protoV1.getMembersList()) {
      assertPorts(dn, ALL_PORTS);
    }
  }
}
