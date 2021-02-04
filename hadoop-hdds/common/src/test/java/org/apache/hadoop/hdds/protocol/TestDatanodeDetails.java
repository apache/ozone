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
package org.apache.hadoop.hdds.protocol;

import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.junit.Test;

import java.util.Set;

import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.ALL_PORTS;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.V0_PORTS;
import static org.apache.hadoop.ozone.ClientVersions.DEFAULT_VERSION;
import static org.apache.hadoop.ozone.ClientVersions.VERSION_HANDLES_UNKNOWN_DN_PORTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for {@link DatanodeDetails}.
 */
public class TestDatanodeDetails {

  @Test
  public void protoIncludesNewPortsOnlyForV1() {
    DatanodeDetails subject = MockDatanodeDetails.randomDatanodeDetails();

    HddsProtos.DatanodeDetailsProto proto = subject.toProto(DEFAULT_VERSION);
    assertPorts(proto, V0_PORTS);

    HddsProtos.DatanodeDetailsProto protoV1 = subject.toProto(
        VERSION_HANDLES_UNKNOWN_DN_PORTS);
    assertPorts(protoV1, ALL_PORTS);
  }

  public static void assertPorts(HddsProtos.DatanodeDetailsProto dn,
      Set<Port.Name> expectedPorts) {
    assertEquals(expectedPorts.size(), dn.getPortsCount());
    for (HddsProtos.Port port : dn.getPortsList()) {
      try {
        assertTrue(expectedPorts.contains(Port.Name.valueOf(port.getName())));
      } catch (IllegalArgumentException e) {
        fail("Unknown port: " + port.getName());
      }
    }
  }

}
