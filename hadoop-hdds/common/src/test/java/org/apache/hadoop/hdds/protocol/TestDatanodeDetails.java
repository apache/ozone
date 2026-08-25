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

package org.apache.hadoop.hdds.protocol;

import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.ALL_PORTS;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.V0_PORTS;
import static org.apache.hadoop.ozone.ClientVersion.DEFAULT_VERSION;
import static org.apache.hadoop.ozone.ClientVersion.VERSION_HANDLES_UNKNOWN_DN_PORTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.ClientVersion;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link DatanodeDetails}.
 */
public class TestDatanodeDetails {

  @Test
  void protoIncludesNewPortsOnlyForV1() {
    DatanodeDetails subject = MockDatanodeDetails.randomDatanodeDetails();

    HddsProtos.DatanodeDetailsProto proto =
        subject.toProto(DEFAULT_VERSION);
    assertPorts(proto, V0_PORTS);

    HddsProtos.DatanodeDetailsProto protoV1 =
        subject.toProto(VERSION_HANDLES_UNKNOWN_DN_PORTS);
    assertPorts(protoV1, ALL_PORTS);
  }

  @Test
  void testRequiredPortsProto() {
    DatanodeDetails subject = MockDatanodeDetails.randomDatanodeDetails();
    Set<Port.Name> requiredPorts = Stream.of(Port.Name.STANDALONE, Port.Name.RATIS)
        .collect(Collectors.toSet());
    HddsProtos.DatanodeDetailsProto proto =
        subject.toProto(ClientVersion.CURRENT, requiredPorts);
    assertPorts(proto, ImmutableSet.copyOf(requiredPorts));

    HddsProtos.DatanodeDetailsProto ioPortProto =
        subject.toProto(ClientVersion.CURRENT, Name.IO_PORTS);
    assertPorts(ioPortProto, ImmutableSet.copyOf(Name.IO_PORTS));
  }

  @Test
  public void testNewBuilderCurrentVersion() {
    // When the current version proto field is absent, the builder default (DEFAULT_VERSION) applies.
    DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
    Set<Port.Name> requiredPorts = Stream.of(Port.Name.STANDALONE, Port.Name.RATIS)
        .collect(Collectors.toSet());
    HddsProtos.DatanodeDetailsProto.Builder protoBuilder = dn.toProtoBuilder(ClientVersion.CURRENT, requiredPorts);
    protoBuilder.clearCurrentVersion();
    DatanodeDetails dn2 = DatanodeDetails.newBuilder(protoBuilder.build()).build();
    assertEquals(HDDSVersion.DEFAULT_VERSION, dn2.getCurrentVersion());

    // When the proto field is present, it round-trips correctly.
    protoBuilder = dn.toProtoBuilder(ClientVersion.CURRENT, requiredPorts);
    DatanodeDetails dn3 = DatanodeDetails.newBuilder(
        protoBuilder.setCurrentVersion(HDDSVersion.SOFTWARE_VERSION.serialize()).build())
        .build();
    assertEquals(HDDSVersion.SOFTWARE_VERSION, dn3.getCurrentVersion());
  }

  @Test
  void portsChangedComparesNameAndValue() {
    DatanodeID id = DatanodeID.randomID();
    DatanodeDetails base = DatanodeDetails.newBuilder()
        .setID(id)
        .addPort(DatanodeDetails.newStandalonePort(9858))
        .addPort(DatanodeDetails.newRatisPort(9859))
        .build();

    // Identical name/value set: no change.
    DatanodeDetails same = DatanodeDetails.newBuilder()
        .setID(id)
        .addPort(DatanodeDetails.newStandalonePort(9858))
        .addPort(DatanodeDetails.newRatisPort(9859))
        .build();
    assertFalse(base.portsChanged(same));

    // Same names, one different value: detected (Port.equals ignores value).
    DatanodeDetails changedValue = DatanodeDetails.newBuilder()
        .setID(id)
        .addPort(DatanodeDetails.newStandalonePort(9858))
        .addPort(DatanodeDetails.newRatisPort(1234))
        .build();
    assertTrue(base.portsChanged(changedValue));

    // Extra port: detected (key set differs).
    DatanodeDetails extraPort = DatanodeDetails.newBuilder()
        .setID(id)
        .addPort(DatanodeDetails.newStandalonePort(9858))
        .addPort(DatanodeDetails.newRatisPort(9859))
        .addPort(DatanodeDetails.newPort(Name.RATIS_DATASTREAM, 9860))
        .build();
    assertTrue(base.portsChanged(extraPort));
  }

  public static void assertPorts(HddsProtos.DatanodeDetailsProto dn,
      Set<Port.Name> expectedPorts) throws IllegalArgumentException {
    assertEquals(expectedPorts.size(), dn.getPortsCount());
    for (HddsProtos.Port port : dn.getPortsList()) {
      assertThat(expectedPorts).contains(Port.Name.valueOf(port.getName()));
    }
  }

}
