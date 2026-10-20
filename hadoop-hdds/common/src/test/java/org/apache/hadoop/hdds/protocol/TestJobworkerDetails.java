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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ExtendedJobWorkDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerPortType;
import org.apache.hadoop.hdds.upgrade.JobworkerVersion;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link JobworkerDetails} class.
 */
public class TestJobworkerDetails {

  private static final String TEST_HOST_NAME = "test-host";
  private static final String TEST_IP_ADDRESS = "127.0.0.1";
  private static final int TEST_PORT_VALUE = 9876;

  /**
   * Test copy constructor functionality.
   */
  @Test
  public void testCopy() {
    UUID uuid = UUID.randomUUID();
    JobworkerDetails original = JobworkerDetails.newBuilder()
        .setUuid(uuid)
        .setHostName(TEST_HOST_NAME)
        .setIpAddress(TEST_IP_ADDRESS)
        .addPort(JobworkerPortType.HTTP, TEST_PORT_VALUE)
        .setVersion("1.0.0")
        .setSetupTime(System.currentTimeMillis())
        .setBuildDate("2023-01-01")
        .build();

    JobworkerDetails copy = new JobworkerDetails(original);

    assertEquals(original.getUuid(), copy.getUuid());
    assertEquals(original.getHostName(), copy.getHostName());
    assertEquals(original.getIpAddress(), copy.getIpAddress());
    assertEquals(original.getNetworkLocation(), copy.getNetworkLocation());
    assertEquals(original.getPorts().get(JobworkerPortType.HTTP),
        copy.getPorts().get(JobworkerPortType.HTTP));
    assertEquals(original.getVersion(), copy.getVersion());
    assertEquals(original.getBuildDate(), copy.getBuildDate());
    assertEquals(original.getSetupTime(), copy.getSetupTime());
  }

  /**
   * Test conversion to and from protocol buffers.
   */
  @Test
  public void testProtoBufConversion() {
    UUID uuid = UUID.randomUUID();
    long setupTime = System.currentTimeMillis();
    String version = "1.0.0";
    String buildDate = "2023-01-01";
    String networkName = "test-network";
    String networkLocation = "/dc1/rack1";

    // Object to Proto
    JobworkerDetails original = JobworkerDetails.newBuilder()
        .setUuid(uuid)
        .setHostName(TEST_HOST_NAME)
        .setIpAddress(TEST_IP_ADDRESS)
        .addPort(JobworkerPortType.HTTP, TEST_PORT_VALUE)
        .setNetworkName(networkName)
        .setNetworkLocation(networkLocation)
        .setVersion(version)
        .setSetupTime(setupTime)
        .setBuildDate(buildDate)
        .setJobworkerVersion(JobworkerVersion.CURRENT)
        .build();
    ExtendedJobWorkDetailsProto extendedProto = original.getExtendedProtoBufMessage();

    // Proto to Object
    JobworkerDetails fromProto = JobworkerDetails.getFromProtoBuf(extendedProto);
    assertEquals(original.getUuid(), fromProto.getUuid());
    assertEquals(original.getHostName(), fromProto.getHostName());
    assertEquals(original.getIpAddress(), fromProto.getIpAddress());
    assertEquals(original.getNetworkName(), fromProto.getNetworkName());
    assertEquals(original.getNetworkLocation(), fromProto.getNetworkLocation());
    assertEquals(original.getVersion(), fromProto.getVersion());
    assertEquals(original.getSetupTime(), fromProto.getSetupTime());
    assertEquals(original.getBuildDate(), fromProto.getBuildDate());
    assertEquals(original.getBuildDate(), fromProto.getBuildDate());
    assertEquals(JobworkerVersion.CURRENT, fromProto.getJobworkerVersion());

    Map<JobworkerPortType, Integer> originalPorts = original.getPorts();
    Map<JobworkerPortType, Integer> fromProtoPorts = fromProto.getPorts();
    assertEquals(originalPorts.size(), fromProtoPorts.size());
    for (JobworkerPortType portType : originalPorts.keySet()) {
      assertTrue(fromProtoPorts.containsKey(portType),
          "Port type " + portType + " missing in converted object");
      assertEquals(originalPorts.get(portType), fromProtoPorts.get(portType),
          "Port values should match for type " + portType);
    }
  }

}
