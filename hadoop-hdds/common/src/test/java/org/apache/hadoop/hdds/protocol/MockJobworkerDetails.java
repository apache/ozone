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

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerPortType;
import org.apache.hadoop.hdds.upgrade.JobworkerVersion;
import org.apache.ozone.test.GenericTestUtils;

/**
 * Provides {@link JobworkerDetails} factory methods for testing.
 */
public final class MockJobworkerDetails {

  /**
   * Creates JobworkerDetails with random UUID and random IP address.
   *
   * @return JobworkerDetail
   */
  public static JobworkerDetails randomJobworkerDetails() {
    return randomJobworkerDetails(null);
  }

  /**
   * Creates JobworkerDetails with random UUID and specified network location.
   *
   * @param loc Network location
   * @return JobworkerDetail
   */
  public static JobworkerDetails randomJobworkerDetails(String loc) {
    return createJobworkerDetails(UUID.randomUUID().toString(), loc);
  }

  /**
   * Creates JobworkerDetails with random UUID, specific hostname, and network
   * location.
   *
   * @param hostname Hostname
   * @param loc Network location
   * @return JobworkerDetail
   */
  public static JobworkerDetails createJobworkerDetails(String hostname,
                                                      String loc) {
    Random random = ThreadLocalRandom.current();
    String ipAddress = random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256);
    return createJobworkerDetails(UUID.randomUUID().toString(), hostname,
        ipAddress, loc);
  }

  /**
   * Creates JobworkerDetails with the given information.
   *
   * @param uuid      JobWorker's UUID
   * @param hostname  hostname of JobWorker
   * @param ipAddress ip address of JobWorker
   * @param networkLocation network location of JobWorker
   * @return JobworkerDetail
   */
  public static JobworkerDetails createJobworkerDetails(String uuid,
                                                      String hostname, String ipAddress, String networkLocation) {
    return createJobworkerDetails(uuid, hostname, ipAddress, networkLocation, 0,
        HddsProtos.NodeOperationalState.IN_SERVICE, JobworkerVersion.CURRENT);
  }

  /**
   * Creates JobworkerDetails with the given information.
   *
   * @param uuid      JobWorker's UUID
   * @return JobworkerDetail
   */
  public static JobworkerDetails createJobworkerDetails(String uuid) {
    return createJobworkerDetails(uuid, GenericTestUtils.PortAllocator.HOSTNAME,
        GenericTestUtils.PortAllocator.HOST_ADDRESS, "/default-rack", 0,
        HddsProtos.NodeOperationalState.IN_SERVICE, JobworkerVersion.CURRENT);
  }

  /**
   * Creates JobworkerDetails with the given information.
   *
   * @param uuid            JobWorker's UUID
   * @param hostname        hostname of JobWorker
   * @param ipAddress       ip address of JobWorker
   * @param networkLocation network location of JobWorker
   * @param port            port number for service
   * @param state           node operational state
   * @param jobworkerVersion JobWorker's JobworkerVersion
   * @return JobworkerDetail
   */
  public static JobworkerDetails createJobworkerDetails(
      String uuid, String hostname, String ipAddress, String networkLocation, int port,
      HddsProtos.NodeOperationalState state, JobworkerVersion jobworkerVersion) {

    JobworkerDetails.Builder jw = JobworkerDetails.newBuilder()
        .setUuid(UUID.fromString(uuid))
        .setHostName(hostname)
        .setIpAddress(ipAddress)
        .setNetworkLocation(networkLocation)
        .setOperationalState(state)
        .setJobworkerVersion(jobworkerVersion);
    if (port > 0) {
      jw.addPort(JobworkerPortType.HTTP, port);
    }

    return jw.build();
  }

  /**
   * Creates JobworkerDetails with random UUID and valid local address and port.
   *
   * @return JobworkerDetail
   */
  public static JobworkerDetails randomLocalJobworkerDetails() {
    int port = GenericTestUtils.PortAllocator.getFreePort();
    return createJobworkerDetails(UUID.randomUUID().toString(),
        GenericTestUtils.PortAllocator.HOSTNAME,
        GenericTestUtils.PortAllocator.HOST_ADDRESS, "/default-rack",
        port, HddsProtos.NodeOperationalState.IN_SERVICE, JobworkerVersion.CURRENT);
  }

  private MockJobworkerDetails() {
    throw new UnsupportedOperationException("no instances");
  }
}
