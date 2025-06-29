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

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.ozone.test.GenericTestUtils;

/**
 * Provides {@link DatanodeDetails} factory methods for testing.
 */
public final class MockDatanodeDetails {

  /**
   * Creates DatanodeDetails with random ID and random IP address.
   *
   * @return DatanodeDetails
   */
  public static DatanodeDetails randomDatanodeDetails() {
    return createDatanodeDetails(DatanodeID.randomID());
  }

  /**
   * Creates DatanodeDetails with random DatanodeID, specific hostname and network
   * location.
   *
   * @return DatanodeDetails
   */
  public static DatanodeDetails createDatanodeDetails(String hostname,
      String loc) {
    Random random = ThreadLocalRandom.current();
    String ipAddress = random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256);
    return createDatanodeDetails(DatanodeID.randomID(), hostname,
        ipAddress, loc);
  }

  /**
   * Creates DatanodeDetails using the given DatanodeID.
   *
   * @param id Datanode's ID
   *
   * @return DatanodeDetails
   */
  public static DatanodeDetails createDatanodeDetails(DatanodeID id) {
    Random random = ThreadLocalRandom.current();
    String ipAddress = random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256);
    return createDatanodeDetails(id, "localhost" + "-" + ipAddress,
        ipAddress, null);
  }

  /**
   * Creates DatanodeDetails with the given information.
   *
   * @param id      Datanode's ID
   * @param hostname  hostname of Datanode
   * @param ipAddress ip address of Datanode
   *
   * @return DatanodeDetails
   */
  public static DatanodeDetails createDatanodeDetails(DatanodeID id,
      String hostname, String ipAddress, String networkLocation) {
    return createDatanodeDetails(id, hostname, ipAddress, networkLocation, 0);
  }

  public static DatanodeDetails createDatanodeDetails(DatanodeID id,
      String hostname, String ipAddress, String networkLocation, int port) {

    DatanodeDetails.Builder dn = DatanodeDetails.newBuilder()
        .setID(id)
        .setHostName(hostname)
        .setIpAddress(ipAddress)
        .setNetworkLocation(networkLocation)
        .setPersistedOpState(HddsProtos.NodeOperationalState.IN_SERVICE)
        .setPersistedOpStateExpiry(0);

    for (DatanodeDetails.Port.Name name : ALL_PORTS) {
      dn.addPort(DatanodeDetails.newPort(name, port));
    }

    return dn.build();
  }

  /**
   * Creates DatanodeDetails with random ID and valid local address and port.
   *
   * @return DatanodeDetails
   */
  public static DatanodeDetails randomLocalDatanodeDetails() {
    return createDatanodeDetails(DatanodeID.randomID(),
        GenericTestUtils.PortAllocator.HOSTNAME,
        GenericTestUtils.PortAllocator.HOST_ADDRESS, null,
        GenericTestUtils.PortAllocator.getFreePort());
  }

  private MockDatanodeDetails() {
    throw new UnsupportedOperationException("no instances");
  }
}
