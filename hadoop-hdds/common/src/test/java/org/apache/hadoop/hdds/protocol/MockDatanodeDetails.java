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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.ALL_PORTS;

/**
 * Provides {@link DatanodeDetails} factory methods for testing.
 */
public final class MockDatanodeDetails {

  /**
   * Creates DatanodeDetails with random UUID and random IP address.
   *
   * @return DatanodeDetails
   */
  public static DatanodeDetails randomDatanodeDetails() {
    return createDatanodeDetails(UUID.randomUUID());
  }

  /**
   * Creates DatanodeDetails with random UUID, specific hostname and network
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
    return createDatanodeDetails(UUID.randomUUID().toString(), hostname,
        ipAddress, loc);
  }

  /**
   * Creates DatanodeDetails using the given UUID.
   *
   * @param uuid Datanode's UUID
   *
   * @return DatanodeDetails
   */
  public static DatanodeDetails createDatanodeDetails(UUID uuid) {
    Random random = ThreadLocalRandom.current();
    String ipAddress = random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256);
    return createDatanodeDetails(uuid.toString(), "localhost" + "-" + ipAddress,
        ipAddress, null);
  }

  /**
   * Creates DatanodeDetails with the given information.
   *
   * @param uuid      Datanode's UUID
   * @param hostname  hostname of Datanode
   * @param ipAddress ip address of Datanode
   *
   * @return DatanodeDetails
   */
  public static DatanodeDetails createDatanodeDetails(String uuid,
      String hostname, String ipAddress, String networkLocation) {
    return createDatanodeDetails(uuid, hostname, ipAddress, networkLocation, 0);
  }

  public static DatanodeDetails createDatanodeDetails(String uuid,
      String hostname, String ipAddress, String networkLocation, int port) {

    DatanodeDetails.Builder dn = DatanodeDetails.newBuilder()
        .setUuid(UUID.fromString(uuid))
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
   * Creates DatanodeDetails with random UUID and valid local address and port.
   *
   * @return DatanodeDetails
   */
  public static DatanodeDetails randomLocalDatanodeDetails()
      throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return createDatanodeDetails(UUID.randomUUID().toString(),
          socket.getInetAddress().getHostName(),
          socket.getInetAddress().getHostAddress(), null,
          socket.getLocalPort());
    }
  }

  private MockDatanodeDetails() {
    throw new UnsupportedOperationException("no instances");
  }
}
