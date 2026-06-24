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

package org.apache.hadoop.ozone.local;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;

/**
 * Allocates unique ports for local Ozone services.
 *
 * <p>This allocator ensures that each service gets a unique port,
 * either by using a preferred port or by finding a free ephemeral port.
 * It tracks all reserved ports to prevent conflicts when multiple
 * services are started in the same JVM.</p>
 */
final class PortAllocator {

  private final Set<Integer> reserved = new HashSet<>();

  /**
   * Reserves a port for use by a local service.
   *
   * @param preferredPort the preferred port to use, or 0 to auto-allocate
   * @return the reserved port number
   * @throws IOException if the preferred port is already reserved or
   *                     if no free port can be found
   */
  int reserve(int preferredPort) throws IOException {
    if (preferredPort > 0) {
      if (!reserved.add(preferredPort)) {
        throw new IOException("Port " + preferredPort
            + " is configured more than once.");
      }
      return preferredPort;
    }

    while (true) {
      int candidate = findFreePort();
      if (reserved.add(candidate)) {
        return candidate;
      }
    }
  }

  /**
   * Finds a free ephemeral port by opening and immediately closing
   * a server socket.
   */
  private static int findFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(false);
      return socket.getLocalPort();
    }
  }
}
