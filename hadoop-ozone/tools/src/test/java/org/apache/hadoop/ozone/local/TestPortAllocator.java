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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link PortAllocator}.
 */
class TestPortAllocator {

  @Test
  void reserveWithPreferredPortReturnsPreferred() throws IOException {
    PortAllocator allocator = new PortAllocator();
    int port = allocator.reserve(9878);
    assertEquals(9878, port);
  }

  @Test
  void reserveWithZeroAllocatesEphemeralPort() throws IOException {
    PortAllocator allocator = new PortAllocator();
    int port = allocator.reserve(0);
    assertTrue(port > 0, "Should allocate a valid port");
    assertTrue(port <= 65535, "Port should be in valid range");
  }

  @Test
  void reserveAllocatesUniqueEphemeralPorts() throws IOException {
    PortAllocator allocator = new PortAllocator();
    Set<Integer> ports = new HashSet<>();

    for (int i = 0; i < 10; i++) {
      int port = allocator.reserve(0);
      assertTrue(ports.add(port),
          "Port " + port + " was allocated more than once");
    }
  }

  @Test
  void reserveRejectsDuplicatePreferredPort() throws IOException {
    PortAllocator allocator = new PortAllocator();
    allocator.reserve(9878);

    IOException exception = assertThrows(IOException.class,
        () -> allocator.reserve(9878));
    assertTrue(exception.getMessage().contains("9878"));
    assertTrue(exception.getMessage().contains("more than once"));
  }

  @Test
  void multipleAllocatorsAreIndependent() throws IOException {
    PortAllocator allocator1 = new PortAllocator();
    PortAllocator allocator2 = new PortAllocator();

    // Both should be able to reserve the same preferred port
    // (they track independently, actual port conflict would happen at bind time)
    int port1 = allocator1.reserve(9878);
    int port2 = allocator2.reserve(9878);

    assertEquals(port1, port2);
  }

  @Test
  void ephemeralPortsAreDifferentEachTime() throws IOException {
    PortAllocator allocator1 = new PortAllocator();
    PortAllocator allocator2 = new PortAllocator();

    int port1 = allocator1.reserve(0);
    int port2 = allocator2.reserve(0);

    // While not guaranteed, ephemeral ports should generally be different
    // This test may occasionally fail but validates the allocation mechanism
    assertNotEquals(port1, port2,
        "Ephemeral ports from different allocators should typically differ");
  }
}
