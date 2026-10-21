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

package org.apache.hadoop.hdds.scm.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HostAndPort} address refresh (HDDS-15533).
 */
public class TestHostAndPort {

  @Test
  public void resolveLatestReturnsNullWhenIpUnchanged() {
    HostAndPort address = new HostAndPort("127.0.0.1", 9861);
    assertNull(address.resolveLatest());
  }

  @Test
  public void setAddressRejectsNull() {
    HostAndPort address = new HostAndPort("127.0.0.1", 9861);
    assertThrows(NullPointerException.class, () -> address.setAddress(null));
  }

  @Test
  public void ipv6HostAndPortString() {
    HostAndPort address = new HostAndPort("2001:db8::1", 9894);
    assertEquals("[2001:db8::1]:9894", address.getHostAndPortString());
  }

  @Test
  public void setAddressDoesNotChangeIdentity() throws Exception {
    HostAndPort address = new HostAndPort("127.0.0.1", 9861);
    InetSocketAddress refreshed =
        new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 0, 7}), 9861);
    address.setAddress(refreshed);
    assertEquals(refreshed, address.getAddress());
    // equals/hashCode stay keyed on host:port so the instance remains a stable map key.
    assertEquals(new HostAndPort("127.0.0.1", 9861), address);
    assertEquals(new HostAndPort("127.0.0.1", 9861).hashCode(), address.hashCode());
  }
}
