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

package org.apache.hadoop.ozone.ha;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetSocketAddress;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.util.OzoneNetUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OzoneNetUtils} class.
 */
public class TestOzoneNetUtils {
  @Test
  public void testGetAddressWithHostName() {
    String fqdn = "pod0.service.com";
    int port = 1234;

    InetSocketAddress addr0 = NetUtils.createSocketAddr(fqdn, port);
    InetSocketAddress addr1 = OzoneNetUtils.getAddressWithHostNameLocal(
            addr0);
    assertEquals("pod0", addr1.getHostName());
    assertEquals(port, addr1.getPort());
  }
}
