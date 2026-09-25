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

package org.apache.hadoop.hdds.scm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.junit.jupiter.api.Test;

/**
 * Test the SCM utility class.
 */
public class TestScmUtils {

  private static final String SERVICE_ID = "scmservice";
  private static final String NODE_ID = "scm1";

  /**
   * Verify that a per-node bind host holding an IPv6 literal, including the
   * wildcard {@code ::}, yields a usable listener address.
   */
  @Test
  void testHaBindAddressesAcceptIPv6BindHost() throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ConfUtils.addKeySuffixes(ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_KEY, SERVICE_ID, NODE_ID), "::");
    conf.set(ConfUtils.addKeySuffixes(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_BIND_HOST_KEY, SERVICE_ID, NODE_ID), "::");
    conf.set(ConfUtils.addKeySuffixes(ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_KEY, SERVICE_ID, NODE_ID),
        "2001:db8::1");

    final InetAddress wildcard = InetAddress.getByName("::");
    final InetAddress literal = InetAddress.getByName("2001:db8::1");

    InetSocketAddress addr = ScmUtils.getClientProtocolServerAddress(conf, SERVICE_ID, NODE_ID);
    assertEquals(wildcard, addr.getAddress());
    assertEquals(ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT, addr.getPort());

    addr = ScmUtils.getScmBlockProtocolServerAddress(conf, SERVICE_ID, NODE_ID);
    assertEquals(wildcard, addr.getAddress());
    assertEquals(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT, addr.getPort());

    addr = ScmUtils.getScmDataNodeBindAddress(conf, SERVICE_ID, NODE_ID);
    assertEquals(literal, addr.getAddress());
    assertEquals(ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT, addr.getPort());
  }
}
