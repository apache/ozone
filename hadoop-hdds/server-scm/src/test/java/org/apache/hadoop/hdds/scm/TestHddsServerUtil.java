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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_KEY;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getSCMAddressForDatanodes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.junit.jupiter.api.Test;

/**
 * Test the HDDS server side utilities.
 */
public class TestHddsServerUtil {

  /**
   * Verify that the datanode endpoint is parsed correctly.
   * This tests the logic used by the DataNodes to determine which address
   * to connect to.
   */
  @Test
  public void testGetScmDataNodeAddress() {
    final OzoneConfiguration conf = new OzoneConfiguration();

    // First try a client address with just a host name. Verify it falls
    // back to the default port.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4");
    InetSocketAddress addr = NetUtils.createSocketAddr(
        SCMNodeInfo.buildNodeInfo(conf).get(0).getScmDatanodeAddress());
    assertEquals("1.2.3.4", addr.getHostString());
    assertEquals(ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT, addr.getPort());

    // Next try a client address with just a host name and port.
    // Verify the port is ignored and the default DataNode port is used.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    addr = NetUtils.createSocketAddr(
        SCMNodeInfo.buildNodeInfo(conf).get(0).getScmDatanodeAddress());
    assertEquals("1.2.3.4", addr.getHostString());
    assertEquals(ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT, addr.getPort());

    // Set both OZONE_SCM_CLIENT_ADDRESS_KEY and
    // OZONE_SCM_DATANODE_ADDRESS_KEY.
    // Verify that the latter overrides and the port number is still the
    // default.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "5.6.7.8");
    addr = NetUtils.createSocketAddr(
            SCMNodeInfo.buildNodeInfo(conf).get(0).getScmDatanodeAddress());
    assertEquals("5.6.7.8", addr.getHostString());
    assertEquals(ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT, addr.getPort());

    // Set both OZONE_SCM_CLIENT_ADDRESS_KEY and
    // OZONE_SCM_DATANODE_ADDRESS_KEY.
    // Verify that the latter overrides and the port number from the latter is
    // used.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "5.6.7.8:200");
    addr = NetUtils.createSocketAddr(
        SCMNodeInfo.buildNodeInfo(conf).get(0).getScmDatanodeAddress());
    assertEquals("5.6.7.8", addr.getHostString());
    assertEquals(200, addr.getPort());
  }

  /**
   * Verify that the client endpoint bind address is computed correctly.
   * This tests the logic used by the SCM to determine its own bind address.
   */
  @Test
  public void testScmClientBindHostDefault() {
    final OzoneConfiguration conf = new OzoneConfiguration();

    // The bind host should be 0.0.0.0 unless OZONE_SCM_CLIENT_BIND_HOST_KEY
    // is set differently.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4");
    InetSocketAddress addr = HddsServerUtil.getScmClientBindAddress(conf);
    assertEquals("0.0.0.0", addr.getHostString());
    assertEquals(ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT, addr.getPort());

    // The bind host should be 0.0.0.0 unless OZONE_SCM_CLIENT_BIND_HOST_KEY
    // is set differently. The port number from OZONE_SCM_CLIENT_ADDRESS_KEY
    // should be respected.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "1.2.3.4:200");
    addr = HddsServerUtil.getScmClientBindAddress(conf);
    assertEquals("0.0.0.0", addr.getHostString());
    assertEquals(100, addr.getPort());

    // OZONE_SCM_CLIENT_BIND_HOST_KEY should be respected.
    // Port number should be default if none is specified via
    // OZONE_SCM_DATANODE_ADDRESS_KEY.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "1.2.3.4");
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_KEY, "5.6.7.8");
    addr = HddsServerUtil.getScmClientBindAddress(conf);
    assertEquals("5.6.7.8", addr.getHostString());
    assertEquals(ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT, addr.getPort());

    // OZONE_SCM_CLIENT_BIND_HOST_KEY should be respected.
    // Port number from OZONE_SCM_CLIENT_ADDRESS_KEY should be
    // respected.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "1.2.3.4:200");
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_KEY, "5.6.7.8");
    addr = HddsServerUtil.getScmClientBindAddress(conf);
    assertEquals("5.6.7.8", addr.getHostString());
    assertEquals(100, addr.getPort());
  }

  /**
   * Verify that the DataNode endpoint bind address is computed correctly.
   * This tests the logic used by the SCM to determine its own bind address.
   */
  @Test
  public void testScmDataNodeBindHostDefault() {
    final OzoneConfiguration conf = new OzoneConfiguration();

    // The bind host should be 0.0.0.0 unless OZONE_SCM_DATANODE_BIND_HOST_KEY
    // is set differently.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4");
    InetSocketAddress addr = HddsServerUtil.getScmDataNodeBindAddress(conf);
    assertEquals("0.0.0.0", addr.getHostString());
    assertEquals(ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT, addr.getPort());

    // The bind host should be 0.0.0.0 unless OZONE_SCM_DATANODE_BIND_HOST_KEY
    // is set differently. The port number from OZONE_SCM_DATANODE_ADDRESS_KEY
    // should be respected.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "1.2.3.4:200");
    addr = HddsServerUtil.getScmDataNodeBindAddress(conf);
    assertEquals("0.0.0.0", addr.getHostString());
    assertEquals(200, addr.getPort());

    // OZONE_SCM_DATANODE_BIND_HOST_KEY should be respected.
    // Port number should be default if none is specified via
    // OZONE_SCM_DATANODE_ADDRESS_KEY.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "1.2.3.4");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_KEY, "5.6.7.8");
    addr = HddsServerUtil.getScmDataNodeBindAddress(conf);
    assertEquals("5.6.7.8", addr.getHostString());
    assertEquals(ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT, addr.getPort());

    // OZONE_SCM_DATANODE_BIND_HOST_KEY should be respected.
    // Port number from OZONE_SCM_DATANODE_ADDRESS_KEY should be
    // respected.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "1.2.3.4:200");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_KEY, "5.6.7.8");
    addr = HddsServerUtil.getScmDataNodeBindAddress(conf);
    assertEquals("5.6.7.8", addr.getHostString());
    assertEquals(200, addr.getPort());
  }

  @Test
  void testGetSCMAddresses() {
    final OzoneConfiguration conf = new OzoneConfiguration();
    Collection<InetSocketAddress> addresses;
    InetSocketAddress addr;
    Iterator<InetSocketAddress> it;

    // Verify valid IP address setup
    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, "1.2.3.4");
    addresses = getSCMAddressForDatanodes(conf);
    assertEquals(1, addresses.size());
    addr = addresses.iterator().next();
    assertEquals("1.2.3.4", addr.getHostName());
    assertEquals(OZONE_SCM_DATANODE_PORT_DEFAULT, addr.getPort());

    // Verify valid hostname setup
    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, "scm1");
    addresses = getSCMAddressForDatanodes(conf);
    assertEquals(1, addresses.size());
    addr = addresses.iterator().next();
    assertEquals("scm1", addr.getHostName());
    assertEquals(OZONE_SCM_DATANODE_PORT_DEFAULT, addr.getPort());

    // Verify valid hostname and port
    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, "scm1:1234");
    addresses = getSCMAddressForDatanodes(conf);
    assertEquals(1, addresses.size());
    addr = addresses.iterator().next();
    assertEquals("scm1", addr.getHostName());
    assertEquals(1234, addr.getPort());

    final Map<String, Integer> hostsAndPorts = new HashMap<>();
    hostsAndPorts.put("scm1", 1234);
    hostsAndPorts.put("scm2", 2345);
    hostsAndPorts.put("scm3", 3456);

    // Verify multiple hosts and port
    conf.setStrings(
        ScmConfigKeys.OZONE_SCM_NAMES, "scm1:1234,scm2:2345,scm3:3456");
    addresses = getSCMAddressForDatanodes(conf);
    assertEquals(3, addresses.size());
    it = addresses.iterator();
    HashMap<String, Integer> expected1 = new HashMap<>(hostsAndPorts);
    while (it.hasNext()) {
      InetSocketAddress current = it.next();
      assertTrue(expected1.remove(current.getHostName(),
          current.getPort()));
    }
    assertThat(expected1).isEmpty();

    // Verify names with spaces
    conf.setStrings(
        ScmConfigKeys.OZONE_SCM_NAMES, " scm1:1234, scm2:2345 , scm3:3456 ");
    addresses = getSCMAddressForDatanodes(conf);
    assertEquals(3, addresses.size());
    it = addresses.iterator();
    HashMap<String, Integer> expected2 = new HashMap<>(hostsAndPorts);
    while (it.hasNext()) {
      InetSocketAddress current = it.next();
      assertTrue(expected2.remove(current.getHostName(),
          current.getPort()));
    }
    assertThat(expected2).isEmpty();

    // Verify empty value
    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, "");
    assertThrows(IllegalArgumentException.class,
        () -> getSCMAddressForDatanodes(conf),
        "Empty value should cause an IllegalArgumentException");

    // Verify invalid hostname
    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, "s..x..:1234");
    assertThrows(IllegalArgumentException.class,
        () -> getSCMAddressForDatanodes(conf),
        "An invalid hostname should cause an IllegalArgumentException");

    // Verify invalid port
    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, "scm:xyz");
    assertThrows(IllegalArgumentException.class,
        () -> getSCMAddressForDatanodes(conf),
        "An invalid port should cause an IllegalArgumentException");

    // Verify a mixed case (valid and invalid value both appears)
    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, "scm1:1234, scm:xyz");
    assertThrows(IllegalArgumentException.class,
        () -> getSCMAddressForDatanodes(conf),
        "An invalid value should cause an IllegalArgumentException");
  }

  @Test
  void testGetSCMAddressesWithHAConfig() {
    OzoneConfiguration conf = new OzoneConfiguration();
    String scmServiceId = "scmserviceId";
    String[] nodes = new String[]{"scm1", "scm2", "scm3"};
    conf.set(ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY, scmServiceId);
    conf.set(ScmConfigKeys.OZONE_SCM_NODES_KEY + "." + scmServiceId,
        "scm1,scm2,scm3");

    int port = 9880;
    List<String> expected = new ArrayList<>();
    for (String nodeId : nodes) {
      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_ADDRESS_KEY,
          scmServiceId, nodeId), "scm");
      conf.setInt(ConfUtils.addKeySuffixes(OZONE_SCM_DATANODE_PORT_KEY,
          scmServiceId, nodeId), ++port);
      expected.add("scm" + ":" + port);
    }

    Collection<InetSocketAddress> scmAddressList =
        getSCMAddressForDatanodes(conf);

    assertNotNull(scmAddressList);
    assertEquals(3, scmAddressList.size());

    for (InetSocketAddress next : scmAddressList) {
      expected.remove(next.getHostName() + ":" + next.getPort());
    }

    assertEquals(0, expected.size());

  }

}
