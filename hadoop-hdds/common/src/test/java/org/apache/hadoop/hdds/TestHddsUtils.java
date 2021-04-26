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
package org.apache.hadoop.hdds;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.hdds.HddsUtils.getSCMAddressForDatanodes;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_KEY;
import static org.hamcrest.core.Is.is;
import org.junit.Assert;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Testing HddsUtils.
 */
public class TestHddsUtils {

  @Test
  public void testGetHostName() {
    Assert.assertEquals(Optional.of("localhost"),
        HddsUtils.getHostName("localhost:1234"));

    Assert.assertEquals(Optional.of("localhost"),
        HddsUtils.getHostName("localhost"));

    Assert.assertEquals(Optional.empty(),
        HddsUtils.getHostName(":1234"));
  }

  @Test
  public void validatePath() throws Exception {
    HddsUtils.validatePath(Paths.get("/"), Paths.get("/"));
    HddsUtils.validatePath(Paths.get("/a"), Paths.get("/"));
    HddsUtils.validatePath(Paths.get("/a"), Paths.get("/a"));
    HddsUtils.validatePath(Paths.get("/a/b"), Paths.get("/a"));
    HddsUtils.validatePath(Paths.get("/a/b/c"), Paths.get("/a"));
    HddsUtils.validatePath(Paths.get("/a/../a/b"), Paths.get("/a"));

    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () -> HddsUtils.validatePath(Paths.get("/b/c"), Paths.get("/a")));
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () -> HddsUtils.validatePath(Paths.get("/"), Paths.get("/a")));
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () -> HddsUtils.validatePath(Paths.get("/a/.."), Paths.get("/a")));
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () -> HddsUtils.validatePath(Paths.get("/a/../b"), Paths.get("/a")));
  }

  @Test
  public void testGetSCMAddresses() {
    final OzoneConfiguration conf = new OzoneConfiguration();
    Collection<InetSocketAddress> addresses;
    InetSocketAddress addr;
    Iterator<InetSocketAddress> it;

    // Verify valid IP address setup
    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, "1.2.3.4");
    addresses = getSCMAddressForDatanodes(conf);
    assertThat(addresses.size(), is(1));
    addr = addresses.iterator().next();
    assertThat(addr.getHostName(), is("1.2.3.4"));
    assertThat(addr.getPort(), is(OZONE_SCM_DATANODE_PORT_DEFAULT));

    // Verify valid hostname setup
    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, "scm1");
    addresses = getSCMAddressForDatanodes(conf);
    assertThat(addresses.size(), is(1));
    addr = addresses.iterator().next();
    assertThat(addr.getHostName(), is("scm1"));
    assertThat(addr.getPort(), is(OZONE_SCM_DATANODE_PORT_DEFAULT));

    // Verify valid hostname and port
    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, "scm1:1234");
    addresses = getSCMAddressForDatanodes(conf);
    assertThat(addresses.size(), is(1));
    addr = addresses.iterator().next();
    assertThat(addr.getHostName(), is("scm1"));
    assertThat(addr.getPort(), is(1234));

    final Map<String, Integer> hostsAndPorts = new HashMap<>();
    hostsAndPorts.put("scm1", 1234);
    hostsAndPorts.put("scm2", 2345);
    hostsAndPorts.put("scm3", 3456);

    // Verify multiple hosts and port
    conf.setStrings(
        ScmConfigKeys.OZONE_SCM_NAMES, "scm1:1234,scm2:2345,scm3:3456");
    addresses = getSCMAddressForDatanodes(conf);
    assertThat(addresses.size(), is(3));
    it = addresses.iterator();
    HashMap<String, Integer> expected1 = new HashMap<>(hostsAndPorts);
    while(it.hasNext()) {
      InetSocketAddress current = it.next();
      assertTrue(expected1.remove(current.getHostName(),
          current.getPort()));
    }
    assertTrue(expected1.isEmpty());

    // Verify names with spaces
    conf.setStrings(
        ScmConfigKeys.OZONE_SCM_NAMES, " scm1:1234, scm2:2345 , scm3:3456 ");
    addresses = getSCMAddressForDatanodes(conf);
    assertThat(addresses.size(), is(3));
    it = addresses.iterator();
    HashMap<String, Integer> expected2 = new HashMap<>(hostsAndPorts);
    while(it.hasNext()) {
      InetSocketAddress current = it.next();
      assertTrue(expected2.remove(current.getHostName(),
          current.getPort()));
    }
    assertTrue(expected2.isEmpty());

    // Verify empty value
    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, "");
    try {
      getSCMAddressForDatanodes(conf);
      fail("Empty value should cause an IllegalArgumentException");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalArgumentException);
    }

    // Verify invalid hostname
    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, "s..x..:1234");
    try {
      getSCMAddressForDatanodes(conf);
      fail("An invalid hostname should cause an IllegalArgumentException");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalArgumentException);
    }

    // Verify invalid port
    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, "scm:xyz");
    try {
      getSCMAddressForDatanodes(conf);
      fail("An invalid port should cause an IllegalArgumentException");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalArgumentException);
    }

    // Verify a mixed case (valid and invalid value both appears)
    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES, "scm1:1234, scm:xyz");
    try {
      getSCMAddressForDatanodes(conf);
      fail("An invalid value should cause an IllegalArgumentException");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalArgumentException);
    }
  }


  @Test
  public void testGetSCMAddressesWithHAConfig() {
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
        HddsUtils.getSCMAddressForDatanodes(conf);

    Assert.assertNotNull(scmAddressList);
    Assert.assertEquals(3, scmAddressList.size());

    Iterator<InetSocketAddress> it = scmAddressList.iterator();
    while (it.hasNext()) {
      InetSocketAddress next = it.next();
      expected.remove(next.getHostName()  + ":" + next.getPort());
    }

    Assert.assertTrue(expected.size() == 0);

  }

}