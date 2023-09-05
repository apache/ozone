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
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

import static org.apache.hadoop.hdds.HddsUtils.getSCMAddressForDatanodes;
import static org.apache.hadoop.hdds.HddsUtils.processForLogging;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Testing HddsUtils.
 */
public class TestHddsUtils {

  private static final String REDACTED_TEXT = "<redacted>";
  private static final String ORIGINAL_VALUE = "Hello, World!";
  private static final String SENSITIVE_CONFIG_KEYS =
          CommonConfigurationKeysPublic.HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS;

  @Test
  public void testGetHostName() {
    assertEquals(Optional.of("localhost"),
        HddsUtils.getHostName("localhost:1234"));

    assertEquals(Optional.of("localhost"),
        HddsUtils.getHostName("localhost"));

    assertEquals(Optional.empty(),
        HddsUtils.getHostName(":1234"));
  }

  @Test
  public void validatePath() {
    HddsUtils.validatePath(Paths.get("/"), Paths.get("/"));
    HddsUtils.validatePath(Paths.get("/a"), Paths.get("/"));
    HddsUtils.validatePath(Paths.get("/a"), Paths.get("/a"));
    HddsUtils.validatePath(Paths.get("/a/b"), Paths.get("/a"));
    HddsUtils.validatePath(Paths.get("/a/b/c"), Paths.get("/a"));
    HddsUtils.validatePath(Paths.get("/a/../a/b"), Paths.get("/a"));

    assertThrows(IllegalArgumentException.class,
        () -> HddsUtils.validatePath(Paths.get("/b/c"), Paths.get("/a")));
    assertThrows(IllegalArgumentException.class,
        () -> HddsUtils.validatePath(Paths.get("/"), Paths.get("/a")));
    assertThrows(IllegalArgumentException.class,
        () -> HddsUtils.validatePath(Paths.get("/a/.."), Paths.get("/a")));
    assertThrows(IllegalArgumentException.class,
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
    assertTrue(expected1.isEmpty());

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
    assertTrue(expected2.isEmpty());

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

    Assertions.assertNotNull(scmAddressList);
    assertEquals(3, scmAddressList.size());

    for (InetSocketAddress next : scmAddressList) {
      expected.remove(next.getHostName() + ":" + next.getPort());
    }

    assertEquals(0, expected.size());

  }

  @Test
  public void testGetNumberFromConfigKeys() {
    final String testnum1 = "8";
    final String testnum2 = "7";
    final String serviceId = "id1";
    final String nodeId = "scm1";

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT,
        testnum1);
    assertEquals(Integer.parseInt(testnum1),
        HddsUtils.getNumberFromConfigKeys(conf,
            OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT).orElse(0));

    /* Test to return first unempty key number from list */
    /* first key is absent */
    assertEquals(Integer.parseInt(testnum1),
        HddsUtils.getNumberFromConfigKeys(conf,
            ConfUtils.addKeySuffixes(OZONE_SCM_DATANODE_PORT_KEY,
                serviceId, nodeId),
            OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT).orElse(0));

    /* now set the empty key and ensure returned value from this key */
    conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_DATANODE_PORT_KEY,
            serviceId, nodeId),
        testnum2);
    assertEquals(Integer.parseInt(testnum2),
        HddsUtils.getNumberFromConfigKeys(conf,
            ConfUtils.addKeySuffixes(OZONE_SCM_DATANODE_PORT_KEY,
                serviceId, nodeId),
            OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT).orElse(0));
  }

  @Test
  public void testRedactSensitivePropsForLogging() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(SENSITIVE_CONFIG_KEYS, String.join("\n",
            "password$",
            "key$"));
    /* Sensitive properties */
    conf.set("ozone.test.password", ORIGINAL_VALUE);
    conf.set("hdds.test.secret.key", ORIGINAL_VALUE);
    /* Non-Sensitive properties */
    conf.set("ozone.normal.config", ORIGINAL_VALUE);
    Map<String, String> processedConf = processForLogging(conf);

    /* Verify that sensitive properties are redacted */
    assertEquals(processedConf.get("ozone.test.password"), REDACTED_TEXT);
    assertEquals(processedConf.get("hdds.test.secret.key"), REDACTED_TEXT);
    /* Verify that non-sensitive properties retain their value */
    assertEquals(processedConf.get("ozone.normal.config"), ORIGINAL_VALUE);
  }
}
