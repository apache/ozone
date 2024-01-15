/**
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

package org.apache.hadoop.hdds.scm.client;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ha.ConfUtils;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This test class verifies the parsing of SCM endpoint config settings. The
 * parsing logic is in
 * {@link org.apache.hadoop.hdds.scm.client.HddsClientUtils}.
 */
@Timeout(300)
public class TestHddsClientUtils {

  /**
   * Verify client endpoint lookup failure if it is not configured.
   */
  @Test
  public void testMissingScmClientAddress() {
    final OzoneConfiguration conf = new OzoneConfiguration();
    assertThrows(ConfigurationException.class,
        () -> HddsUtils.getScmAddressForClients(conf));
  }

  /**
   * Verify that the client endpoint can be correctly parsed from
   * configuration.
   */
  @Test
  public void testGetScmClientAddress() {
    final OzoneConfiguration conf = new OzoneConfiguration();

    // First try a client address with just a host name. Verify it falls
    // back to the default port.
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4");
    checkAddr(conf, "1.2.3.4", OZONE_SCM_CLIENT_PORT_DEFAULT);

    // Next try a client address with a host name and port. Verify both
    // are used correctly.
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    checkAddr(conf, "1.2.3.4", 100);

  }

  @Test
  public void testGetScmClientAddressForHA() {
    OzoneConfiguration conf = new OzoneConfiguration();
    String scmServiceId = "scmservice";
    conf.set(ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY, scmServiceId);

    String[] nodes = new String[] {"scm1", "scm2", "scm3"};
    conf.set(ScmConfigKeys.OZONE_SCM_NODES_KEY + "." + scmServiceId,
        "scm1,scm2,scm3");
    conf.set(ScmConfigKeys.OZONE_SCM_NODE_ID_KEY, "scm1");

    int port = 9880;
    int i = 1;
    for (String nodeId : nodes) {
      conf.setInt(ConfUtils.addKeySuffixes(OZONE_SCM_CLIENT_PORT_KEY,
          scmServiceId, nodeId), port);
      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_ADDRESS_KEY,
          scmServiceId, nodeId), "localhost");
    }

    Collection<InetSocketAddress> scmClientAddr =
        HddsUtils.getScmAddressForClients(conf);

    port = 9880;

    for (InetSocketAddress scmAddr : scmClientAddr) {
      assertEquals(scmAddr.getHostName(), "localhost");
      assertEquals(scmAddr.getPort(), port++);
    }

  }

  private void checkAddr(OzoneConfiguration conf, String address, int port) {
    Iterator<InetSocketAddress> scmAddrIterator =
        HddsUtils.getScmAddressForClients(conf).iterator();
    assertTrue(scmAddrIterator.hasNext());
    InetSocketAddress scmAddr = scmAddrIterator.next();
    assertEquals(address, scmAddr.getHostString());
    assertEquals(port, scmAddr.getPort());
  }

  @Test
  public void testBlockClientFallbackToClientNoPort() {
    // When OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY is undefined it should
    // fallback to OZONE_SCM_CLIENT_ADDRESS_KEY.
    final String scmHost = "host123";
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, scmHost);
    final InetSocketAddress address = NetUtils.createSocketAddr(
        SCMNodeInfo.buildNodeInfo(conf).get(0).getBlockClientAddress());
    assertEquals(scmHost, address.getHostName());
    assertEquals(OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT, address.getPort());
  }

  @Test
  public void testClientFallbackToScmNamesNoPort() {
    // When OZONE_SCM_CLIENT_ADDRESS_KEY is undefined, it should fallback
    // to OZONE_SCM_NAMES.
    final String scmHost = "host456";
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_NAMES, scmHost);
    final Collection<InetSocketAddress> address =
        HddsUtils.getScmAddressForClients(conf);
    assertTrue(address.iterator().hasNext());
    InetSocketAddress socketAddress = address.iterator().next();
    assertEquals(scmHost, socketAddress.getHostName());
    assertEquals(OZONE_SCM_CLIENT_PORT_DEFAULT, socketAddress.getPort());
  }

  @Test
  @SuppressWarnings("StringSplitter")
  public void testClientFallbackToScmNamesWithPort() {
    // When OZONE_SCM_CLIENT_ADDRESS_KEY is undefined, it should fallback
    // to OZONE_SCM_NAMES.
    //
    // Verify that the OZONE_SCM_NAMES port number is ignored, if present.
    // Instead we should use OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT.
    final String scmHost = "host456:300";
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_NAMES, scmHost);
    final Collection<InetSocketAddress> address =
        HddsUtils.getScmAddressForClients(conf);
    assertTrue(address.iterator().hasNext());
    InetSocketAddress socketAddress = address.iterator().next();
    assertEquals(scmHost.split(":")[0],
        socketAddress.getHostName());
    assertEquals(OZONE_SCM_CLIENT_PORT_DEFAULT, socketAddress.getPort());
  }

  @Test
  @SuppressWarnings("StringSplitter")
  public void testBlockClientFallbackToClientWithPort() {
    // When OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY is undefined it should
    // fallback to OZONE_SCM_CLIENT_ADDRESS_KEY.
    //
    // Verify that the OZONE_SCM_CLIENT_ADDRESS_KEY port number is ignored,
    // if present. Instead we should use OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT.
    final String scmHost = "host123:100";
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, scmHost);
    final InetSocketAddress address = NetUtils.createSocketAddr(
        SCMNodeInfo.buildNodeInfo(conf).get(0).getBlockClientAddress());
    assertEquals(scmHost.split(":")[0], address.getHostName());
    assertEquals(OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT, address.getPort());
  }

  @Test
  public void testVerifyResourceName() {
    final String validName = "my-bucket.01";
    HddsClientUtils.verifyResourceName(validName);

    final String shortestValidName = StringUtils.repeat("a",
        OzoneConsts.OZONE_MIN_BUCKET_NAME_LENGTH);
    HddsClientUtils.verifyResourceName(shortestValidName);

    // various kinds of invalid names
    final String ipaddr = "192.68.1.1";
    final String dotDash = "not.-a-name";
    final String dashDot = "not-a-.name";
    final String dotDot = "not..a-name";
    final String upperCase = "notAname";
    final String endDot = "notaname.";
    final String startDot = ".notaname";
    final String unicodeCharacters = "ｚｚｚ";
    final String tooShort = StringUtils.repeat("a",
        OzoneConsts.OZONE_MIN_BUCKET_NAME_LENGTH - 1);

    List<String> invalidNames = new ArrayList<>();
    invalidNames.add(ipaddr);
    invalidNames.add(dotDash);
    invalidNames.add(dashDot);
    invalidNames.add(dotDot);
    invalidNames.add(upperCase);
    invalidNames.add(endDot);
    invalidNames.add(startDot);
    invalidNames.add(unicodeCharacters);
    invalidNames.add(tooShort);

    for (String name : invalidNames) {
      try {
        HddsClientUtils.verifyResourceName(name);
        fail("Did not reject invalid string [" + name + "] as a name");
      } catch (IllegalArgumentException e) {
        // throwing up on an invalid name. we're good
      }
    }
  }

  @Test
  public void testVerifyKeyName() {
    List<String> invalidNames = new ArrayList<>();
    invalidNames.add("#");
    invalidNames.add("ab^cd");
    invalidNames.add("test|name~");
    invalidNames.add("~hi!ozone");
    invalidNames.add("test<string>");
    invalidNames.add("10%3=1");
    invalidNames.add("photo[0201]");
    invalidNames.add("square_right]");
    invalidNames.add("my\\file");
    invalidNames.add("for}");
    invalidNames.add("{curly-left");
    invalidNames.add("\"hi\"");
    invalidNames.add("\\\\~`");
    invalidNames.add("Code`");


    for (String name : invalidNames) {
      try {
        HddsClientUtils.verifyKeyName(name);
        fail("Did not reject invalid string [" + name + "] as a name");
      } catch (IllegalArgumentException e) {
        // throwing up on an invalid name. it's working.
      }
    }

    List<String> validNames = new ArrayList<>();
    validNames.add("123_123");
    validNames.add("abcd/abcd");
    validNames.add("test-name");
    validNames.add("hi!ozone");
    validNames.add("test(string)");
    validNames.add("10*3+1");
    validNames.add("photo'0201'");
    validNames.add("my.name");
    validNames.add("you&me");
    validNames.add("1=0");
    validNames.add("print;");
    validNames.add("3:5:2");
    validNames.add("a,b,c");
    validNames.add("my name is");
    validNames.add("xyz@mail");
    validNames.add("dollar$");

    for (String name : validNames) {
      try {
        HddsClientUtils.verifyKeyName(name);
        // not throwing up on a valid name. it's working.
      } catch (IllegalArgumentException e) {
        // throwing up on an valid name. it's not working.
        fail("Rejected valid string [" + name + "] as a name");
      }
    }
  }

  @Test
  void testContainsException() {
    Exception ex1 = new ConnectException();
    Exception ex2 = new IOException(ex1);
    Exception ex3 = new IllegalArgumentException(ex2);

    assertSame(ex1,
        HddsClientUtils.containsException(ex3, ConnectException.class));
    assertSame(ex2,
        HddsClientUtils.containsException(ex3, IOException.class));
    assertSame(ex3,
        HddsClientUtils.containsException(ex3, IllegalArgumentException.class));
    assertNull(
        HddsClientUtils.containsException(ex3, IllegalStateException.class));
  }
}
