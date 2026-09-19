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

package org.apache.hadoop.hdds;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_HOST_NAME_KEY;
import static org.apache.hadoop.hdds.HddsUtils.processForLogging;
import static org.apache.hadoop.hdds.HddsUtils.validateAdvertisedAddress;
import static org.apache.hadoop.hdds.HddsUtils.validateAdvertisedHost;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Testing HddsUtils.
 */
public class TestHddsUtils {

  private static final String REDACTED_TEXT = "<redacted>";
  private static final String ORIGINAL_VALUE = "Hello, World!";
  private static final String SENSITIVE_CONFIG_KEYS =
          CommonConfigurationKeysPublic.HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS;

  @Test
  void testGetHostName() {
    assertEquals(Optional.of("localhost"),
        HddsUtils.getHostName("localhost:1234"));

    assertEquals(Optional.of("localhost"),
        HddsUtils.getHostName("localhost"));

    assertEquals(Optional.empty(),
        HddsUtils.getHostName(":1234"));

    assertEquals(Optional.of("::1"),
        HddsUtils.getHostName("[::1]:9862"));

    assertEquals(Optional.of("::1"),
        HddsUtils.getHostName("::1"));

    assertEquals(Optional.of("2001:db8::1"),
        HddsUtils.getHostName("2001:db8::1"));

    assertEquals(Optional.of("2001:db8::1"),
        HddsUtils.getHostName("[2001:db8::1]:9862"));

    assertEquals(Optional.of("2001:db8::1"),
        HddsUtils.getHostName("[2001:db8::1]"));

    // Malformed host:port input is rejected, matching getHostPort().
    assertThrows(IllegalArgumentException.class,
        () -> HddsUtils.getHostName("a:b"));
  }

  @Test
  void testGetHostPort() {
    assertEquals(OptionalInt.of(9876), HddsUtils.getHostPort("0.0.0.0:9876"));
    assertEquals(OptionalInt.of(9862), HddsUtils.getHostPort("localhost:9862"));
    assertEquals(OptionalInt.of(9862), HddsUtils.getHostPort("[2001:db8::1]:9862"));
    assertEquals(OptionalInt.empty(), HddsUtils.getHostPort("localhost"));
  }

  @Test
  void testGetHostPortString() {
    // Hostnames and IPv4 literals are joined with a plain colon.
    assertEquals("host1:9858", HddsUtils.getHostPortString("host1", 9858));
    assertEquals("1.2.3.4:9858", HddsUtils.getHostPortString("1.2.3.4", 9858));

    // Bare IPv6 literals must be bracketed so the result is an unambiguous
    // Ratis/gRPC target.
    assertEquals("[2001:db8::1]:9858", HddsUtils.getHostPortString("2001:db8::1", 9858));
    assertEquals("[::1]:9858", HddsUtils.getHostPortString("::1", 9858));

    // Already-bracketed IPv6 literals keep a single pair of brackets.
    assertEquals("[2001:db8::1]:9858", HddsUtils.getHostPortString("[2001:db8::1]", 9858));
  }

  @Test
  void testParseRatisRoleStringIPv4() {
    String input = "hostname1:9894:LEADER:peer-uuid-123:192.168.1.1";
    String[] result = HddsUtils.parseRatisRoleString(input);
    assertEquals("hostname1", result[0]);
    assertEquals("9894", result[1]);
    assertEquals("LEADER", result[2]);
    assertEquals("peer-uuid-123", result[3]);
    assertEquals("192.168.1.1", result[4]);
  }

  @Test
  void testParseRatisRoleStringIPv6() {
    String input = "[2001:db8::1]:9894:LEADER:peer1:[2001:db8:0:0:0:0:0:1]";
    String[] result = HddsUtils.parseRatisRoleString(input);
    assertEquals("2001:db8::1", result[0]);
    assertEquals("9894", result[1]);
    assertEquals("LEADER", result[2]);
    assertEquals("peer1", result[3]);
    assertEquals("2001:db8:0:0:0:0:0:1", result[4]);
  }

  @Test
  void testParseRatisRoleStringIPv6Follower() {
    String input = "[::1]:9894:FOLLOWER:abc-def:[0:0:0:0:0:0:0:1]";
    String[] result = HddsUtils.parseRatisRoleString(input);
    assertEquals("::1", result[0]);
    assertEquals("9894", result[1]);
    assertEquals("FOLLOWER", result[2]);
    assertEquals("abc-def", result[3]);
    assertEquals("0:0:0:0:0:0:0:1", result[4]);
  }

  @Test
  void testParseRatisRoleStringEmptyHostIp() {
    String input = "scm-host:9894:LEADER:uuid123:";
    String[] result = HddsUtils.parseRatisRoleString(input);
    assertEquals("scm-host", result[0]);
    assertEquals("9894", result[1]);
    assertEquals("LEADER", result[2]);
    assertEquals("uuid123", result[3]);
    assertEquals("", result[4]);
  }

  @Test
  void testParseRatisRoleStringRejectsNull() {
    assertThrows(IllegalArgumentException.class,
        () -> HddsUtils.parseRatisRoleString(null));
  }

  @Test
  void testParseRatisRoleStringRejectsEmpty() {
    assertThrows(IllegalArgumentException.class,
        () -> HddsUtils.parseRatisRoleString(""));
  }

  @Test
  void testParseRatisRoleStringRejectsTooFewFields() {
    assertThrows(IllegalArgumentException.class,
        () -> HddsUtils.parseRatisRoleString("host:9894"));
  }

  static List<Arguments> validPaths() {
    return Arrays.asList(
        Arguments.of("/", "/"),
        Arguments.of("/a", "/"),
        Arguments.of("/a", "/a"),
        Arguments.of("/a/b", "/a"),
        Arguments.of("/a/b/c", "/a"),
        Arguments.of("/a/../a/b", "/a")
    );
  }

  @ParameterizedTest
  @MethodSource("validPaths")
  void validatePathAcceptsValidPath(String path, String ancestor) {
    HddsUtils.validatePath(Paths.get(path), Paths.get(ancestor));
  }

  static List<Arguments> invalidPaths() {
    return Arrays.asList(
        Arguments.of("/b/c", "/a"),
        Arguments.of("/", "/a"),
        Arguments.of("/a/..", "/a"),
        Arguments.of("/a/../b", "/a")
    );
  }

  @ParameterizedTest
  @MethodSource("invalidPaths")
  void validatePathRejectsInvalidPath(String path, String ancestor) {
    assertThrows(IllegalArgumentException.class,
        () -> HddsUtils.validatePath(Paths.get(path), Paths.get(ancestor)));
  }

  @Test
  void testGetNumberFromConfigKeys() {
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
  void testRedactSensitivePropsForLogging() {
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

  @ParameterizedTest
  @MethodSource("ambiguousIPv6Authorities")
  void getPortNumberFromConfigKeysRejectsAmbiguousIPv6Authority(String value, String host, String port) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, value);

    ConfigurationException e = assertThrows(ConfigurationException.class,
        () -> HddsUtils.getPortNumberFromConfigKeys(conf, OZONE_SCM_CLIENT_ADDRESS_KEY));

    // Both readings have to be spelled out, since the configured text does not
    // say which one was meant.
    assertThat(e.getMessage())
        .contains(OZONE_SCM_CLIENT_ADDRESS_KEY)
        .contains(value)
        .contains("[" + host + "]:" + port)
        .contains("[" + value + "]");
  }

  static List<Arguments> ambiguousIPv6Authorities() {
    return Arrays.asList(
        Arguments.of("2001:db8::1:9862", "2001:db8::1", "9862"),
        Arguments.of("fd00::1:2", "fd00::1", "2")
    );
  }

  @ParameterizedTest
  @ValueSource(strings = {"::1", "2001:db8::1", "2001:db8::1:abcd", "fe80::1%eth0"})
  void getPortNumberFromConfigKeysRejectsBareIPv6Literal(String value) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, value);

    ConfigurationException e = assertThrows(ConfigurationException.class,
        () -> HddsUtils.getPortNumberFromConfigKeys(conf, OZONE_SCM_CLIENT_ADDRESS_KEY));

    assertThat(e.getMessage())
        .contains(OZONE_SCM_CLIENT_ADDRESS_KEY)
        .contains("[" + value + "]");
  }

  @ParameterizedTest
  @MethodSource("unambiguousAuthorities")
  void getPortNumberFromConfigKeysAcceptsUnambiguousAuthority(String value, OptionalInt expectedPort) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, value);

    assertEquals(expectedPort,
        HddsUtils.getPortNumberFromConfigKeys(conf, OZONE_SCM_CLIENT_ADDRESS_KEY));
  }

  static List<Arguments> unambiguousAuthorities() {
    return Arrays.asList(
        Arguments.of("[2001:db8::1]:9862", OptionalInt.of(9862)),
        Arguments.of("[2001:db8::1:9862]:9862", OptionalInt.of(9862)),
        Arguments.of("[2001:db8::1]", OptionalInt.empty()),
        Arguments.of("192.0.2.1:9862", OptionalInt.of(9862)),
        Arguments.of("scm1.example.com:9862", OptionalInt.of(9862)),
        Arguments.of("scm1.example.com", OptionalInt.empty())
    );
  }

  /**
   * A bind host names no port, so the bracket rule must leave it alone: an
   * explicit IPv6 listener is configured as a bare {@code ::}.
   */
  @Test
  void getHostNameFromConfigKeysAcceptsBareIPv6BindHost() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_BIND_HOST_KEY, "::");

    assertEquals(Optional.of("::"),
        HddsUtils.getHostNameFromConfigKeys(conf, OZONE_SCM_CLIENT_BIND_HOST_KEY));
  }

  @ParameterizedTest
  @ValueSource(strings = {"scm1.example.com", "192.0.2.1", "2001:db8::1", "localhost", "127.0.0.1", "::1"})
  void validateAdvertisedHostAcceptsReachableHost(String host) {
    assertDoesNotThrow(() -> validateAdvertisedHost(OZONE_SCM_CLIENT_ADDRESS_KEY, host));
  }

  @ParameterizedTest
  @ValueSource(strings = {"0.0.0.0", "::", "169.254.1.1", "fe80::1", "fe80::1%eth0", "2001:db8::1/64"})
  void validateAdvertisedHostRejectsUnadvertisableHost(String host) {
    ConfigurationException e = assertThrows(ConfigurationException.class,
        () -> validateAdvertisedHost(OZONE_SCM_CLIENT_ADDRESS_KEY, host));

    assertThat(e.getMessage())
        .contains(OZONE_SCM_CLIENT_ADDRESS_KEY)
        .contains(host);
  }

  @ParameterizedTest
  @ValueSource(strings = {"0.0.0.0:9860", "[::]:9860", "[fe80::1%eth0]:9860"})
  void validateAdvertisedAddressRejectsUnadvertisableHostWithPort(String value) {
    assertThrows(ConfigurationException.class,
        () -> validateAdvertisedAddress(OZONE_SCM_CLIENT_ADDRESS_KEY, value));
  }

  @Test
  void getHostNameRejectsUnadvertisableDatanodeHostname() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_HOST_NAME_KEY, "0.0.0.0");

    ConfigurationException e = assertThrows(ConfigurationException.class,
        () -> HddsUtils.getHostName(conf));

    assertThat(e.getMessage()).contains(HDDS_DATANODE_HOST_NAME_KEY);
  }

  @Test
  void getHostNameAcceptsConfiguredDatanodeHostname() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_HOST_NAME_KEY, "dn1.example.com");

    assertEquals("dn1.example.com", HddsUtils.getHostName(conf));
  }

  @Test
  void getScmAddressForClientsRejectsUnadvertisableClientAddress() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "0.0.0.0:9860");

    ConfigurationException e = assertThrows(ConfigurationException.class,
        () -> HddsUtils.getScmAddressForClients(conf));

    assertThat(e.getMessage()).contains(OZONE_SCM_CLIENT_ADDRESS_KEY);
  }

  @Test
  void getScmAddressForClientsRejectsUnadvertisableScmName() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_NAMES, "0.0.0.0");

    ConfigurationException e = assertThrows(ConfigurationException.class,
        () -> HddsUtils.getScmAddressForClients(conf));

    assertThat(e.getMessage()).contains(OZONE_SCM_NAMES);
  }
}
