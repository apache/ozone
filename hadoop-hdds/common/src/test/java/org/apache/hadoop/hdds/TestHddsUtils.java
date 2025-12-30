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

import static org.apache.hadoop.hdds.HddsUtils.processForLogging;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
}
