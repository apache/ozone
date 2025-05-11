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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.FileNotFoundException;
import java.net.URL;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Test the node schema loader. */
public class TestNodeSchemaLoader {

  /**
   * Test cases that do not use the parameters should be executed only once.
   */
  private static final String VALID_SCHEMA_FILE = "good.xml";

  /**
   * Parameterized test cases for various error conditions.
   */
  public static Stream<Arguments> getSchemaFiles() {
    return Stream.of(
        arguments("enforce-error.xml", "layer without prefix defined"),
        arguments("invalid-cost.xml", "Cost should be positive number or 0"),
        arguments("multiple-leaf.xml", "Multiple LEAF layers are found"),
        arguments("multiple-root.xml", "Multiple ROOT layers are found"),
        arguments("no-leaf.xml", "No LEAF layer is found"),
        arguments("no-root.xml", "No ROOT layer is found"),
        arguments("path-layers-size-mismatch.xml",
            "Topology path depth doesn't match layer element numbers"),
        arguments("path-with-id-reference-failure.xml",
            "No layer found for id"),
        arguments("unknown-layer-type.xml", "Unsupported layer type"),
        arguments("wrong-path-order-1.xml",
            "Topology path doesn't start with ROOT layer"),
        arguments("wrong-path-order-2.xml",
            "Topology path doesn't end with LEAF layer"),
        arguments("no-topology.xml", "no or multiple <topology> element"),
        arguments("multiple-topology.xml", "no or multiple <topology> element"),
        arguments("invalid-version.xml", "Bad layoutversion value"),
        arguments("external-entity.xml", "accessExternalDTD")
    );
  }

  @ParameterizedTest
  @MethodSource("getSchemaFiles")
  public void testInvalid(String schemaFile, String errMsg) {
    String filePath = getClassloaderResourcePath(schemaFile);
    Exception e = assertThrows(IllegalArgumentException.class,
        () -> NodeSchemaLoader.getInstance().loadSchemaFromFile(filePath));
    assertMessageContains(e.getMessage(), errMsg, schemaFile);
  }

  @Test
  public void testGood() throws Exception {
    String filePath = getClassloaderResourcePath(VALID_SCHEMA_FILE);
    NodeSchemaLoader.getInstance().loadSchemaFromFile(filePath);
  }

  @Test
  public void testNotExist() {
    String filePath = getClassloaderResourcePath(VALID_SCHEMA_FILE)
        .replace(VALID_SCHEMA_FILE, "non-existent.xml");
    Exception e = assertThrows(FileNotFoundException.class,
        () -> NodeSchemaLoader.getInstance().loadSchemaFromFile(filePath));
    assertMessageContains(e.getMessage(), "not found", "non-existent.xml");
  }

  private static void assertMessageContains(
      String actual, String expected, String testCase) {
    if (!actual.contains(expected)) {
      fail(String.format(
          "Expected message for '%s' to contain '%s', but got: '%s'",
          testCase, expected, actual));
    }
  }

  private static String getClassloaderResourcePath(String file) {
    URL resource = Thread.currentThread().getContextClassLoader()
        .getResource("networkTopologyTestFiles/" + file);
    assertNotNull(resource);
    return resource.getPath();
  }
}
