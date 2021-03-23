/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.net;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test the node schema loader. */
@RunWith(Enclosed.class)
public class TestNodeSchemaLoader {

  /**
   * Parameterized test cases for various error conditions.
   */
  @RunWith(Parameterized.class)
  public static class ParameterizedTests {

    private final String schemaFile;
    private final String errMsg;

    @Rule
    public Timeout testTimeout = Timeout.seconds(2);

    @Parameters
    public static Collection<Object[]> getSchemaFiles() {
      Object[][] schemaFiles = new Object[][]{
          {"enforce-error.xml", "layer without prefix defined"},
          {"invalid-cost.xml", "Cost should be positive number or 0"},
          {"multiple-leaf.xml", "Multiple LEAF layers are found"},
          {"multiple-root.xml", "Multiple ROOT layers are found"},
          {"no-leaf.xml", "No LEAF layer is found"},
          {"no-root.xml", "No ROOT layer is found"},
          {"path-layers-size-mismatch.xml",
              "Topology path depth doesn't match layer element numbers"},
          {"path-with-id-reference-failure.xml",
              "No layer found for id"},
          {"unknown-layer-type.xml", "Unsupported layer type"},
          {"wrong-path-order-1.xml",
              "Topology path doesn't start with ROOT layer"},
          {"wrong-path-order-2.xml",
              "Topology path doesn't end with LEAF layer"},
          {"no-topology.xml", "no or multiple <topology> element"},
          {"multiple-topology.xml", "no or multiple <topology> element"},
          {"invalid-version.xml", "Bad layoutversion value"},
          {"external-entity.xml", "accessExternalDTD"},
      };
      return Arrays.asList(schemaFiles);
    }

    public ParameterizedTests(String schemaFile, String errMsg) {
      this.schemaFile = schemaFile;
      this.errMsg = errMsg;
    }

    @Test
    public void testInvalid() {
      String filePath = getClassloaderResourcePath(schemaFile);
      Exception e = assertThrows(IllegalArgumentException.class,
          () -> NodeSchemaLoader.getInstance().loadSchemaFromFile(filePath));
      assertMessageContains(e.getMessage(), errMsg, schemaFile);
    }
  }

  /**
   * Test cases that do not use the parameters, should be executed only once.
   */
  public static class NonParameterizedTests {

    private static final String VALID_SCHEMA_FILE = "good.xml";

    @Rule
    public Timeout testTimeout = Timeout.seconds(2);

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
