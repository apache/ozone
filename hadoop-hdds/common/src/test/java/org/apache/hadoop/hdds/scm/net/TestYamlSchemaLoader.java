/**
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

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** Test the node schema loader. */
@Timeout(30)
public class TestYamlSchemaLoader {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestYamlSchemaLoader.class);
  private final ClassLoader classLoader =
      Thread.currentThread().getContextClassLoader();

  public void loadSchemaFromFile(String schemaFile, String errMsg) {
    try {
      String filePath = classLoader.getResource(
          "./networkTopologyTestFiles/" + schemaFile).getPath();
      NodeSchemaLoader.getInstance().loadSchemaFromFile(filePath);
      fail("expect exceptions");
    } catch (Throwable e) {
      assertTrue(e.getMessage().contains(errMsg));
    }
  }

  public static Stream<Arguments> getSchemaFiles() {
    return Stream.of(
        arguments("multiple-root.yaml", "Multiple root"),
        arguments("middle-leaf.yaml", "Leaf node in the middle")
    );
  }

  @ParameterizedTest
  @MethodSource("getSchemaFiles")
  public void testGood(String schemaFile, String errMsg) {
    loadSchemaFromFile(schemaFile, errMsg);
    try {
      String filePath = classLoader.getResource(
          "./networkTopologyTestFiles/good.yaml").getPath();
      NodeSchemaLoader.getInstance().loadSchemaFromFile(filePath);
    } catch (Throwable e) {
      fail("should succeed");
    }
  }

  @ParameterizedTest
  @MethodSource("getSchemaFiles")
  public void testNotExist(String schemaFile, String errMsg) {
    loadSchemaFromFile(schemaFile, errMsg);
    String filePath = classLoader.getResource(
        "./networkTopologyTestFiles/good.yaml").getPath() + ".backup";
    try {
      NodeSchemaLoader.getInstance().loadSchemaFromFile(filePath);
      fail("should fail");
    } catch (Throwable e) {
      assertTrue(e.getMessage().contains("not found"));
    }
  }

  @ParameterizedTest
  @MethodSource("getSchemaFiles")
  public void testDefaultYaml(String schemaFile, String errMsg) {
    loadSchemaFromFile(schemaFile, errMsg);
    try {
      String filePath = classLoader.getResource(
          "network-topology-default.yaml").getPath();
      NodeSchemaLoader.NodeSchemaLoadResult result =
          NodeSchemaLoader.getInstance().loadSchemaFromFile(filePath);
      assertEquals(3, result.getSchemaList().size());
    } catch (Throwable e) {
      fail("should succeed");
    }
  }
}
