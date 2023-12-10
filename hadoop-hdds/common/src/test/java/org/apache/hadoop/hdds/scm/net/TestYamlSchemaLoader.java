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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** Test the node schema loader. */
@Timeout(30)
public class TestYamlSchemaLoader {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestYamlSchemaLoader.class);
  private final ClassLoader classLoader =
      Thread.currentThread().getContextClassLoader();

  public static Stream<Arguments> getSchemaFiles() {
    return Stream.of(
        arguments("multiple-root.yaml", "Multiple root"),
        arguments("middle-leaf.yaml", "Leaf node in the middle")
    );
  }

  @ParameterizedTest
  @MethodSource("getSchemaFiles")
  public void loadSchemaFromFile(String schemaFile, String errMsg) {
    String filePath = classLoader.getResource(
        "./networkTopologyTestFiles/" + schemaFile).getPath();
    Throwable e = assertThrows(IllegalArgumentException.class, () ->
        NodeSchemaLoader.getInstance().loadSchemaFromFile(filePath));
    assertTrue(e.getMessage().contains(errMsg));
  }

  @Test
  public void testGood() {
    String filePath = classLoader.getResource(
        "./networkTopologyTestFiles/good.yaml").getPath();
    assertDoesNotThrow(() ->
        NodeSchemaLoader.getInstance().loadSchemaFromFile(filePath));
  }

  @Test
  public void testNotExist() {
    String filePath = classLoader.getResource(
        "./networkTopologyTestFiles/good.yaml").getPath() + ".backup";
    Throwable e = assertThrows(FileNotFoundException.class, () ->
        NodeSchemaLoader.getInstance().loadSchemaFromFile(filePath));
    assertTrue(e.getMessage().contains("not found"));
  }

  @Test
  public void testDefaultYaml() {
    String filePath = classLoader.getResource(
        "network-topology-default.yaml").getPath();
    NodeSchemaLoader.NodeSchemaLoadResult result =
        assertDoesNotThrow(() ->
            NodeSchemaLoader.getInstance().loadSchemaFromFile(filePath));
    assertEquals(3, result.getSchemaList().size());
  }
}
