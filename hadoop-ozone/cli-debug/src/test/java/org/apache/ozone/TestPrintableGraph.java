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

package org.apache.ozone.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.google.common.graph.MutableGraph;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ozone.rocksdiff.CompactionNode;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * This class is used for testing the PrintableGraph class.
 * It contains methods to test the generation and printing of graphs with different types.
 */
@ExtendWith(MockitoExtension.class)
public class TestPrintableGraph {
  @TempDir
  private Path dir;

  @Mock
  private MutableGraph<CompactionNode> mutableGraph;

  @ParameterizedTest
  @EnumSource(PrintableGraph.GraphType.class)
  void testPrintNoGraphMessage(PrintableGraph.GraphType graphType) {
    PrintableGraph graph = new PrintableGraph(mutableGraph, graphType);
    try {
      graph.generateImage(dir.resolve(graphType.name()).toString());
    } catch (IOException e) {
      assertEquals("Graph is empty.", e.getMessage());
    }
  }

  @ParameterizedTest
  @EnumSource(PrintableGraph.GraphType.class)
  void testPrintActualGraph(PrintableGraph.GraphType graphType) throws IOException {
    Set<CompactionNode> nodes = Stream.of(
        new CompactionNode("fileName1", 100, "startKey1", "endKey1", "columnFamily1"),
        new CompactionNode("fileName2", 200, "startKey2", "endKey2", null),
        new CompactionNode("fileName3", 300, null, "endKey3", "columnFamily3"),
        new CompactionNode("fileName4", 400, "startKey4", null, "columnFamily4")
    ).collect(Collectors.toSet());
    when(mutableGraph.nodes()).thenReturn(nodes);

    PrintableGraph graph = new PrintableGraph(mutableGraph, graphType);
    graph.generateImage(dir.resolve(graphType.name()).toString());

    assertTrue(Files.exists(dir.resolve(graphType.name())), "Graph hasn't been generated");
  }
}
