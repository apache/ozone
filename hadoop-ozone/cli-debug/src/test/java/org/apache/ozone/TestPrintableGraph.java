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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ozone.rocksdiff.FlushLinkedList;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * This class is used for testing the PrintableGraph class.
 * It contains methods to test the generation and printing of graphs with different types.
 */
public class TestPrintableGraph {
  @TempDir
  private Path dir;

  @ParameterizedTest
  @EnumSource(PrintableGraph.GraphType.class)
  void testPrintNoGraphMessage(PrintableGraph.GraphType graphType) {
    FlushLinkedList flushLinkedList = new FlushLinkedList();
    PrintableGraph graph = new PrintableGraph(flushLinkedList, graphType);
    try {
      graph.generateImage(dir.resolve(graphType.name()).toString());
    } catch (IOException e) {
      assertEquals("Graph is empty.", e.getMessage());
    }
  }

  @ParameterizedTest
  @EnumSource(PrintableGraph.GraphType.class)
  void testPrintActualGraph(PrintableGraph.GraphType graphType) throws IOException {
    FlushLinkedList flushLinkedList = new FlushLinkedList();

    // Add flush nodes in time order
    flushLinkedList.addFlush("000001", 100, 1000L,
        "startKey1", "endKey1", "keyTable");
    flushLinkedList.addFlush("000002", 200, 2000L,
        "startKey2", "endKey2", "directoryTable");
    flushLinkedList.addFlush("000003", 300, 3000L,
        "startKey3", "endKey3", "fileTable");
    flushLinkedList.addFlush("000004", 400, 4000L,
        "startKey4", "endKey4", "keyTable");

    PrintableGraph graph = new PrintableGraph(flushLinkedList, graphType);
    graph.generateImage(dir.resolve(graphType.name()).toString());

    assertTrue(Files.exists(dir.resolve(graphType.name())), "Graph hasn't been generated");
  }
}
