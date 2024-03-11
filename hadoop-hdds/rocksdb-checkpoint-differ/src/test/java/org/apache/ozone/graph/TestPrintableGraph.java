package org.apache.ozone.graph;

import com.google.common.graph.MutableGraph;
import org.apache.ozone.rocksdiff.CompactionNode;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

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

  private static Stream<Arguments> graphTypes() {
    return Stream.of(
      Arguments.of(PrintableGraph.GraphType.KEY_SIZE),
      Arguments.of(PrintableGraph.GraphType.CUMULATIVE_SIZE),
      Arguments.of(PrintableGraph.GraphType.FILE_NAME)
    );
  }

  @ParameterizedTest
  @MethodSource("graphTypes")
  void testPrintNoGraphMessage(PrintableGraph.GraphType graphType) {
    PrintableGraph graph = new PrintableGraph(mutableGraph, graphType);
    try {
      graph.generateImage(dir.resolve(graphType.name()).toString());
    } catch (IOException e) {
      assertEquals("Graph is empty.", e.getMessage());
    }
  }

  @ParameterizedTest
  @MethodSource("graphTypes")
  void testPrintActualGraph(PrintableGraph.GraphType graphType) throws IOException {
    Set<CompactionNode> nodes = Stream.of(
        new CompactionNode("fileName1",
            100, 100, "startKey1", "endKey1", "columnFamily1"),
        new CompactionNode("fileName2",
        200, 200, "startKey2", "endKey2", null),
        new CompactionNode("fileName3",
        300, 300, null, "endKey3", "columnFamily3"),
        new CompactionNode("fileName4",
        400, 400, "startKey4", null, "columnFamily4")
    ).collect(Collectors.toSet());
    when(mutableGraph.nodes()).thenReturn(nodes);

    PrintableGraph graph = new PrintableGraph(mutableGraph, graphType);
    graph.generateImage(dir.resolve(graphType.name()).toString());

    assertTrue(Files.exists(dir.resolve(graphType.name())), "Graph hasn't been generated");
  }
}
