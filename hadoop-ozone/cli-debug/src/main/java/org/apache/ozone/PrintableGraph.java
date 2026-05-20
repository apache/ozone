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

import com.google.common.graph.EndpointPair;
import com.google.common.graph.MutableGraph;
import com.mxgraph.layout.hierarchical.mxHierarchicalLayout;
import com.mxgraph.layout.mxIGraphLayout;
import com.mxgraph.util.mxCellRenderer;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import javax.imageio.ImageIO;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.ozone.rocksdiff.CompactionNode;
import org.jgrapht.Graph;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.DefaultDirectedGraph;

/**
 * Wrapped over {@link Graph} to get an image of {@link MutableGraph}.
 */
public class PrintableGraph {

  private final Graph<String, Edge> graph;

  public PrintableGraph(MutableGraph<CompactionNode> guavaGraph,
                        GraphType graphType) {
    this.graph = getGraph(guavaGraph, graphType);
  }

  public void generateImage(String fileName) throws IOException {
    if (CollectionUtils.isEmpty(graph.vertexSet())) {
      throw new IOException("Graph is empty.");
    }

    JGraphXAdapter<String, Edge> jGraphXAdapter = new JGraphXAdapter<>(graph);
    mxIGraphLayout mxIGraphLayout =
        new mxHierarchicalLayout(jGraphXAdapter);
    mxIGraphLayout.execute(jGraphXAdapter.getDefaultParent());

    BufferedImage bufferedImage =
        mxCellRenderer.createBufferedImage(jGraphXAdapter, null, 2,
            Color.WHITE, true, null);

    File newFIle = new File(fileName);
    ImageIO.write(bufferedImage, "PNG", newFIle);
  }

  /**
   * Convert guava's {@link MutableGraph} to jgrapht's {@link Graph}.
   */
  public Graph<String, Edge> getGraph(
      MutableGraph<CompactionNode> guavaGraph,
      GraphType graphType
  ) {

    Graph<String, Edge> jgrapht =
        new DefaultDirectedGraph<>(Edge.class);

    for (CompactionNode node : guavaGraph.nodes()) {
      jgrapht.addVertex(getVertex(node, graphType));
    }

    for (EndpointPair<CompactionNode> edge : guavaGraph.edges()) {
      jgrapht.addEdge(getVertex(edge.source(), graphType),
          getVertex(edge.target(), graphType));
    }

    return jgrapht;
  }

  private String getVertex(CompactionNode node, GraphType graphType) {
    switch (graphType) {
    case KEY_SIZE:
      return
          node.getFileName() + "::" + node.getTotalNumberOfKeys();
    case CUMULATIVE_SIZE:
      return
          node.getFileName() + "::" + node.getCumulativeKeysReverseTraversal();
    case FILE_NAME:
    default:
      return node.getFileName();
    }
  }

  /**
   * Enum to print different type of node's name in the graph image.
   */
  public enum GraphType {
    /**
     * To use SST file name as node name.
     */
    FILE_NAME,

    /**
     * To use SST file name and total key in the file as node name.
     */
    KEY_SIZE,

    /**
     * To use SST file name and cumulative key as node name.
     */
    CUMULATIVE_SIZE
  }
}
