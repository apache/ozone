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

import com.mxgraph.layout.hierarchical.mxHierarchicalLayout;
import com.mxgraph.layout.mxIGraphLayout;
import com.mxgraph.util.mxCellRenderer;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import javax.imageio.ImageIO;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.ozone.rocksdiff.FlushLinkedList;
import org.apache.ozone.rocksdiff.FlushNode;
import org.jgrapht.Graph;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.DefaultDirectedGraph;

/**
 * Wrapper to generate a visual graph image from {@link FlushLinkedList}.
 * The graph represents flush operations in time order, with each node
 * being an L0 SST file created by a flush operation.
 */
public class PrintableGraph {

  private static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss";

  private final Graph<String, Edge> graph;

  /**
   * Create a printable graph from a FlushLinkedList.
   *
   * @param flushLinkedList The flush linked list to visualize
   * @param graphType       How to label the nodes
   */
  public PrintableGraph(FlushLinkedList flushLinkedList, GraphType graphType) {
    this.graph = getGraphFromFlushList(flushLinkedList, graphType);
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
   * Convert a FlushLinkedList to a jgrapht Graph.
   * The graph is a simple timeline where each flush node connects
   * to the next one in chronological order.
   *
   * @param flushLinkedList The flush list to convert
   * @param graphType       How to label the nodes
   * @return A directed graph representing the flush timeline
   */
  private Graph<String, Edge> getGraphFromFlushList(
      FlushLinkedList flushLinkedList,
      GraphType graphType) {

    Graph<String, Edge> jgrapht = new DefaultDirectedGraph<>(Edge.class);

    List<FlushNode> flushNodes = flushLinkedList.getFlushNodes();

    if (flushNodes.isEmpty()) {
      return jgrapht;
    }

    // Add all vertices
    for (FlushNode node : flushNodes) {
      jgrapht.addVertex(getVertex(node, graphType));
    }

    // Add edges connecting consecutive nodes in time order
    for (int i = 0; i < flushNodes.size() - 1; i++) {
      FlushNode current = flushNodes.get(i);
      FlushNode next = flushNodes.get(i + 1);
      jgrapht.addEdge(
          getVertex(current, graphType),
          getVertex(next, graphType));
    }

    return jgrapht;
  }

  private String getVertex(FlushNode node, GraphType graphType) {
    switch (graphType) {
    case SEQUENCE_NUMBER:
      return node.getFileName() + "\nseq=" + node.getSnapshotGeneration();
    case FLUSH_TIME:
      SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_PATTERN);
      String formattedTime = dateFormat.format(new Date(node.getFlushTime()));
      return node.getFileName() + "\n" + formattedTime;
    case DETAILED:
      return String.format("%s%nseq=%d%ncf=%s",
          node.getFileName(),
          node.getSnapshotGeneration(),
          node.getColumnFamily());
    case FILE_NAME:
    default:
      return node.getFileName();
    }
  }

  /**
   * Enum to specify how to label nodes in the graph image.
   */
  public enum GraphType {
    /**
     * Show only SST file name.
     */
    FILE_NAME,

    /**
     * Show SST file name and sequence number.
     */
    SEQUENCE_NUMBER,

    /**
     * Show SST file name and flush timestamp.
     */
    FLUSH_TIME,

    /**
     * Show SST file name, sequence number, and column family.
     */
    DETAILED
  }
}
