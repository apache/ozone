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

package org.apache.ozone.rocksdiff;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ozone.compaction.log.CompactionFileInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class storing DAGs of SST files for tracking compactions.
 */
public class CompactionDag {
  private static final Logger LOG = LoggerFactory.getLogger(CompactionDag.class);

  private final ConcurrentMap<String, CompactionNode> compactionNodeMap = new ConcurrentHashMap<>();
  private final MutableGraph<CompactionNode> forwardCompactionDAG = GraphBuilder.directed().build();
  private final MutableGraph<CompactionNode> backwardCompactionDAG = GraphBuilder.directed().build();

  private CompactionNode addNodeToDAG(String file, long seqNum, String startKey, String endKey, String columnFamily) {
    CompactionNode fileNode = new CompactionNode(file, seqNum, startKey, endKey, columnFamily);
    backwardCompactionDAG.addNode(fileNode);
    forwardCompactionDAG.addNode(fileNode);
    return fileNode;
  }

  /**
   * Populate the compaction DAG with input and output SST files lists.
   *
   * @param inputFiles  List of compaction input files.
   * @param outputFiles List of compaction output files.
   * @param seqNum      DB transaction sequence number.
   */
  public void populateCompactionDAG(List<CompactionFileInfo> inputFiles,
      List<CompactionFileInfo> outputFiles,
      long seqNum) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Input files: {} -> Output files: {}", inputFiles, outputFiles);
    }

    for (CompactionFileInfo outfile : outputFiles) {
      final CompactionNode outfileNode = compactionNodeMap.computeIfAbsent(outfile.getFileName(),
          file -> addNodeToDAG(file, seqNum, outfile.getStartKey(), outfile.getEndKey(), outfile.getColumnFamily()));

      for (CompactionFileInfo infile : inputFiles) {
        final CompactionNode infileNode = compactionNodeMap.computeIfAbsent(infile.getFileName(),
            file -> addNodeToDAG(file, seqNum, infile.getStartKey(), infile.getEndKey(), infile.getColumnFamily()));

        // Draw the edges
        if (!Objects.equals(outfileNode.getFileName(), infileNode.getFileName())) {
          forwardCompactionDAG.putEdge(outfileNode, infileNode);
          backwardCompactionDAG.putEdge(infileNode, outfileNode);
        }
      }
    }
  }

  public Set<String> pruneNodesFromDag(Set<CompactionNode> nodesToRemove) {
    pruneBackwardDag(backwardCompactionDAG, nodesToRemove);
    Set<String> sstFilesPruned = pruneForwardDag(forwardCompactionDAG, nodesToRemove);
    // Remove SST file nodes from compactionNodeMap too,
    // since those nodes won't be needed after clean up.
    nodesToRemove.forEach(compactionNodeMap::remove);
    return sstFilesPruned;
  }

  /**
   * Prunes backward DAG's upstream from the level, that needs to be removed.
   */
  Set<String> pruneBackwardDag(MutableGraph<CompactionNode> backwardDag, Set<CompactionNode> startNodes) {
    Set<String> removedFiles = new HashSet<>();
    Set<CompactionNode> currentLevel = startNodes;

    while (!currentLevel.isEmpty()) {
      Set<CompactionNode> nextLevel = new HashSet<>();
      for (CompactionNode current : currentLevel) {
        if (!backwardDag.nodes().contains(current)) {
          continue;
        }

        nextLevel.addAll(backwardDag.predecessors(current));
        backwardDag.removeNode(current);
        removedFiles.add(current.getFileName());
      }
      currentLevel = nextLevel;
    }

    return removedFiles;
  }

  /**
   * Prunes forward DAG's downstream from the level that needs to be removed.
   */
  Set<String> pruneForwardDag(MutableGraph<CompactionNode> forwardDag, Set<CompactionNode> startNodes) {
    Set<String> removedFiles = new HashSet<>();
    Set<CompactionNode> currentLevel = new HashSet<>(startNodes);

    while (!currentLevel.isEmpty()) {
      Set<CompactionNode> nextLevel = new HashSet<>();
      for (CompactionNode current : currentLevel) {
        if (!forwardDag.nodes().contains(current)) {
          continue;
        }

        nextLevel.addAll(forwardDag.successors(current));
        forwardDag.removeNode(current);
        removedFiles.add(current.getFileName());
      }

      currentLevel = nextLevel;
    }

    return removedFiles;
  }

  public MutableGraph<CompactionNode> getForwardCompactionDAG() {
    return forwardCompactionDAG;
  }

  public MutableGraph<CompactionNode> getBackwardCompactionDAG() {
    return backwardCompactionDAG;
  }

  public ConcurrentMap<String, CompactionNode> getCompactionMap() {
    return compactionNodeMap;
  }

  public CompactionNode getCompactionNode(String fileName) {
    return compactionNodeMap.get(fileName);
  }
}
