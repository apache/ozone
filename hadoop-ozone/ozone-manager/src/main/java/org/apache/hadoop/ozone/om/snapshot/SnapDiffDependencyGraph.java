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

package org.apache.hadoop.ozone.om.snapshot;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;

/**
 * Directed graph of snapshot diff entries and Kahn topological sort for
 * dependency-ordered report emission.
 *
 * <p>Dependency rules encoded by edges (edge {@code u -> v} means {@code u}
 * must appear before {@code v}):
 * <ul>
 *   <li>Parent CREATE/RENAME/MODIFY before child CREATE/RENAME/MODIFY.</li>
 *   <li>Child DELETE before parent DELETE.</li>
 *   <li>Non-delete entry before DELETE of its parent object.</li>
 *   <li>DELETE before CREATE/RENAME that targets the same path.</li>
 *   <li>RENAME before CREATE that reuses the rename source path.</li>
 *   <li>For the same object, an entry at the RENAME source path before the
 *       RENAME, and the RENAME before an entry at its target path.</li>
 * </ul>
 *
 * <p>A RENAME target path cannot match a CREATE path in the same diff report.
 */
public final class SnapDiffDependencyGraph {

  private final List<SnapDiffDependencyEntry> nodes = new ArrayList<>();
  private final Map<Integer, Set<Integer>> adjacencyList = new HashMap<>();
  private final Map<Integer, Integer> inDegree = new HashMap<>();

  /**
   * @throws IllegalStateException if entries contain a RENAME target path that
   *     matches a CREATE path, or if dependency edges form a cycle
   */
  public SnapDiffDependencyGraph(List<SnapDiffDependencyEntry> entries) {
    for (SnapDiffDependencyEntry entry : entries) {
      addNode(entry);
    }
    buildDependencyEdges();
  }

  /**
   * Returns entries in dependency order using Kahn's algorithm.
   *
   * @return topologically sorted dependency entries
   * @throws IllegalStateException if the graph contains a cycle
   */
  public List<SnapDiffDependencyEntry> getOrderedEntries() {
    Queue<Integer> zeroInDegree = new ArrayDeque<>();
    for (int nodeId = 0; nodeId < nodes.size(); nodeId++) {
      if (inDegree.get(nodeId) == 0) {
        zeroInDegree.add(nodeId);
      }
    }

    List<SnapDiffDependencyEntry> orderedEntries = new ArrayList<>(nodes.size());
    while (!zeroInDegree.isEmpty()) {
      int nodeId = zeroInDegree.remove();
      orderedEntries.add(nodes.get(nodeId));
      for (int dependentNodeId : adjacencyList.get(nodeId)) {
        int updatedInDegree = inDegree.get(dependentNodeId) - 1;
        inDegree.put(dependentNodeId, updatedInDegree);
        if (updatedInDegree == 0) {
          zeroInDegree.add(dependentNodeId);
        }
      }
    }

    if (orderedEntries.size() != nodes.size()) {
      throw new IllegalStateException(
          "Cycle detected in snapshot diff dependency graph");
    }
    return orderedEntries;
  }

  /**
   * Converts dependency-ordered entries to report entries.
   */
  public static List<DiffReportEntry> toOrderedReportEntries(
      List<SnapDiffDependencyEntry> orderedEntries) {
    List<DiffReportEntry> reportEntries =
        new ArrayList<>(orderedEntries.size());
    for (SnapDiffDependencyEntry entry : orderedEntries) {
      reportEntries.add(entry.getReportEntry());
    }
    return reportEntries;
  }

  private int addNode(SnapDiffDependencyEntry entry) {
    int nodeId = nodes.size();
    nodes.add(entry);
    adjacencyList.put(nodeId, new HashSet<>());
    inDegree.put(nodeId, 0);
    return nodeId;
  }

  private void addEdge(int fromNodeId, int toNodeId) {
    if (fromNodeId == toNodeId) {
      return;
    }
    if (adjacencyList.get(fromNodeId).add(toNodeId)) {
      inDegree.put(toNodeId, inDegree.get(toNodeId) + 1);
    }
  }

  private void buildDependencyEdges() {
    // One objectId can map to multiple non-delete nodes, for example RENAME + MODIFY.
    Map<Long, List<Integer>> nonDeleteNodesByObjectId = new HashMap<>();
    // Each objectId has at most one DELETE entry after top-level delete retention.
    Map<Long, Integer> deleteNodesByObjectId = new HashMap<>();
    Map<String, List<Integer>> createNodesByPath = new HashMap<>();
    Map<String, List<Integer>> deleteNodesByPath = new HashMap<>();
    Map<String, List<Integer>> renameNodesBySourcePath = new HashMap<>();
    Map<String, List<Integer>> renameNodesByTargetPath = new HashMap<>();

    for (int nodeId = 0; nodeId < nodes.size(); nodeId++) {
      SnapDiffDependencyEntry entry = nodes.get(nodeId);
      if (entry.isDelete()) {
        deleteNodesByObjectId.put(entry.getObjectId(), nodeId);
        addToPathIndex(deleteNodesByPath, entry.getSourcePath(), nodeId);
      } else {
        nonDeleteNodesByObjectId
            .computeIfAbsent(entry.getObjectId(), ignored -> new ArrayList<>())
            .add(nodeId);
        DiffType diffType = entry.getDiffType();
        if (diffType == DiffType.CREATE) {
          addToPathIndex(createNodesByPath, entry.getSourcePath(), nodeId);
        } else if (diffType == DiffType.RENAME) {
          addToPathIndex(renameNodesBySourcePath, entry.getSourcePath(), nodeId);
          addToPathIndex(renameNodesByTargetPath, entry.getTargetPath(), nodeId);
        }
      }
    }

    validateRenameTargetDoesNotMatchCreatePath(renameNodesByTargetPath,
        createNodesByPath);

    for (int nodeId = 0; nodeId < nodes.size(); nodeId++) {
      SnapDiffDependencyEntry entry = nodes.get(nodeId);
      long parentObjectId = entry.getParentObjectId();
      if (parentObjectId <= 0L) {
        continue;
      }

      Integer parentDeleteNodeId = deleteNodesByObjectId.get(parentObjectId);
      if (entry.isDelete()) {
        if (parentDeleteNodeId != null) {
          addEdge(nodeId, parentDeleteNodeId);
        }
      } else {
        addEdgesFromNodes(
            nonDeleteNodesByObjectId.getOrDefault(parentObjectId,
                Collections.emptyList()),
            nodeId);
        if (parentDeleteNodeId != null) {
          addEdge(nodeId, parentDeleteNodeId);
        }
      }
    }

    addPathConflictEdges(deleteNodesByPath, createNodesByPath,
        renameNodesByTargetPath);
    addRenameBeforeCreateEdges(renameNodesBySourcePath, createNodesByPath);
    addIntraObjectRenameEdges(nonDeleteNodesByObjectId);
  }

  /**
   * Orders the non-delete entries of a single object relative to its RENAME.
   * An entry reported at the RENAME source (from-snapshot) path must be applied
   * before the path is renamed away; an entry reported at the RENAME target
   * (to-snapshot) path must be applied after the rename creates that path.
   */
  private void addIntraObjectRenameEdges(
      Map<Long, List<Integer>> nonDeleteNodesByObjectId) {
    for (List<Integer> objectNodes : nonDeleteNodesByObjectId.values()) {
      if (objectNodes.size() < 2) {
        continue;
      }
      Integer renameNodeId = null;
      for (int nodeId : objectNodes) {
        if (nodes.get(nodeId).getDiffType() == DiffType.RENAME) {
          renameNodeId = nodeId;
          break;
        }
      }
      if (renameNodeId == null) {
        continue;
      }
      SnapDiffDependencyEntry rename = nodes.get(renameNodeId);
      for (int nodeId : objectNodes) {
        if (nodeId == renameNodeId) {
          continue;
        }
        String path = nodes.get(nodeId).getSourcePath();
        if (path == null) {
          continue;
        }
        if (path.equals(rename.getSourcePath())) {
          addEdge(nodeId, renameNodeId);
        } else if (path.equals(rename.getTargetPath())) {
          addEdge(renameNodeId, nodeId);
        }
      }
    }
  }

  private static void addToPathIndex(Map<String, List<Integer>> pathIndex,
      String path, int nodeId) {
    if (path == null) {
      return;
    }
    pathIndex.computeIfAbsent(path, ignored -> new ArrayList<>()).add(nodeId);
  }

  private void addEdgesToNodes(int fromNodeId, List<Integer> toNodeIds) {
    for (int toNodeId : toNodeIds) {
      addEdge(fromNodeId, toNodeId);
    }
  }

  private void addEdgesFromNodes(List<Integer> fromNodeIds, int toNodeId) {
    for (int fromNodeId : fromNodeIds) {
      addEdge(fromNodeId, toNodeId);
    }
  }

  private void addPathConflictEdges(Map<String, List<Integer>> deleteNodesByPath,
      Map<String, List<Integer>> createNodesByPath,
      Map<String, List<Integer>> renameNodesByTargetPath) {
    for (Map.Entry<String, List<Integer>> deleteEntry :
        deleteNodesByPath.entrySet()) {
      String path = deleteEntry.getKey();
      for (int deleteNodeId : deleteEntry.getValue()) {
        addEdgesToNodes(deleteNodeId,
            createNodesByPath.getOrDefault(path, Collections.emptyList()));
        addEdgesToNodes(deleteNodeId,
            renameNodesByTargetPath.getOrDefault(path,
                Collections.emptyList()));
      }
    }
  }

  private void addRenameBeforeCreateEdges(
      Map<String, List<Integer>> renameNodesBySourcePath,
      Map<String, List<Integer>> createNodesByPath) {
    for (Map.Entry<String, List<Integer>> createEntry :
        createNodesByPath.entrySet()) {
      String path = createEntry.getKey();
      for (int renameNodeId : renameNodesBySourcePath.getOrDefault(path,
          Collections.emptyList())) {
        addEdgesToNodes(renameNodeId, createEntry.getValue());
      }
    }
  }

  private static void validateRenameTargetDoesNotMatchCreatePath(
      Map<String, List<Integer>> renameNodesByTargetPath,
      Map<String, List<Integer>> createNodesByPath) {
    for (Map.Entry<String, List<Integer>> createEntry :
        createNodesByPath.entrySet()) {
      String path = createEntry.getKey();
      if (!renameNodesByTargetPath.containsKey(path)) {
        continue;
      }
      throw new IllegalStateException(String.format(
          "Invalid snapshot diff report: RENAME target path '%s' cannot match "
              + "a CREATE path in the same diff", path));
    }
  }
}
