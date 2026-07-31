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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Directed graph of snapshot diff entries and Kahn topological sort for
 * dependency-ordered report emission.
 *
 * <p>Dependency rules encoded by edges (edge {@code u -> v} means {@code u}
 * must appear before {@code v}):
 * <ul>
 *   <li>Descendant DELETE before ancestor DELETE or RENAME(source), using
 *       strict source-path prefixes when intermediate directories are omitted
 *       from the report.</li>
 *   <li>Descendant non-delete before ancestor DELETE on a strict source-path
 *       prefix.</li>
 *   <li>Ancestor CREATE/RENAME(target) before descendant CREATE/RENAME/MODIFY
 *       on a strict to-snapshot path prefix.</li>
 *   <li>DELETE before CREATE/RENAME(target) that targets the same path.</li>
 *   <li>RENAME(source) before CREATE that reuses the rename source path.</li>
 *   <li>RENAME(source) before RENAME(target) that reuses the same path, so a
 *       path is freed before another rename occupies it.</li>
 *   <li>For the same object, an entry at the RENAME source path before the
 *       RENAME, and the RENAME before an entry at its target path.</li>
 *   <li>A RENAME target path cannot match a CREATE path in the same diff
 *       report; such input is rejected with {@link IllegalStateException}.</li>
 * </ul>
 */
public final class SnapDiffDependencyGraph {

  private static final Logger LOG =
      LoggerFactory.getLogger(SnapDiffDependencyGraph.class);

  private static final int INITIAL_EDGE_CAPACITY = 16;
  private static final int[] EMPTY_INT_ARRAY = new int[0];
  // Hotspot/OpenJDK cap the largest array a little below Integer.MAX_VALUE.
  // Node and edge counts are assumed to fit in int; the configured changed-key
  // limit (one billion) is well below this bound.
  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
  private static final char PATH_SEPARATOR = '/';

  private final List<SnapDiffDependencyEntry> nodes = new ArrayList<>();

  // Edges collected during construction, each encoded as (from << 32 | to).
  // Deduplicated and compacted into the CSR arrays below by buildCsr(), then
  // released so the graph keeps only the primitive adjacency.
  private long[] edges = new long[INITIAL_EDGE_CAPACITY];
  private int edgeCount;

  // Compressed sparse row adjacency. The out-edges of node i are the targets
  // adjTargets[adjOffsets[i] .. adjOffsets[i + 1]).
  private int[] adjOffsets;
  private int[] adjTargets;
  private int[] inDegree;

  // Construction-only grouping of node ids by objectId for intra-object
  // RENAME ordering. Released once edges are built.
  private long[] groupObjectIds;
  private int[] groupOffsets;
  private int[] groupNodes;

  /**
   * @throws IllegalStateException if entries contain a RENAME target path that
   *     matches a CREATE path, or if dependency edges form a cycle
   */
  public SnapDiffDependencyGraph(List<SnapDiffDependencyEntry> entries) {
    nodes.addAll(entries);
    buildDependencyEdges();
    buildCsr();
  }

  /**
   * Returns entries in dependency order using Kahn's algorithm.
   *
   * @return topologically sorted dependency entries
   * @throws IllegalStateException if the graph contains a cycle
   */
  public List<SnapDiffDependencyEntry> getOrderedEntries() {
    int nodeCount = nodes.size();
    // Work on a local copy of the in-degrees so the method is idempotent and
    // does not mutate the graph's shared state.
    int[] remainingInDegree = Arrays.copyOf(inDegree, nodeCount);
    // Split the ready set into two ring buffers so that, among nodes whose
    // dependencies are already satisfied, DELETEs are emitted before other
    // types. This retains the baseline "deletes first" ordering wherever the
    // dependency edges leave the order free. Each node is enqueued at most
    // once, so buffers sized to the node count are large enough.
    int[] deleteReady = new int[nodeCount];
    int[] otherReady = new int[nodeCount];
    int deleteHead = 0;
    int deleteTail = 0;
    int otherHead = 0;
    int otherTail = 0;
    for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
      if (remainingInDegree[nodeId] == 0) {
        if (nodes.get(nodeId).isDelete()) {
          deleteReady[deleteTail++] = nodeId;
        } else {
          otherReady[otherTail++] = nodeId;
        }
      }
    }

    List<SnapDiffDependencyEntry> orderedEntries = new ArrayList<>(nodeCount);
    while (deleteHead < deleteTail || otherHead < otherTail) {
      int nodeId = deleteHead < deleteTail
          ? deleteReady[deleteHead++] : otherReady[otherHead++];
      orderedEntries.add(nodes.get(nodeId));
      for (int i = adjOffsets[nodeId]; i < adjOffsets[nodeId + 1]; i++) {
        int dependentNodeId = adjTargets[i];
        if (--remainingInDegree[dependentNodeId] == 0) {
          if (nodes.get(dependentNodeId).isDelete()) {
            deleteReady[deleteTail++] = dependentNodeId;
          } else {
            otherReady[otherTail++] = dependentNodeId;
          }
        }
      }
    }

    if (orderedEntries.size() != nodeCount) {
      logUnresolvedNodes(orderedEntries);
      throw new IllegalStateException(
          "Cycle detected in snapshot diff dependency graph");
    }
    return orderedEntries;
  }

  private void logUnresolvedNodes(
      List<SnapDiffDependencyEntry> orderedEntries) {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    Set<SnapDiffDependencyEntry> resolved =
        Collections.newSetFromMap(new IdentityHashMap<>());
    resolved.addAll(orderedEntries);
    List<String> unresolved = new ArrayList<>();
    for (SnapDiffDependencyEntry entry : nodes) {
      if (!resolved.contains(entry)) {
        unresolved.add(entry.getDiffType() + " " + entry.getSourcePath());
      }
    }
    LOG.debug("Cycle detected in snapshot diff dependency graph. "
        + "Unresolved entries: {}", unresolved);
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

  private void addEdge(int fromNodeId, int toNodeId) {
    if (fromNodeId == toNodeId) {
      return;
    }
    if (edgeCount == edges.length) {
      edges = Arrays.copyOf(edges, grownCapacity(edges.length));
    }
    edges[edgeCount++] =
        ((long) fromNodeId << Integer.SIZE) | (toNodeId & 0xFFFFFFFFL);
  }

  private static int grownCapacity(int currentCapacity) {
    // Grow by 1.5x to bound the transient overshoot of the edge buffer, and
    // clamp to MAX_ARRAY_SIZE so the growth cannot overflow into a negative
    // (or unallocatable) size.
    int grown = currentCapacity + (currentCapacity >> 1);
    if (grown < 0 || grown > MAX_ARRAY_SIZE) {
      if (currentCapacity >= MAX_ARRAY_SIZE) {
        throw new IllegalStateException(
            "Snapshot diff dependency graph exceeds maximum edge capacity");
      }
      return MAX_ARRAY_SIZE;
    }
    return grown;
  }

  /**
   * Deduplicates the collected edges and packs them into the CSR arrays.
   * Encoding each edge as {@code (from << 32 | to)} lets a single sort group
   * neighbors by source node and place duplicates next to each other, so the
   * repeated {@code HashSet} allocations of the previous representation are no
   * longer needed. The temporary edge buffer is released afterwards.
   */
  private void buildCsr() {
    int nodeCount = nodes.size();
    adjOffsets = new int[nodeCount + 1];
    inDegree = new int[nodeCount];
    if (edgeCount == 0) {
      adjTargets = EMPTY_INT_ARRAY;
      edges = null;
      return;
    }

    Arrays.sort(edges, 0, edgeCount);

    // First pass over the sorted edges: skip duplicates while counting the
    // out-edges per source node and the in-degree per target node.
    long previous = edges[0];
    int uniqueCount = 1;
    adjOffsets[(int) (previous >>> Integer.SIZE) + 1]++;
    inDegree[(int) previous]++;
    for (int i = 1; i < edgeCount; i++) {
      long edge = edges[i];
      if (edge == previous) {
        continue;
      }
      previous = edge;
      uniqueCount++;
      adjOffsets[(int) (edge >>> Integer.SIZE) + 1]++;
      inDegree[(int) edge]++;
    }

    // Prefix sum turns per-node out-edge counts into CSR start offsets.
    for (int i = 0; i < nodeCount; i++) {
      adjOffsets[i + 1] += adjOffsets[i];
    }

    // Second pass: the unique edges are already ordered by source then target,
    // so appending their targets sequentially matches the CSR offsets.
    adjTargets = new int[uniqueCount];
    previous = edges[0];
    int index = 0;
    adjTargets[index++] = (int) previous;
    for (int i = 1; i < edgeCount; i++) {
      long edge = edges[i];
      if (edge == previous) {
        continue;
      }
      previous = edge;
      adjTargets[index++] = (int) edge;
    }
    edges = null;
  }

  private void buildDependencyEdges() {
    // At most one object occupies a given path on a given snapshot side, so a
    // single node id per path is sufficient (see addToPathIndex).
    Map<String, Integer> createNodesByPath = new HashMap<>();
    Map<String, Integer> deleteNodesByPath = new HashMap<>();
    Map<String, Integer> renameNodesBySourcePath = new HashMap<>();
    Map<String, Integer> renameNodesByTargetPath = new HashMap<>();

    for (int nodeId = 0; nodeId < nodes.size(); nodeId++) {
      SnapDiffDependencyEntry entry = nodes.get(nodeId);
      if (entry.isDelete()) {
        addToPathIndex(deleteNodesByPath, entry.getSourcePath(), nodeId);
      } else {
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

    addPathConflictEdges(deleteNodesByPath, createNodesByPath,
        renameNodesByTargetPath);
    addRenameBeforeCreateEdges(renameNodesBySourcePath, createNodesByPath);
    addRenameChainEdges(renameNodesBySourcePath, renameNodesByTargetPath);
    addPathPrefixFromSnapshotEdges(deleteNodesByPath, renameNodesBySourcePath);
    addPathPrefixToSnapshotEdges(createNodesByPath, renameNodesByTargetPath);

    buildObjectIdGroups();
    addIntraObjectRenameEdges();

    groupObjectIds = null;
    groupOffsets = null;
    groupNodes = null;
  }

  /**
   * Groups node ids by objectId into primitive arrays for intra-object RENAME
   * ordering. objectIds are sorted and de-duplicated so the matching node ids
   * for one objectId are the slice of {@link #groupNodes} delimited by
   * {@link #groupOffsets}.
   */
  private void buildObjectIdGroups() {
    int nodeCount = nodes.size();
    long[] sorted = new long[nodeCount];
    for (int i = 0; i < nodeCount; i++) {
      sorted[i] = nodes.get(i).getObjectId();
    }
    Arrays.sort(sorted);

    int uniqueCount = 0;
    long previous = 0L;
    for (int i = 0; i < nodeCount; i++) {
      long objectId = sorted[i];
      if (uniqueCount == 0 || objectId != previous) {
        sorted[uniqueCount++] = objectId;
        previous = objectId;
      }
    }
    groupObjectIds = Arrays.copyOf(sorted, uniqueCount);

    int[] rankByNode = new int[nodeCount];
    groupOffsets = new int[uniqueCount + 1];
    for (int i = 0; i < nodeCount; i++) {
      int rank = Arrays.binarySearch(groupObjectIds, nodes.get(i).getObjectId());
      rankByNode[i] = rank;
      groupOffsets[rank + 1]++;
    }
    for (int i = 0; i < uniqueCount; i++) {
      groupOffsets[i + 1] += groupOffsets[i];
    }

    groupNodes = new int[nodeCount];
    int[] fillCursor = Arrays.copyOf(groupOffsets, uniqueCount);
    for (int i = 0; i < nodeCount; i++) {
      groupNodes[fillCursor[rankByNode[i]]++] = i;
    }
  }

  /**
   * Orders the non-delete entries of a single object relative to its RENAME.
   * An entry reported at the RENAME source (from-snapshot) path must be applied
   * before the path is renamed away; an entry reported at the RENAME target
   * (to-snapshot) path must be applied after the rename creates that path.
   */
  private void addIntraObjectRenameEdges() {
    for (int rank = 0; rank < groupObjectIds.length; rank++) {
      int start = groupOffsets[rank];
      int end = groupOffsets[rank + 1];
      if (end - start < 2) {
        continue;
      }
      int renameNodeId = -1;
      for (int i = start; i < end; i++) {
        if (nodes.get(groupNodes[i]).getDiffType() == DiffType.RENAME) {
          renameNodeId = groupNodes[i];
          break;
        }
      }
      if (renameNodeId < 0) {
        continue;
      }
      SnapDiffDependencyEntry rename = nodes.get(renameNodeId);
      for (int i = start; i < end; i++) {
        int nodeId = groupNodes[i];
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

  private static void addToPathIndex(Map<String, Integer> pathIndex,
      String path, int nodeId) {
    if (path == null) {
      return;
    }
    // At most one object can occupy a path on a given snapshot side, so the
    // first node wins. A second node on the same path indicates unexpected or
    // duplicate diff input.
    Integer existing = pathIndex.putIfAbsent(path, nodeId);
    if (existing != null) {
      LOG.debug("Multiple diff entries share path '{}'; keeping node {}, "
          + "ignoring node {}", path, existing, nodeId);
    }
  }

  /**
   * Orders renames that reuse the same path: the rename whose source frees a
   * path must precede the rename whose target occupies that same path.
   */
  private void addRenameChainEdges(
      Map<String, Integer> renameNodesBySourcePath,
      Map<String, Integer> renameNodesByTargetPath) {
    for (Map.Entry<String, Integer> sourceEntry :
        renameNodesBySourcePath.entrySet()) {
      Integer targetNodeId =
          renameNodesByTargetPath.get(sourceEntry.getKey());
      if (targetNodeId != null) {
        addEdge(sourceEntry.getValue(), targetNodeId);
      }
    }
  }

  private void addPathConflictEdges(Map<String, Integer> deleteNodesByPath,
      Map<String, Integer> createNodesByPath,
      Map<String, Integer> renameNodesByTargetPath) {
    for (Map.Entry<String, Integer> deleteEntry :
        deleteNodesByPath.entrySet()) {
      String path = deleteEntry.getKey();
      int deleteNodeId = deleteEntry.getValue();
      Integer createNodeId = createNodesByPath.get(path);
      if (createNodeId != null) {
        addEdge(deleteNodeId, createNodeId);
      }
      Integer renameTargetNodeId = renameNodesByTargetPath.get(path);
      if (renameTargetNodeId != null) {
        addEdge(deleteNodeId, renameTargetNodeId);
      }
    }
  }

  private void addRenameBeforeCreateEdges(
      Map<String, Integer> renameNodesBySourcePath,
      Map<String, Integer> createNodesByPath) {
    for (Map.Entry<String, Integer> createEntry :
        createNodesByPath.entrySet()) {
      Integer renameNodeId =
          renameNodesBySourcePath.get(createEntry.getKey());
      if (renameNodeId != null) {
        addEdge(renameNodeId, createEntry.getValue());
      }
    }
  }

  /**
   * From-snapshot path-prefix edges. Intermediate directories may be omitted
   * from the report; ancestor ordering is derived from strict path prefixes.
   */
  private void addPathPrefixFromSnapshotEdges(
      Map<String, Integer> deleteNodesByPath,
      Map<String, Integer> renameNodesBySourcePath) {
    for (Map.Entry<String, Integer> deleteEntry :
        deleteNodesByPath.entrySet()) {
      int descendantDeleteNodeId = deleteEntry.getValue();
      forEachStrictPrefix(deleteEntry.getKey(), ancestorPath -> {
        Integer ancestorDeleteNodeId = deleteNodesByPath.get(ancestorPath);
        if (ancestorDeleteNodeId != null) {
          addEdge(descendantDeleteNodeId, ancestorDeleteNodeId);
        }
        Integer ancestorRenameNodeId =
            renameNodesBySourcePath.get(ancestorPath);
        if (ancestorRenameNodeId != null) {
          addEdge(descendantDeleteNodeId, ancestorRenameNodeId);
        }
      });
    }

    for (int nodeId = 0; nodeId < nodes.size(); nodeId++) {
      SnapDiffDependencyEntry entry = nodes.get(nodeId);
      if (entry.isDelete()) {
        continue;
      }
      final int fromNodeId = nodeId;
      forEachStrictPrefix(entry.getSourcePath(), ancestorPath -> {
        Integer ancestorDeleteNodeId = deleteNodesByPath.get(ancestorPath);
        if (ancestorDeleteNodeId != null) {
          addEdge(fromNodeId, ancestorDeleteNodeId);
        }
      });
    }
  }

  /**
   * To-snapshot path-prefix edges. A descendant CREATE/RENAME/MODIFY cannot be
   * applied until ancestor directories exist on the to-snapshot side.
   */
  private void addPathPrefixToSnapshotEdges(
      Map<String, Integer> createNodesByPath,
      Map<String, Integer> renameNodesByTargetPath) {
    for (int nodeId = 0; nodeId < nodes.size(); nodeId++) {
      SnapDiffDependencyEntry entry = nodes.get(nodeId);
      if (entry.isDelete()) {
        continue;
      }
      String toSnapshotPath = getToSnapshotPath(entry);
      if (toSnapshotPath == null) {
        continue;
      }
      final int toNodeId = nodeId;
      forEachStrictPrefix(toSnapshotPath, ancestorPath -> {
        Integer ancestorCreateNodeId = createNodesByPath.get(ancestorPath);
        if (ancestorCreateNodeId != null) {
          addEdge(ancestorCreateNodeId, toNodeId);
        }
        Integer ancestorRenameNodeId =
            renameNodesByTargetPath.get(ancestorPath);
        if (ancestorRenameNodeId != null) {
          addEdge(ancestorRenameNodeId, toNodeId);
        }
      });
    }
  }

  private static String getToSnapshotPath(SnapDiffDependencyEntry entry) {
    if (entry.getDiffType() == DiffType.RENAME) {
      return entry.getTargetPath();
    }
    return entry.getSourcePath();
  }

  /**
   * Invokes {@code consumer} for each strict prefix of {@code path}. For
   * {@code A/B/C/file1} the prefixes are {@code A}, {@code A/B}, and
   * {@code A/B/C}. A path with no separator has no strict prefixes.
   */
  private static void forEachStrictPrefix(String path, PrefixConsumer consumer) {
    if (path == null) {
      return;
    }
    int separator = path.indexOf(PATH_SEPARATOR);
    while (separator > 0) {
      consumer.accept(path.substring(0, separator));
      separator = path.indexOf(PATH_SEPARATOR, separator + 1);
    }
  }

  @FunctionalInterface
  private interface PrefixConsumer {
    void accept(String prefix);
  }

  private static void validateRenameTargetDoesNotMatchCreatePath(
      Map<String, Integer> renameNodesByTargetPath,
      Map<String, Integer> createNodesByPath) {
    for (String path : createNodesByPath.keySet()) {
      if (renameNodesByTargetPath.containsKey(path)) {
        throw new IllegalStateException(String.format(
            "Invalid snapshot diff report: RENAME target path '%s' cannot match "
                + "a CREATE path in the same diff", path));
      }
    }
  }
}
