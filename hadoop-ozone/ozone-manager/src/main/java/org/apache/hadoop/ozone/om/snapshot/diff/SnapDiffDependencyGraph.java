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

package org.apache.hadoop.ozone.om.snapshot.diff;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Directed graph of snapshot diff entries and Kahn topological sort for
 * dependency-ordered report emission.
 *
 * <p>Entries must satisfy the path namespace contract in
 * {@link SnapDiffDependencyEntry}. DELETE and MODIFY paths are from-snapshot,
 * CREATE paths are to-snapshot, and RENAME carries both.
 *
 * <p>Dependency rules encoded by edges (edge {@code u -> v} means {@code u}
 * must appear before {@code v}):
 * <ul>
 *   <li>Descendant DELETE before ancestor DELETE or RENAME(source), using
 *       strict source-path prefixes when intermediate directories are omitted
 *       from the report.</li>
 *   <li>Descendant RENAME or MODIFY before ancestor DELETE or RENAME(source)
 *       on a strict source-path prefix. CREATE is omitted because its source
 *       path is a to-snapshot path, not a from-snapshot path.</li>
 *   <li>Ancestor CREATE/RENAME(target) before descendant CREATE/RENAME/MODIFY
 *       on a strict to-snapshot path prefix, except when the ancestor path is
 *       reoccupied (freed on the from-snapshot side by DELETE or RENAME
 *       source, and re-occupied on the to-snapshot side by CREATE or RENAME
 *       target in the same diff) and the descendant is a from-snapshot
 *       MODIFY/RENAME still reported under that path.</li>
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
  private static final int MAX_CYCLE_ENTRIES_IN_MESSAGE = 16;
  // Defensive JVM-side ceiling on the growable long[] edge buffer used during
  // construction (see grownCapacity). Hotspot/OpenJDK cap the largest array a
  // little below Integer.MAX_VALUE; going any higher throws OutOfMemoryError
  // with an unhelpful message.
  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
  private static final String PATH_SEPARATOR = OM_KEY_PREFIX;

  private final List<SnapDiffDependencyEntry> nodes = new ArrayList<>();

  // Edges collected during construction, each encoded as (from << 32 | to).
  // Deduplicated and compacted into the CSR arrays below by buildCsr(), then
  // released so the graph keeps only the primitive adjacency.
  private long[] edges;
  private int edgeCount;

  // Compressed sparse row adjacency. The out-edges of node i are the targets
  // adjTargets[adjOffsets[i] .. adjOffsets[i + 1]).
  private int[] adjOffsets;
  private int[] adjTargets;
  private int[] inDegree;

  // Construction-only grouping of node ids by objectId for intra-object
  // RENAME ordering. Only objectIds that have a RENAME entry are tracked;
  private long[] groupObjectIds;
  private int[] groupOffsets;
  private int[] groupNodes;

  /**
   * @throws IllegalStateException if entries contain a RENAME target path that
   *     matches a CREATE path
   */
  public SnapDiffDependencyGraph(List<SnapDiffDependencyEntry> entries) {
    Objects.requireNonNull(entries, "entries");
    nodes.addAll(entries);
    // Size the initial edge buffer to roughly the node count so realistic
    // graphs (edge density 3-8 per node) skip most of the doubling copies.
    // Clamp to INITIAL_EDGE_CAPACITY so tiny inputs still allocate cheaply.
    edges = new long[Math.max(INITIAL_EDGE_CAPACITY, nodes.size())];
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
      List<String> unresolved = collectUnresolvedNodes(orderedEntries);
      LOG.debug("Cycle detected in snapshot diff dependency graph. "
          + "Unresolved entries: {}", unresolved);
      int total = unresolved.size();
      List<String> sample = total > MAX_CYCLE_ENTRIES_IN_MESSAGE
          ? unresolved.subList(0, MAX_CYCLE_ENTRIES_IN_MESSAGE) : unresolved;
      throw new IllegalStateException(String.format(
          "Cycle detected in snapshot diff dependency graph; %d unresolved "
              + "entries (showing up to %d): %s",
          total, MAX_CYCLE_ENTRIES_IN_MESSAGE, sample));
    }
    return orderedEntries;
  }

  private List<String> collectUnresolvedNodes(
      List<SnapDiffDependencyEntry> orderedEntries) {
    Set<SnapDiffDependencyEntry> resolved =
        Collections.newSetFromMap(new IdentityHashMap<>());
    resolved.addAll(orderedEntries);
    List<String> unresolved = new ArrayList<>();
    for (SnapDiffDependencyEntry entry : nodes) {
      if (!resolved.contains(entry)) {
        unresolved.add(entry.getDiffType() + " " + entry.getSourcePath());
      }
    }
    return unresolved;
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
    // single node id per path is sufficient. Count each category first
    // so the four indexes can be pre-sized exactly.
    int createCount = 0;
    int deleteCount = 0;
    int renameCount = 0;
    for (SnapDiffDependencyEntry entry : nodes) {
      DiffType diffType = entry.getDiffType();
      if (diffType == DiffType.DELETE) {
        deleteCount++;
      } else if (diffType == DiffType.CREATE) {
        createCount++;
      } else if (diffType == DiffType.RENAME) {
        renameCount++;
      }
    }
    Map<String, Integer> createNodesByPath =
        new HashMap<>(expectedHashMapCapacity(createCount));
    Map<String, Integer> deleteNodesByPath =
        new HashMap<>(expectedHashMapCapacity(deleteCount));
    Map<String, Integer> renameNodesBySourcePath =
        new HashMap<>(expectedHashMapCapacity(renameCount));
    Map<String, Integer> renameNodesByTargetPath =
        new HashMap<>(expectedHashMapCapacity(renameCount));

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

    Set<String> reoccupiedPaths = buildReoccupiedPaths(deleteNodesByPath,
        renameNodesBySourcePath, createNodesByPath, renameNodesByTargetPath);

    addPathConflictEdges(deleteNodesByPath, createNodesByPath,
        renameNodesByTargetPath);
    addRenameBeforeCreateEdges(renameNodesBySourcePath, createNodesByPath);
    addRenameChainEdges(renameNodesBySourcePath, renameNodesByTargetPath);
    addPathPrefixFromSnapshotEdges(deleteNodesByPath, renameNodesBySourcePath);
    addPathPrefixToSnapshotEdges(createNodesByPath, renameNodesByTargetPath,
        reoccupiedPaths);

    buildObjectIdGroups(renameCount);
    addIntraObjectRenameEdges();

    groupObjectIds = null;
    groupOffsets = null;
    groupNodes = null;

    // Path strings were cached on each entry so that repeated edge-building
    // passes did not re-decode. The graph no longer reads them once edges
    // are built; drop the caches to reclaim per-entry heap.
    for (SnapDiffDependencyEntry entry : nodes) {
      entry.clearPathCache();
    }
  }

  /**
   * Groups node ids by objectId, restricted to objectIds that have a RENAME
   * entry (only those can pick up intra-object edges). The slice of
   * {@link #groupNodes} between {@link #groupOffsets}{@code [rank]} and
   * {@code [rank + 1]} lists the node ids for {@link #groupObjectIds}
   * {@code [rank]}.
   *
   * <p>Uses a single hash-map lookup per node and no per-node binary search,
   * so peak transient memory here is bounded by the number of nodes that
   * belong to a renamed object rather than the full node count.
   */
  private void buildObjectIdGroups(int renameCount) {
    if (renameCount == 0) {
      groupObjectIds = new long[0];
      groupOffsets = new int[1];
      groupNodes = EMPTY_INT_ARRAY;
      return;
    }
    int nodeCount = nodes.size();
    // Rank the RENAMEd objectIds. Presize to the exact rename count; there is
    // at most one RENAME per objectId (a second RENAME on the same id is
    // rejected by addToPathIndex).
    Map<Long, Integer> rankByObjectId =
        new HashMap<>(expectedHashMapCapacity(renameCount));
    long[] renameObjectIds = new long[renameCount];
    int uniqueCount = 0;
    for (int i = 0; i < nodeCount && uniqueCount < renameCount; i++) {
      SnapDiffDependencyEntry entry = nodes.get(i);
      if (entry.getDiffType() == DiffType.RENAME) {
        renameObjectIds[uniqueCount] = entry.getObjectId();
        rankByObjectId.put(entry.getObjectId(), uniqueCount);
        uniqueCount++;
      }
    }
    groupObjectIds = renameObjectIds;

    // Count how many nodes belong to each renamed object.
    groupOffsets = new int[uniqueCount + 1];
    int groupedNodeCount = 0;
    for (int i = 0; i < nodeCount; i++) {
      Integer rank = rankByObjectId.get(nodes.get(i).getObjectId());
      if (rank != null) {
        groupOffsets[rank + 1]++;
        groupedNodeCount++;
      }
    }
    for (int i = 0; i < uniqueCount; i++) {
      groupOffsets[i + 1] += groupOffsets[i];
    }

    // Fill groupNodes. Sized to groupedNodeCount, not the full node count.
    groupNodes = new int[groupedNodeCount];
    int[] fillCursor = Arrays.copyOf(groupOffsets, uniqueCount);
    for (int i = 0; i < nodeCount; i++) {
      Integer rank = rankByObjectId.get(nodes.get(i).getObjectId());
      if (rank != null) {
        groupNodes[fillCursor[rank]++] = i;
      }
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

  /**
   * Converts an expected element count to the initial capacity that keeps a
   * default-load-factor (0.75) HashMap from resizing while it fills to that
   * count. Mirrors what JDK 19's {@code HashMap.newHashMap(int)} does, but
   * stays on Java 8 source.
   */
  private static int expectedHashMapCapacity(int expectedSize) {
    if (expectedSize <= 0) {
      return 1;
    }
    // ceil(expectedSize / 0.75) with an overflow guard for large sizes.
    long capacity = (long) expectedSize + (expectedSize + 2) / 3;
    return capacity > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) capacity;
  }

  private static void addToPathIndex(Map<String, Integer> pathIndex,
      String path, int nodeId) {
    if (path == null) {
      return;
    }
    // At most one object can occupy a path on a given snapshot side, so the
    // first node wins. A second node on the same path indicates malformed diff
    // input (per SnapshotDiffManager, an object appears at most once in each of
    // the CREATE/DELETE/RENAME(source)/RENAME(target) indices).
    Integer existing = pathIndex.putIfAbsent(path, nodeId);
    if (existing != null) {
      LOG.warn("Multiple diff entries share path '{}'; keeping node {}, "
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
   *
   * <p>DELETE, RENAME, and MODIFY entries all carry a from-snapshot source
   * path (RENAME uses the pre-rename source, and MODIFY is reported at the
   * old key path per SnapshotDiffManager). CREATE is excluded because a
   * CREATE's source path is a to-snapshot path; to-snapshot prefixes are
   * handled by {@link #addPathPrefixToSnapshotEdges}. Mixing the two would
   * generate spurious edges (e.g. after DELETE A + CREATE A, a CREATE A/child
   * would incorrectly point at DELETE A and produce a cycle).
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
      DiffType diffType = entry.getDiffType();
      if (diffType != DiffType.RENAME && diffType != DiffType.MODIFY) {
        continue;
      }
      final int fromNodeId = nodeId;
      forEachStrictPrefix(entry.getSourcePath(), ancestorPath -> {
        Integer ancestorDeleteNodeId = deleteNodesByPath.get(ancestorPath);
        if (ancestorDeleteNodeId != null) {
          addEdge(fromNodeId, ancestorDeleteNodeId);
        }
        Integer ancestorRenameNodeId =
            renameNodesBySourcePath.get(ancestorPath);
        if (ancestorRenameNodeId != null) {
          addEdge(fromNodeId, ancestorRenameNodeId);
        }
      });
    }
  }

  /**
   * To-snapshot path-prefix edges. A descendant CREATE/RENAME/MODIFY cannot be
   * applied until ancestor directories exist on the to-snapshot side.
   *
   * <p>When a path appears in both {@code deleteNodesByPath} and
   * {@code createNodesByPath} it is a <em>replaced</em> path (old object
   * deleted, new object created at the same path). A CREATE at a replaced path
   * must not order from-snapshot MODIFY/RENAME descendants that still live
   * under the old tree, or a cycle appears (CREATE {@code ->} child {@code ->}
   * DELETE {@code ->} CREATE). Pure CREATE descendants (new to-snapshot keys)
   * still depend on the ancestor CREATE.
   */
  private void addPathPrefixToSnapshotEdges(
      Map<String, Integer> createNodesByPath,
      Map<String, Integer> renameNodesByTargetPath,
      Set<String> reoccupiedPaths) {
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
        if (ancestorCreateNodeId != null
            && !shouldSkipReoccupiedPathToSnapshotEdge(ancestorPath,
            reoccupiedPaths, entry)) {
          addEdge(ancestorCreateNodeId, toNodeId);
        }
        Integer ancestorRenameNodeId =
            renameNodesByTargetPath.get(ancestorPath);
        if (ancestorRenameNodeId != null
            && !shouldSkipReoccupiedPathToSnapshotEdge(ancestorPath,
            reoccupiedPaths, entry)) {
          addEdge(ancestorRenameNodeId, toNodeId);
        }
      });
    }
  }

  /**
   * Paths that are freed on the from-snapshot side (DELETE or RENAME source)
   * and re-occupied on the to-snapshot side (CREATE or RENAME target) in the
   * same diff. A from-snapshot MODIFY/RENAME descendant reported under such a
   * path still lives in the old subtree, so it must not depend on the
   * to-snapshot occupier of the ancestor path.
   */
  private static Set<String> buildReoccupiedPaths(
      Map<String, Integer> deleteNodesByPath,
      Map<String, Integer> renameNodesBySourcePath,
      Map<String, Integer> createNodesByPath,
      Map<String, Integer> renameNodesByTargetPath) {
    int occupierCount = createNodesByPath.size() + renameNodesByTargetPath.size();
    Set<String> reoccupiedPaths =
        new HashSet<>(expectedHashMapCapacity(occupierCount));
    for (String path : createNodesByPath.keySet()) {
      if (deleteNodesByPath.containsKey(path)
          || renameNodesBySourcePath.containsKey(path)) {
        reoccupiedPaths.add(path);
      }
    }
    for (String path : renameNodesByTargetPath.keySet()) {
      if (deleteNodesByPath.containsKey(path)
          || renameNodesBySourcePath.containsKey(path)) {
        reoccupiedPaths.add(path);
      }
    }
    return reoccupiedPaths;
  }

  /**
   * Skip {@code ancestorPath -> descendant} to-snapshot prefix edges when the
   * ancestor path is reoccupied and the descendant is a from-snapshot
   * MODIFY/RENAME still reported under the old tree at that path.
   */
  private static boolean shouldSkipReoccupiedPathToSnapshotEdge(
      String ancestorPath,
      Set<String> reoccupiedPaths,
      SnapDiffDependencyEntry descendant) {
    if (!reoccupiedPaths.contains(ancestorPath)) {
      return false;
    }
    DiffType diffType = descendant.getDiffType();
    if (diffType != DiffType.MODIFY && diffType != DiffType.RENAME) {
      return false;
    }
    return isStrictPathPrefix(ancestorPath, descendant.getSourcePath());
  }

  private static boolean isStrictPathPrefix(String prefix, String path) {
    if (prefix == null || path == null) {
      return false;
    }
    return path.startsWith(prefix + PATH_SEPARATOR);
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
