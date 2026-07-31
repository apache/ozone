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

import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.CREATE;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.DELETE;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.MODIFY;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.RENAME;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone.getDiffReportEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.junit.jupiter.api.Test;

class TestSnapDiffDependencyGraph {

  @Test
  void testAncestorCreateBeforeDescendantCreateOnPathPrefix() {
    // CREATE parent/child requires CREATE parent first (to-snapshot prefix).
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(2L, 1L, CREATE, "parent/child"),
        entry(1L, 0L, CREATE, "parent"));

    List<DiffType> orderedTypes = toDiffTypes(sort(entries));
    assertEquals(Arrays.asList(CREATE, CREATE), orderedTypes);
    assertPathOrder(entries, "parent", "parent/child");
  }

  @Test
  void testDescendantDeleteBeforeAncestorDeleteOnPathPrefix() {
    // DELETE parent/child requires DELETE parent/child before DELETE parent.
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(1L, 0L, DELETE, "parent"),
        entry(2L, 1L, DELETE, "parent/child"));

    List<DiffType> orderedTypes = toDiffTypes(sort(entries));
    assertEquals(Arrays.asList(DELETE, DELETE), orderedTypes);
    assertPathOrder(entries, "parent/child", "parent");
  }

  @Test
  void testDeleteBeforeCreateOnSamePath() {
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(2L, 0L, CREATE, "dir/key"),
        entry(1L, 0L, DELETE, "dir/key"));

    List<DiffType> orderedTypes = toDiffTypes(sort(entries));
    assertEquals(Arrays.asList(DELETE, CREATE), orderedTypes);
  }

  @Test
  void testDeleteBeforeRenameOnSameTargetPath() {
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(2L, 0L, RENAME, "old/key", "dir/key"),
        entry(1L, 0L, DELETE, "dir/key"));

    List<DiffType> orderedTypes = toDiffTypes(sort(entries));
    assertEquals(Arrays.asList(DELETE, RENAME), orderedTypes);
  }

  @Test
  void testRenameBeforeCreateWhenRenameFreesSourcePath() {
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(3L, 0L, CREATE, "dir/key"),
        entry(2L, 0L, RENAME, "dir/key", "dir/renamed-key"));

    List<DiffType> orderedTypes = toDiffTypes(sort(entries));
    assertEquals(Arrays.asList(RENAME, CREATE), orderedTypes);
  }

  @Test
  void testRenameTargetPathMatchingCreateThrowsException() {
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(3L, 0L, CREATE, "dir/key"),
        entry(2L, 0L, RENAME, "old/key", "dir/key"));

    assertThrows(IllegalStateException.class,
        () -> new SnapDiffDependencyGraph(entries));
  }

  @Test
  void testDescendantDeleteBeforeAncestorRenameOnPathPrefix() {
    // DELETE A/child must precede RENAME A -> B (A is a strict prefix of A/child).
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(1L, 0L, RENAME, "A", "B"),
        entry(2L, 1L, DELETE, "A/child"));

    List<DiffType> orderedTypes = toDiffTypes(sort(entries));
    assertEquals(Arrays.asList(DELETE, RENAME), orderedTypes);
  }

  @Test
  void testDescendantDeleteBeforeDeepAncestorRenameOnPathPrefix() {
    // file1 at A/B/C/file1 is deleted; ancestor A/B is renamed to D/B; A is
    // deleted. Intermediate directory C is omitted from the report. The delete
    // must precede the ancestor rename because A/B/C/file1 is under A/B.
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(2L, 4L, RENAME, "A/B", "D/B"),
        entry(5L, 4L, DELETE, "A/B/C/file1"),
        entry(1L, 0L, DELETE, "A"));

    List<SnapDiffDependencyEntry> ordered = sort(entries);
    assertBefore(ordered, indexOf(ordered, DELETE, "A/B/C/file1"),
        indexOf(ordered, RENAME, "A/B"));
    assertBefore(ordered, indexOf(ordered, RENAME, "A/B"),
        indexOf(ordered, DELETE, "A"));
  }

  @Test
  void testDescendantDeleteBeforeDeepAncestorDeleteOnPathPrefix() {
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(1L, 0L, DELETE, "A"),
        entry(3L, 2L, DELETE, "A/B/C/file1"));

    List<SnapDiffDependencyEntry> ordered = sort(entries);
    assertBefore(ordered, indexOf(ordered, DELETE, "A/B/C/file1"),
        indexOf(ordered, DELETE, "A"));
  }

  @Test
  void testAncestorCreateBeforeDeepDescendantCreateOnPathPrefix() {
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(3L, 2L, CREATE, "A/B/C"),
        entry(1L, 0L, CREATE, "A"));

    List<SnapDiffDependencyEntry> ordered = sort(entries);
    assertBefore(ordered, indexOf(ordered, CREATE, "A"),
        indexOf(ordered, CREATE, "A/B/C"));
  }

  @Test
  void testDescendantNonDeleteBeforeAncestorDeleteOnPathPrefix() {
    // RENAME A/B -> C/B must precede DELETE A (A is a strict prefix of A/B).
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(1L, 0L, DELETE, "A"),
        entry(2L, 1L, RENAME, "A/B", "C/B"));

    List<DiffType> orderedTypes = toDiffTypes(sort(entries));
    assertEquals(Arrays.asList(RENAME, DELETE), orderedTypes);
  }

  @Test
  void testRenameAfterTargetParentCreatedAndBeforeSourceParentDeleted() {
    // RENAME A/B -> C/B needs CREATE C first (to-snapshot prefix on C/B),
    // then must precede DELETE A (from-snapshot prefix on A/B).
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        renameEntry(2L, 1L, 3L, "A/B", "C/B"),
        entry(3L, 0L, CREATE, "C"),
        entry(1L, 0L, DELETE, "A"));

    List<SnapDiffDependencyEntry> ordered = sort(entries);
    assertBefore(ordered, indexOf(ordered, CREATE, "C"),
        indexOf(ordered, RENAME, "A/B"));
    assertBefore(ordered, indexOf(ordered, RENAME, "A/B"),
        indexOf(ordered, DELETE, "A"));
  }

  @Test
  void testRenameChainFreesPathBeforeReuse() {
    // N (objectId 2) is renamed P2 -> P3, freeing P2. M (objectId 1) is renamed
    // P1 -> P2, occupying P2. The rename that frees P2 must be applied before
    // the rename that occupies it. M's rename is listed first to expose the
    // missing chain edge.
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(1L, 0L, RENAME, "P1", "P2"),
        entry(2L, 0L, RENAME, "P2", "P3"));

    List<SnapDiffDependencyEntry> ordered = sort(entries);
    assertBefore(ordered, indexOf(ordered, RENAME, "P2"),
        indexOf(ordered, RENAME, "P1"));
  }

  @Test
  void testGetOrderedEntriesIsIdempotent() {
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(2L, 1L, CREATE, "parent/child"),
        entry(1L, 0L, CREATE, "parent"));

    SnapDiffDependencyGraph graph = new SnapDiffDependencyGraph(entries);
    List<SnapDiffDependencyEntry> first = graph.getOrderedEntries();
    List<SnapDiffDependencyEntry> second = graph.getOrderedEntries();

    assertEquals(first, second);
    assertEquals(Arrays.asList(CREATE, CREATE), toDiffTypes(second));
    assertEquals("parent", second.get(0).getSourcePath());
    assertEquals("parent/child", second.get(1).getSourcePath());
  }

  @Test
  void testModifyAndRenameForSameObjectKeepDependencyOrder() {
    // CREATE parent via to-snapshot prefix; MODIFY/RENAME order via intra-object rule.
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(2L, 1L, MODIFY, "parent/child"),
        entry(2L, 1L, RENAME, "parent/old-child", "parent/child"),
        entry(1L, 0L, CREATE, "parent"));

    List<SnapDiffDependencyEntry> ordered = sort(entries);
    assertEquals(CREATE, ordered.get(0).getDiffType());
    assertEquals("parent", ordered.get(0).getSourcePath());
    assertTrue(ordered.subList(1, 3).stream()
        .noneMatch(SnapDiffDependencyEntry::isDelete));
    assertTrue(ordered.subList(1, 3).stream()
        .map(SnapDiffDependencyEntry::getObjectId)
        .allMatch(objectId -> objectId == 2L));
  }

  @Test
  void testModifyAtSourcePathOrderedBeforeRename() {
    // Same objectId: MODIFY at the RENAME source path must precede the RENAME.
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(2L, 1L, RENAME, "parent/a.txt", "parent/b.txt"),
        entry(2L, 1L, MODIFY, "parent/a.txt"));

    List<DiffType> orderedTypes = toDiffTypes(sort(entries));
    assertEquals(Arrays.asList(MODIFY, RENAME), orderedTypes);
  }

  @Test
  void testTopologicalSortDetectsCycle() {
    // Cross-object path swap: each RENAME must precede the other (rename chain).
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(1L, 0L, RENAME, "A", "B"),
        entry(2L, 0L, RENAME, "B", "A"));

    assertThrows(IllegalStateException.class,
        () -> new SnapDiffDependencyGraph(entries).getOrderedEntries());
  }

  @Test
  void testToOrderedReportEntries() {
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(2L, 1L, CREATE, "parent/child"),
        entry(1L, 0L, CREATE, "parent"));

    List<DiffReportEntry> orderedEntries = SnapDiffDependencyGraph
        .toOrderedReportEntries(new SnapDiffDependencyGraph(entries).getOrderedEntries());

    assertEquals(2, orderedEntries.size());
    assertEquals(CREATE, orderedEntries.get(0).getType());
    assertEquals("parent",
        new String(orderedEntries.get(0).getSourcePath(), StandardCharsets.UTF_8));
    assertEquals("parent/child",
        new String(orderedEntries.get(1).getSourcePath(), StandardCharsets.UTF_8));
  }

  private static List<SnapDiffDependencyEntry> sort(
      List<SnapDiffDependencyEntry> entries) {
    return new SnapDiffDependencyGraph(entries).getOrderedEntries();
  }

  private static SnapDiffDependencyEntry entry(long objectId, long parentObjectId,
      DiffType diffType, String sourcePath) {
    return new SnapDiffDependencyEntry(objectId, parentObjectId,
        getDiffReportEntry(diffType, sourcePath));
  }

  private static SnapDiffDependencyEntry entry(long objectId, long parentObjectId,
      DiffType diffType, String sourcePath, String targetPath) {
    return new SnapDiffDependencyEntry(objectId, parentObjectId,
        getDiffReportEntry(diffType, sourcePath, targetPath));
  }

  private static SnapDiffDependencyEntry renameEntry(long objectId,
      long sourceParentObjectId, long targetParentObjectId, String sourcePath,
      String targetPath) {
    return new SnapDiffDependencyEntry(objectId, sourceParentObjectId,
        targetParentObjectId, getDiffReportEntry(RENAME, sourcePath, targetPath));
  }

  private static int indexOf(List<SnapDiffDependencyEntry> ordered,
      DiffType diffType, String sourcePath) {
    for (int i = 0; i < ordered.size(); i++) {
      SnapDiffDependencyEntry entry = ordered.get(i);
      if (entry.getDiffType() == diffType
          && sourcePath.equals(entry.getSourcePath())) {
        return i;
      }
    }
    throw new AssertionError(
        "Entry not found: " + diffType + " " + sourcePath);
  }

  private static void assertBefore(List<SnapDiffDependencyEntry> ordered,
      int firstIndex, int secondIndex) {
    assertTrue(firstIndex < secondIndex,
        "Expected index " + firstIndex + " before " + secondIndex
            + " in " + toDiffTypes(ordered));
  }

  private static List<DiffType> toDiffTypes(
      List<SnapDiffDependencyEntry> entries) {
    return entries.stream()
        .map(SnapDiffDependencyEntry::getDiffType)
        .collect(Collectors.toList());
  }

  private static void assertPathOrder(List<SnapDiffDependencyEntry> entries,
      String firstPath, String secondPath) {
    List<SnapDiffDependencyEntry> ordered = sort(entries);
    assertEquals(firstPath, ordered.get(0).getSourcePath());
    assertEquals(secondPath, ordered.get(1).getSourcePath());
  }
}
