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
  void testParentCreateBeforeChildCreate() {
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(2L, 1L, CREATE, "parent/child"),
        entry(1L, 0L, CREATE, "parent"));

    List<DiffType> orderedTypes = toDiffTypes(sort(entries));
    assertEquals(Arrays.asList(CREATE, CREATE), orderedTypes);
    assertPathOrder(entries, "parent", "parent/child");
  }

  @Test
  void testChildDeleteBeforeParentDelete() {
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
  void testRenameBeforeDeleteParentDirectory() {
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(1L, 0L, DELETE, "A"),
        entry(2L, 1L, RENAME, "A/B", "C/B"));

    List<DiffType> orderedTypes = toDiffTypes(sort(entries));
    assertEquals(Arrays.asList(RENAME, DELETE), orderedTypes);
  }

  @Test
  void testModifyAndRenameForSameObjectKeepDependencyOrder() {
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
    // Object 2 is both modified and renamed. MODIFY is reported at the
    // from-snapshot (pre-rename) path "parent/a.txt", and RENAME moves that
    // same path to "parent/b.txt". The content change must be applied while
    // the path is still "parent/a.txt", so MODIFY must precede RENAME.
    // RENAME is listed first to expose the missing intra-object edge.
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(2L, 1L, RENAME, "parent/a.txt", "parent/b.txt"),
        entry(2L, 1L, MODIFY, "parent/a.txt"));

    List<DiffType> orderedTypes = toDiffTypes(sort(entries));
    assertEquals(Arrays.asList(MODIFY, RENAME), orderedTypes);
  }

  @Test
  void testTopologicalSortDetectsCycle() {
    List<SnapDiffDependencyEntry> entries = Arrays.asList(
        entry(1L, 2L, CREATE, "a"),
        entry(2L, 1L, CREATE, "b"));

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
