/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.snapshot;

import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.service.SnapshotDeletingService;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.getINode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
/**
 * Class to test snapshot utilities.
 */
public class TestOmSnapshotUtils {

  /**
   * Test linkFiles().
   */
  @Test
  public void testLinkFiles(@TempDir File tempDir) throws Exception {

    // Create the tree to link from
    File dir1 = new File(tempDir, "tree1/dir1");
    File dir2 = new File(tempDir, "tree1/dir2");
    File tree1 = new File(tempDir, "tree1");
    assertTrue(dir1.mkdirs());
    assertTrue(dir2.mkdirs());
    File f1 = new File(tempDir, "tree1/dir1/f1");
    Files.write(f1.toPath(), "dummyData".getBytes(UTF_8));

    // Create pointers to expected files/links.
    File tree2 = new File(tempDir, "tree2");
    File f1Link = new File(tempDir, "tree2/dir1/f1");

    // Expected files/links shouldn't exist yet.
    assertFalse(tree2.exists());
    assertFalse(f1Link.exists());

    OmSnapshotUtils.linkFiles(tree1, tree2);

    // Expected files/links should exist now.
    assertTrue(tree2.exists());
    assertTrue(f1Link.exists());
    assertEquals(getINode(f1.toPath()), getINode(f1Link.toPath()));

    Set<String> tree1Files = Files.walk(tree1.toPath()).
        map(Path::toString).
        map((s) -> s.replace("tree1", "tree2")).
        collect(Collectors.toSet());
    Set<String> tree2Files = Files.walk(tree2.toPath()).
        map(Path::toString).collect(Collectors.toSet());

    assertEquals(tree1Files, tree2Files);
    GenericTestUtils.deleteDirectory(tempDir);
  }


  private static Stream<Arguments> testCasesForIgnoreSnapshotGc() {
    SnapshotInfo filteredSnapshot =
        SnapshotInfo.newBuilder().setSstFiltered(true).setName("snap1").build();
    SnapshotInfo unFilteredSnapshot =
        SnapshotInfo.newBuilder().setSstFiltered(false).setName("snap1")
            .build();
    // {IsSnapshotFiltered,isSnapshotDeleted,IsSstServiceEnabled = ShouldIgnore}
    return Stream.of(Arguments.of(filteredSnapshot,
            SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED, true, false),
        Arguments.of(filteredSnapshot,
            SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE, true, true),
        Arguments.of(unFilteredSnapshot,
            SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED, true, true),
        Arguments.of(unFilteredSnapshot,
            SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE, true, true),
        Arguments.of(filteredSnapshot,
            SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED, false, false),
        Arguments.of(unFilteredSnapshot,
            SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED, false, false),
        Arguments.of(unFilteredSnapshot,
            SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE, false, true),
        Arguments.of(filteredSnapshot,
            SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE, false, true));
  }

  @ParameterizedTest
  @MethodSource("testCasesForIgnoreSnapshotGc")
  public void testProcessSnapshotLogicInSDS(SnapshotInfo snapshotInfo,
      SnapshotInfo.SnapshotStatus status, boolean isSstFilteringSvcEnabled,
      boolean expectedOutcome) {
    snapshotInfo.setSnapshotStatus(status);
    assertEquals(expectedOutcome,
        SnapshotDeletingService.shouldIgnoreSnapshot(snapshotInfo,
            isSstFilteringSvcEnabled));
  }

}
