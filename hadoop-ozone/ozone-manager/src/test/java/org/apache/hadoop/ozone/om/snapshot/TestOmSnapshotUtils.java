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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.utils.IOUtils.getINode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
  }
}
