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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.getINode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Class to test annotation based interceptor that checks whether
 * Ozone snapshot feature is enabled.
 */
public class TestOmSnapshotUtils {

  /**
   * Check Aspect implementation with SnapshotFeatureEnabledUtil.
   */
  @Test
  public void testLinkFiles(@TempDir File tempdir) throws Exception {
    File dir1 = new File(tempdir, "tree1/dir1");
    File dir2 = new File(tempdir, "tree1/dir2");
    File tree1 = new File(tempdir, "tree1");
    File tree2 = new File(tempdir, "tree2");
    assertTrue(dir1.mkdirs());
    assertTrue(dir2.mkdirs());
    File f1 = new File(tempdir, "tree1/dir1/f1");
    Files.write(f1.toPath(), "dummyData".getBytes(UTF_8));
    File f1Link = new File(tempdir, "tree2/dir1/f1");

    OmSnapshotUtils.linkFiles(tree1, tree2);

    Set<String> tree1Files = Files.walk(tree1.toPath()).
        map(Path::toString).
        map((s) -> s.replace("tree1", "tree2")).
        collect(Collectors.toSet());
    Set<String> tree2Files = Files.walk(tree2.toPath()).
        map(Path::toString).collect(Collectors.toSet());

    assertEquals(tree1Files, tree2Files);
    assertEquals(getINode(f1.toPath()), getINode(f1Link.toPath()));
  }
}
