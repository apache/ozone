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

package org.apache.hadoop.ozone.om.snapshot.defrag;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.utils.IOUtils.getINode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import org.apache.hadoop.hdds.utils.db.InodeMetadataRocksDBCheckpoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Class to test InodeMetadataRocksDBCheckpoint.
 */
public class TestInodeMetadataRocksDBCheckpoint {

  /**
   * Test createHardLinks().
   */
  @Test
  public void testCreateHardLinksWithOmDbPrefix(@TempDir File tempDir) throws Exception {
    // Create a test dir inside temp dir
    File testDir  = new File(tempDir, "testDir");
    assertTrue(testDir.mkdir(), "Failed to create test dir");
    // Create source file
    File sourceFile = new File(testDir, "source.sst");
    Files.write(sourceFile.toPath(), "test content".getBytes(UTF_8));

    // Create hardlink file with "om.db/" prefixed paths
    File hardlinkFile = new File(testDir, "hardLinkFile"); // OmSnapshotManager.OM_HARDLINK_FILE
    String hardlinkContent =
        "om.db/target1.sst\tsource.sst\n" +
            "target2.sst\tsource.sst\n";
    Files.write(hardlinkFile.toPath(), hardlinkContent.getBytes(UTF_8));
    Object sourceFileInode = getINode(sourceFile.toPath());
    // Execute createHardLinks
    InodeMetadataRocksDBCheckpoint obtainedCheckpoint =
        new InodeMetadataRocksDBCheckpoint(testDir.toPath());
    assertNotNull(obtainedCheckpoint);

    // Verify hard links created correctly
    File target1 = new File(testDir, "om.db/target1.sst");
    File target2 = new File(testDir, "target2.sst");

    assertTrue(target1.exists(),
        "Hard link should be created");
    assertTrue(target2.exists(),
        "Hard link should be created");

    // Verify content is same using inode comparison
    assertEquals(sourceFileInode, getINode(target1.toPath()),
        "Hard links should have same inode as source");
    assertEquals(sourceFileInode, getINode(target2.toPath()),
        "Hard links should have same inode as source");
  }
}
