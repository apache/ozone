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

package org.apache.hadoop.ozone.om;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class tests creating and reading snapshot data YAML files.
 */
public class TestOmSnapshotLocalDataYaml {

  private static String testRoot = new FileSystemTestHelper().getTestRootDir();

  private static final Instant NOW = Instant.now();

  @BeforeEach
  public void setUp() {
    assertTrue(new File(testRoot).mkdirs());
  }

  @AfterEach
  public void cleanup() {
    FileUtil.fullyDelete(new File(testRoot));
  }

  /**
   * Creates a snapshot local data YAML file.
   */
  private File writeToYaml(String snapshotName) throws IOException {
    String yamlFilePath = snapshotName + ".yaml";

    OmSnapshotLocalDataYaml dataYaml = new OmSnapshotLocalDataYaml();

    // Set version
    dataYaml.setVersion(42);
    // Set SST filtered flag
    dataYaml.setSstFiltered(true);

    // Add some uncompacted SST files
    dataYaml.addUncompactedSSTFile("table1", "sst1");
    dataYaml.addUncompactedSSTFile("table1", "sst2");
    dataYaml.addUncompactedSSTFile("table2", "sst3");

    // Set last compaction time
    dataYaml.setLastCompactionTime(NOW.toEpochMilli());

    // Set needs compaction flag
    dataYaml.setNeedsCompaction(true);

    // Add some compacted SST files
    dataYaml.addCompactedSSTFile(1, "table1", "compacted-sst1");
    dataYaml.addCompactedSSTFile(1, "table2", "compacted-sst2");
    dataYaml.addCompactedSSTFile(2, "table1", "compacted-sst3");

    File yamlFile = new File(testRoot, yamlFilePath);

    // Create YAML file with SnapshotData
    dataYaml.writeToYaml(yamlFile);

    // Check YAML file exists
    assertTrue(yamlFile.exists());

    return yamlFile;
  }

  @Test
  public void testWriteToYaml() throws IOException {
    File yamlFile = writeToYaml("snapshot1");

    // Read from YAML file
    OmSnapshotLocalDataYaml snapshotData = OmSnapshotLocalDataYaml.getFromYamlFile(yamlFile);

    // Verify fields
    assertEquals(42, snapshotData.getVersion());
    assertTrue(snapshotData.getSstFiltered());

    Map<String, List<String>> uncompactedFiles = snapshotData.getUncompactedSSTFileList();
    assertEquals(2, uncompactedFiles.size());
    assertEquals(2, uncompactedFiles.get("table1").size());
    assertEquals(1, uncompactedFiles.get("table2").size());
    assertTrue(uncompactedFiles.get("table1").contains("sst1"));
    assertTrue(uncompactedFiles.get("table1").contains("sst2"));
    assertTrue(uncompactedFiles.get("table2").contains("sst3"));

    assertEquals(NOW.toEpochMilli(), snapshotData.getLastCompactionTime());
    assertTrue(snapshotData.getNeedsCompaction());

    Map<Integer, Map<String, List<String>>> compactedFiles = snapshotData.getCompactedSSTFileList();
    assertEquals(2, compactedFiles.size());
    assertTrue(compactedFiles.containsKey(1));
    assertTrue(compactedFiles.containsKey(2));
    assertEquals(2, compactedFiles.get(1).size());
    assertEquals(1, compactedFiles.get(2).size());
    assertTrue(compactedFiles.get(1).get("table1").contains("compacted-sst1"));
    assertTrue(compactedFiles.get(1).get("table2").contains("compacted-sst2"));
    assertTrue(compactedFiles.get(2).get("table1").contains("compacted-sst3"));
  }

  @Test
  public void testUpdateSnapshotDataFile() throws IOException {
    File yamlFile = writeToYaml("snapshot2");

    // Read from YAML file
    OmSnapshotLocalDataYaml dataYaml =
        OmSnapshotLocalDataYaml.getFromYamlFile(yamlFile);

    // Update snapshot data
    dataYaml.setSstFiltered(false);
    dataYaml.setNeedsCompaction(false);
    dataYaml.addUncompactedSSTFile("table3", "sst4");
    dataYaml.addCompactedSSTFile(3, "table3", "compacted-sst4");

    // Write updated data back to file
    dataYaml.writeToYaml(yamlFile);

    // Read back the updated data
    dataYaml = OmSnapshotLocalDataYaml.getFromYamlFile(yamlFile);

    // Verify updated data
    assertThat(dataYaml.getSstFiltered()).isFalse();
    assertThat(dataYaml.getNeedsCompaction()).isFalse();

    Map<String, List<String>> uncompactedFiles = dataYaml.getUncompactedSSTFileList();
    assertEquals(3, uncompactedFiles.size());
    assertTrue(uncompactedFiles.containsKey("table3"));
    assertTrue(uncompactedFiles.get("table3").contains("sst4"));

    Map<Integer, Map<String, List<String>>> compactedFiles = dataYaml.getCompactedSSTFileList();
    assertEquals(3, compactedFiles.size());
    assertTrue(compactedFiles.containsKey(3));
    assertTrue(compactedFiles.get(3).containsKey("table3"));
    assertTrue(compactedFiles.get(3).get("table3").contains("compacted-sst4"));
  }

  @Test
  public void testEmptyFile() throws IOException {
    File emptyFile = new File(testRoot, "empty.yaml");
    assertTrue(emptyFile.createNewFile());

    IOException ex = assertThrows(IOException.class, () ->
        OmSnapshotLocalDataYaml.getFromYamlFile(emptyFile));

    assertThat(ex).hasMessageContaining("Failed to load snapshot file. File is empty.");
  }

  @Test
  public void testChecksum() throws IOException {
    File yamlFile = writeToYaml("snapshot3");

    // Read from YAML file
    OmSnapshotLocalDataYaml snapshotData = OmSnapshotLocalDataYaml.getFromYamlFile(yamlFile);

    // Get the original checksum
    String originalChecksum = snapshotData.getChecksum();

    // Verify the checksum is not null or empty
    assertThat(originalChecksum).isNotNull().isNotEmpty();

    assertTrue(OmSnapshotLocalDataYaml.verifyChecksum(snapshotData));
  }

  @Test
  public void testYamlContainsAllFields() throws IOException {
    File yamlFile = writeToYaml("snapshot4");

    String content = FileUtils.readFileToString(yamlFile, Charset.defaultCharset());

    // Verify the YAML content contains all expected fields
    assertThat(content).contains(OzoneConsts.OM_SLD_VERSION);
    assertThat(content).contains(OzoneConsts.OM_SLD_CHECKSUM);
    assertThat(content).contains(OzoneConsts.OM_SLD_IS_SST_FILTERED);
    assertThat(content).contains(OzoneConsts.OM_SLD_UNCOMPACTED_SST_FILE_LIST);
    assertThat(content).contains(OzoneConsts.OM_SLD_LAST_COMPACTION_TIME);
    assertThat(content).contains(OzoneConsts.OM_SLD_NEEDS_COMPACTION);
    assertThat(content).contains(OzoneConsts.OM_SLD_COMPACTED_SST_FILE_LIST);
  }
}
