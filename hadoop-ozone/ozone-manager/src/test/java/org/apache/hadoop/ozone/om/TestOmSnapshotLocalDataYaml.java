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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.StringUtils.string2Bytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData.VersionMeta;
import org.apache.ozone.compaction.log.SstFileInfo;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.LiveFileMetaData;
import org.yaml.snakeyaml.Yaml;

/**
 * This class tests creating and reading snapshot data YAML files.
 */
public class TestOmSnapshotLocalDataYaml {

  private static String testRoot = new FileSystemTestHelper().getTestRootDir();
  private static OmSnapshotManager omSnapshotManager;
  private static final Yaml YAML = new OmSnapshotLocalDataYaml.YamlFactory().create();
  private static final UncheckedAutoCloseableSupplier<Yaml> YAML_SUPPLIER = new UncheckedAutoCloseableSupplier<Yaml>() {
    @Override
    public Yaml get() {
      return YAML;
    }

    @Override
    public void close() {

    }
  };

  private static final Instant NOW = Instant.now();

  @BeforeAll
  public static void setupClassMocks() throws IOException {
    omSnapshotManager = mock(OmSnapshotManager.class);
    when(omSnapshotManager.getSnapshotLocalYaml()).thenReturn(YAML_SUPPLIER);
  }

  @BeforeEach
  public void setUp() {
    assertTrue(new File(testRoot).mkdirs());
  }

  @AfterEach
  public void cleanup() {
    FileUtil.fullyDelete(new File(testRoot));
  }

  private LiveFileMetaData createLiveFileMetaData(String fileName, String table, String smallestKey,
      String largestKey) {
    LiveFileMetaData lfm = mock(LiveFileMetaData.class);
    when(lfm.columnFamilyName()).thenReturn(string2Bytes(table));
    when(lfm.fileName()).thenReturn(fileName);
    when(lfm.smallestKey()).thenReturn(StringUtils.string2Bytes(smallestKey));
    when(lfm.largestKey()).thenReturn(StringUtils.string2Bytes(largestKey));
    return lfm;
  }

  /**
   * Creates a snapshot local data YAML file.
   */
  private Pair<File, UUID> writeToYaml(String snapshotName) throws IOException {
    String yamlFilePath = snapshotName + ".yaml";
    UUID previousSnapshotId = UUID.randomUUID();
    // Create snapshot data with uncompacted SST files
    List<LiveFileMetaData> uncompactedSSTFileList = asList(
        createLiveFileMetaData("sst1", "table1", "k1", "k2"),
        createLiveFileMetaData("sst2", "table1", "k3", "k4"),
        createLiveFileMetaData("sst3", "table2", "k4", "k5"));
    OmSnapshotLocalDataYaml dataYaml = new OmSnapshotLocalDataYaml(uncompactedSSTFileList, previousSnapshotId);

    // Set version
    dataYaml.setVersion(42);
    // Set SST filtered flag
    dataYaml.setSstFiltered(true);

    // Set last compaction time
    dataYaml.setLastCompactionTime(NOW.toEpochMilli());

    // Set needs compaction flag
    dataYaml.setNeedsCompaction(true);

    // Add some compacted SST files
    dataYaml.addVersionSSTFileInfos(ImmutableList.of(
        new SstFileInfo("compacted-sst1", "k1", "k2", "table1"),
        new SstFileInfo("compacted-sst2", "k3", "k4", "table2")),
        1);
    dataYaml.addVersionSSTFileInfos(Collections.singletonList(
        new SstFileInfo("compacted-sst3", "k4", "k5", "table1")), 3);

    File yamlFile = new File(testRoot, yamlFilePath);

    // Create YAML file with SnapshotData
    dataYaml.writeToYaml(omSnapshotManager, yamlFile);

    // Check YAML file exists
    assertTrue(yamlFile.exists());

    return Pair.of(yamlFile, previousSnapshotId);
  }

  @Test
  public void testWriteToYaml() throws IOException {
    Pair<File, UUID> yamlFilePrevIdPair = writeToYaml("snapshot1");
    File yamlFile = yamlFilePrevIdPair.getLeft();
    UUID prevSnapId = yamlFilePrevIdPair.getRight();

    // Read from YAML file
    OmSnapshotLocalDataYaml snapshotData = OmSnapshotLocalDataYaml.getFromYamlFile(omSnapshotManager, yamlFile);

    // Verify fields
    assertEquals(44, snapshotData.getVersion());
    assertTrue(snapshotData.getSstFiltered());

    VersionMeta uncompactedFiles = snapshotData.getVersionSstFileInfos().get(0);
    assertEquals(new VersionMeta(0,
        ImmutableList.of(new SstFileInfo("sst1", "k1", "k2", "table1"),
            new SstFileInfo("sst2", "k3", "k4", "table1"),
            new SstFileInfo("sst3", "k4", "k5", "table2"))), uncompactedFiles);
    assertEquals(NOW.toEpochMilli(), snapshotData.getLastCompactionTime());
    assertTrue(snapshotData.getNeedsCompaction());

    Map<Integer, VersionMeta> compactedFiles = snapshotData.getVersionSstFileInfos();
    assertEquals(3, compactedFiles.size());
    assertTrue(compactedFiles.containsKey(43));
    assertTrue(compactedFiles.containsKey(44));
    assertEquals(2, compactedFiles.get(43).getSstFiles().size());
    assertEquals(1, compactedFiles.get(44).getSstFiles().size());
    assertEquals(prevSnapId, snapshotData.getPreviousSnapshotId());
    assertEquals(ImmutableMap.of(
        0, new VersionMeta(0,
            ImmutableList.of(new SstFileInfo("sst1", "k1", "k2", "table1"),
                new SstFileInfo("sst2", "k3", "k4", "table1"),
                new SstFileInfo("sst3", "k4", "k5", "table2"))),
        43, new VersionMeta(1,
            ImmutableList.of(new SstFileInfo("compacted-sst1", "k1", "k2", "table1"),
                new SstFileInfo("compacted-sst2", "k3", "k4", "table2"))),
        44, new VersionMeta(3,
            ImmutableList.of(new SstFileInfo("compacted-sst3", "k4", "k5", "table1")))), compactedFiles);
  }

  @Test
  public void testUpdateSnapshotDataFile() throws IOException {
    Pair<File, UUID> yamlFilePrevIdPair = writeToYaml("snapshot2");
    File yamlFile = yamlFilePrevIdPair.getLeft();
    // Read from YAML file
    OmSnapshotLocalDataYaml dataYaml =
        OmSnapshotLocalDataYaml.getFromYamlFile(omSnapshotManager, yamlFile);

    // Update snapshot data
    dataYaml.setSstFiltered(false);
    dataYaml.setNeedsCompaction(false);
    dataYaml.addVersionSSTFileInfos(
        singletonList(new SstFileInfo("compacted-sst4", "k5", "k6", "table3")), 5);

    // Write updated data back to file
    dataYaml.writeToYaml(omSnapshotManager, yamlFile);

    // Read back the updated data
    dataYaml = OmSnapshotLocalDataYaml.getFromYamlFile(omSnapshotManager, yamlFile);

    // Verify updated data
    assertThat(dataYaml.getSstFiltered()).isFalse();
    assertThat(dataYaml.getNeedsCompaction()).isFalse();

    Map<Integer, VersionMeta> compactedFiles = dataYaml.getVersionSstFileInfos();
    assertEquals(4, compactedFiles.size());
    assertTrue(compactedFiles.containsKey(45));
    assertEquals(new VersionMeta(5, ImmutableList.of(new SstFileInfo("compacted-sst4", "k5", "k6", "table3"))),
        compactedFiles.get(45));
  }

  @Test
  public void testEmptyFile() throws IOException {
    File emptyFile = new File(testRoot, "empty.yaml");
    assertTrue(emptyFile.createNewFile());

    IOException ex = assertThrows(IOException.class, () ->
        OmSnapshotLocalDataYaml.getFromYamlFile(omSnapshotManager, emptyFile));

    assertThat(ex).hasMessageContaining("Failed to load snapshot file. File is empty.");
  }

  @Test
  public void testChecksum() throws IOException {
    Pair<File, UUID> yamlFilePrevIdPair = writeToYaml("snapshot3");
    File yamlFile = yamlFilePrevIdPair.getLeft();
    // Read from YAML file
    OmSnapshotLocalDataYaml snapshotData = OmSnapshotLocalDataYaml.getFromYamlFile(omSnapshotManager, yamlFile);

    // Get the original checksum
    String originalChecksum = snapshotData.getChecksum();

    // Verify the checksum is not null or empty
    assertThat(originalChecksum).isNotNull().isNotEmpty();

    assertTrue(OmSnapshotLocalDataYaml.verifyChecksum(omSnapshotManager, snapshotData));
  }

  @Test
  public void testYamlContainsAllFields() throws IOException {
    Pair<File, UUID> yamlFilePrevIdPair = writeToYaml("snapshot4");
    File yamlFile = yamlFilePrevIdPair.getLeft();
    String content = FileUtils.readFileToString(yamlFile, Charset.defaultCharset());

    // Verify the YAML content contains all expected fields
    assertThat(content).contains(OzoneConsts.OM_SLD_VERSION);
    assertThat(content).contains(OzoneConsts.OM_SLD_CHECKSUM);
    assertThat(content).contains(OzoneConsts.OM_SLD_IS_SST_FILTERED);
    assertThat(content).contains(OzoneConsts.OM_SLD_LAST_COMPACTION_TIME);
    assertThat(content).contains(OzoneConsts.OM_SLD_NEEDS_COMPACTION);
    assertThat(content).contains(OzoneConsts.OM_SLD_VERSION_SST_FILE_INFO);
  }
}
