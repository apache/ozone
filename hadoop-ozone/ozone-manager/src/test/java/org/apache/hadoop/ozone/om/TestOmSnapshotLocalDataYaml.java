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
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData.VersionMeta;
import org.apache.hadoop.ozone.util.ObjectSerializer;
import org.apache.hadoop.ozone.util.YamlSerializer;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.junit.jupiter.api.AfterAll;
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
  private static final OmSnapshotLocalDataYaml.YamlFactory YAML_FACTORY = new OmSnapshotLocalDataYaml.YamlFactory();
  private static ObjectSerializer<OmSnapshotLocalData> omSnapshotLocalDataSerializer;

  private static final Instant NOW = Instant.now();

  @BeforeAll
  public static void setupSerializer() throws IOException {
    omSnapshotLocalDataSerializer = new YamlSerializer<OmSnapshotLocalData>(YAML_FACTORY) {
      @Override
      public void computeAndSetChecksum(Yaml yaml, OmSnapshotLocalData data) throws IOException {
        data.computeAndSetChecksum(yaml);
      }
    };
  }

  @AfterAll
  public static void cleanupSerializer() throws IOException {
    if (omSnapshotLocalDataSerializer != null) {
      omSnapshotLocalDataSerializer.close();
    }
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
  private Pair<File, UUID> writeToYaml(UUID snapshotId, String snapshotName, TransactionInfo transactionInfo)
      throws IOException {
    String yamlFilePath = snapshotName + ".yaml";
    UUID previousSnapshotId = UUID.randomUUID();
    // Create snapshot data with not defragged SST files
    List<LiveFileMetaData> notDefraggedSSTFileList = asList(
        createLiveFileMetaData("sst1", "table1", "k1", "k2"),
        createLiveFileMetaData("sst2", "table1", "k3", "k4"),
        createLiveFileMetaData("sst3", "table2", "k4", "k5"));
    OmSnapshotLocalData dataYaml = new OmSnapshotLocalData(snapshotId, notDefraggedSSTFileList,
        previousSnapshotId, transactionInfo, 10);

    // Set version
    dataYaml.setVersion(42);
    // Set SST filtered flag
    dataYaml.setSstFiltered(true);

    // Set last defrag time
    dataYaml.setLastDefragTime(NOW.toEpochMilli());

    // Set needs defrag flag
    dataYaml.setNeedsDefrag(true);

    // Add some defragged SST files
    dataYaml.addVersionSSTFileInfos(ImmutableList.of(
        createLiveFileMetaData("defragged-sst1", "table1", "k1", "k2"),
        createLiveFileMetaData("defragged-sst2", "table2", "k3", "k4")),
        1);
    dataYaml.addVersionSSTFileInfos(Collections.singletonList(
        createLiveFileMetaData("defragged-sst3", "table1", "k4", "k5")), 3);

    File yamlFile = new File(testRoot, yamlFilePath);

    // Create YAML file with SnapshotData
    omSnapshotLocalDataSerializer.save(yamlFile, dataYaml);

    // Check YAML file exists
    assertTrue(yamlFile.exists());

    return Pair.of(yamlFile, previousSnapshotId);
  }

  @Test
  public void testWriteToYaml() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    TransactionInfo transactionInfo = TransactionInfo.valueOf(ThreadLocalRandom.current().nextLong(),
        ThreadLocalRandom.current().nextLong());
    Pair<File, UUID> yamlFilePrevIdPair = writeToYaml(snapshotId, "snapshot1", transactionInfo);
    File yamlFile = yamlFilePrevIdPair.getLeft();
    UUID prevSnapId = yamlFilePrevIdPair.getRight();

    // Read from YAML file
    OmSnapshotLocalData snapshotData = omSnapshotLocalDataSerializer.load(yamlFile);

    // Verify fields
    assertEquals(44, snapshotData.getVersion());
    assertEquals(10, snapshotData.getDbTxSequenceNumber());
    assertTrue(snapshotData.getSstFiltered());
    assertEquals(transactionInfo, snapshotData.getTransactionInfo());

    VersionMeta notDefraggedSSTFiles = snapshotData.getVersionSstFileInfos().get(0);
    assertEquals(new VersionMeta(0,
        ImmutableList.of(new SstFileInfo("sst1", "k1", "k2", "table1"),
            new SstFileInfo("sst2", "k3", "k4", "table1"),
            new SstFileInfo("sst3", "k4", "k5", "table2"))), notDefraggedSSTFiles);
    assertEquals(NOW.toEpochMilli(), snapshotData.getLastDefragTime());
    assertTrue(snapshotData.getNeedsDefrag());

    Map<Integer, VersionMeta> defraggedSSTFiles = snapshotData.getVersionSstFileInfos();
    assertEquals(3, defraggedSSTFiles.size());
    assertTrue(defraggedSSTFiles.containsKey(43));
    assertTrue(defraggedSSTFiles.containsKey(44));
    assertEquals(2, defraggedSSTFiles.get(43).getSstFiles().size());
    assertEquals(1, defraggedSSTFiles.get(44).getSstFiles().size());
    assertEquals(prevSnapId, snapshotData.getPreviousSnapshotId());
    assertEquals(snapshotId, snapshotData.getSnapshotId());
    assertEquals(ImmutableMap.of(
        0, new VersionMeta(0,
            ImmutableList.of(new SstFileInfo("sst1", "k1", "k2", "table1"),
                new SstFileInfo("sst2", "k3", "k4", "table1"),
                new SstFileInfo("sst3", "k4", "k5", "table2"))),
        43, new VersionMeta(1,
            ImmutableList.of(new SstFileInfo("defragged-sst1", "k1", "k2", "table1"),
                new SstFileInfo("defragged-sst2", "k3", "k4", "table2"))),
        44, new VersionMeta(3,
            ImmutableList.of(new SstFileInfo("defragged-sst3", "k4", "k5", "table1")))), defraggedSSTFiles);
  }

  @Test
  public void testUpdateSnapshotDataFile() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    Pair<File, UUID> yamlFilePrevIdPair = writeToYaml(snapshotId, "snapshot2", null);
    File yamlFile = yamlFilePrevIdPair.getLeft();
    // Read from YAML file
    OmSnapshotLocalData dataYaml =
        omSnapshotLocalDataSerializer.load(yamlFile);
    TransactionInfo transactionInfo = TransactionInfo.valueOf(ThreadLocalRandom.current().nextLong(),
        ThreadLocalRandom.current().nextLong());
    // Update snapshot data
    dataYaml.setSstFiltered(false);
    dataYaml.setNeedsDefrag(false);
    dataYaml.addVersionSSTFileInfos(
        singletonList(createLiveFileMetaData("defragged-sst4", "table3", "k5", "k6")), 5);
    dataYaml.setTransactionInfo(transactionInfo);

    // Write updated data back to file
    omSnapshotLocalDataSerializer.save(yamlFile, dataYaml);

    // Read back the updated data
    dataYaml = omSnapshotLocalDataSerializer.load(yamlFile);

    // Verify updated data
    assertThat(dataYaml.getSstFiltered()).isFalse();
    assertThat(dataYaml.getNeedsDefrag()).isFalse();
    assertEquals(transactionInfo, dataYaml.getTransactionInfo());

    Map<Integer, VersionMeta> defraggedFiles = dataYaml.getVersionSstFileInfos();
    assertEquals(4, defraggedFiles.size());
    assertTrue(defraggedFiles.containsKey(45));
    assertEquals(new VersionMeta(5, ImmutableList.of(new SstFileInfo("defragged-sst4", "k5", "k6", "table3"))),
        defraggedFiles.get(45));
  }

  @Test
  public void testEmptyFile() throws IOException {
    File emptyFile = new File(testRoot, "empty.yaml");
    assertTrue(emptyFile.createNewFile());

    IOException ex = assertThrows(IOException.class, () -> omSnapshotLocalDataSerializer.load(emptyFile));

    assertThat(ex).hasMessageContaining("Failed to load file. File is empty.");
  }

  @Test
  public void testChecksum() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    TransactionInfo transactionInfo = TransactionInfo.valueOf(ThreadLocalRandom.current().nextLong(),
        ThreadLocalRandom.current().nextLong());
    Pair<File, UUID> yamlFilePrevIdPair = writeToYaml(snapshotId, "snapshot3", transactionInfo);
    File yamlFile = yamlFilePrevIdPair.getLeft();
    // Read from YAML file
    OmSnapshotLocalData snapshotData = omSnapshotLocalDataSerializer.load(yamlFile);

    // Get the original checksum
    String originalChecksum = snapshotData.getChecksum();

    // Verify the checksum is not null or empty
    assertThat(originalChecksum).isNotNull().isNotEmpty();

    assertTrue(omSnapshotLocalDataSerializer.verifyChecksum(snapshotData));
  }

  @Test
  public void testYamlContainsAllFields() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    TransactionInfo transactionInfo = TransactionInfo.valueOf(ThreadLocalRandom.current().nextLong(),
        ThreadLocalRandom.current().nextLong());
    Pair<File, UUID> yamlFilePrevIdPair = writeToYaml(snapshotId, "snapshot4", transactionInfo);
    File yamlFile = yamlFilePrevIdPair.getLeft();
    String content = FileUtils.readFileToString(yamlFile, Charset.defaultCharset());

    // Verify the YAML content contains all expected fields
    assertThat(content).contains(OzoneConsts.OM_SLD_VERSION);
    assertThat(content).contains(OzoneConsts.OM_SLD_CHECKSUM);
    assertThat(content).contains(OzoneConsts.OM_SLD_IS_SST_FILTERED);
    assertThat(content).contains(OzoneConsts.OM_SLD_LAST_DEFRAG_TIME);
    assertThat(content).contains(OzoneConsts.OM_SLD_NEEDS_DEFRAG);
    assertThat(content).contains(OzoneConsts.OM_SLD_VERSION_SST_FILE_INFO);
    assertThat(content).contains(OzoneConsts.OM_SLD_SNAP_ID);
    assertThat(content).contains(OzoneConsts.OM_SLD_PREV_SNAP_ID);
    assertThat(content).contains(OzoneConsts.OM_SLD_TXN_INFO);
  }
}
