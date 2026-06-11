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

package org.apache.hadoop.ozone.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.GenericPartitionFieldSummary;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Testing path rewrite of iceberg table metadata files.
 */
class TestRewriteTablePathOzoneAction {

  private static final int COMMITS = 4;
  private static final String TABLE_NAME = "test_table";

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "c1", Types.IntegerType.get()),
      Types.NestedField.required(2, "c2", Types.StringType.get()),
      Types.NestedField.optional(3, "c3", Types.StringType.get()));

  private String sourcePrefix = null;
  private String targetPrefix = null;
  private Table table = null;

  @TempDir
  private Path tableDir;
  @TempDir
  private Path targetDir;
  @TempDir
  private Path stagingDir;

  @BeforeEach
  public void setupTableLocation() {
    String tableLocation = tableDir.toUri().toString().replaceFirst("^file:///", "file:/") + TABLE_NAME;
    this.table = createTable(tableLocation + "/");
    this.sourcePrefix = tableLocation;
    this.targetPrefix = targetDir.toUri().toString().replaceFirst("^file:///", "file:/") + TABLE_NAME;
  }

  @Test
  void fullTablePathRewrite() throws Exception {
    RewriteTablePath.Result result = new RewriteTablePathOzoneAction(table)
        .rewriteLocationPrefix(sourcePrefix, targetPrefix)
        .stagingLocation(stagingDir.toString() + "/")
        .execute();

    List<String> metadataPaths = metadataLogEntryPaths(table);
    Set<String> expectedTargets = new HashSet<>();
    for (String path : metadataPaths) {
      expectedTargets.add(RewriteTablePathUtil.newPath(path, sourcePrefix, targetPrefix));
    }

    Set<Pair<String, String>> csvPairs = readCsvPairs(table, result.fileListLocation());
    Set<String> actualTargets = csvPairs.stream().map(Pair::second).collect(Collectors.toSet());
    assertTrue(actualTargets.containsAll(expectedTargets),
        "Copy plan should contain all expected version file targets");

    // Verify all internal paths inside each staged metadata file are rewritten to target.
    assertAllInternalPathsRewritten(csvPairs, targetPrefix);
  }

  @Test
  void tablePathRewriteForStartAndNoEndVersionProvided() throws Exception {
    List<String> metadataPaths = metadataLogEntryPaths(table);
    String startName = RewriteTablePathUtil.fileName(metadataPaths.get(2));

    RewriteTablePath.Result result = new RewriteTablePathOzoneAction(table)
        .rewriteLocationPrefix(sourcePrefix, targetPrefix)
        .stagingLocation(stagingDir.toString() + "/")
        .startVersion(startName)
        .execute();

    List<String> expectedPaths = new ArrayList<>();
    for (int i = metadataPaths.size() - 1; i >= 3; i--) {
      expectedPaths.add(metadataPaths.get(i));
    }

    Set<String> expectedTargets = new HashSet<>();
    for (String versionPath : expectedPaths) {
      expectedTargets.add(RewriteTablePathUtil.newPath(versionPath, sourcePrefix, targetPrefix));
    }

    Set<Pair<String, String>> csvPairs = readCsvPairs(table, result.fileListLocation());
    Set<String> actualTargets = csvPairs.stream().map(Pair::second).collect(Collectors.toSet());
    assertTrue(actualTargets.containsAll(expectedTargets),
        "Copy plan should contain all expected version file targets");

    // Verify all internal paths inside each staged metadata file are rewritten to target
    assertAllInternalPathsRewritten(csvPairs, targetPrefix);
  }

  @Test
  void tablePathRewriteForOnlyEndVersionProvided() throws Exception {
    List<String> metadataPaths = metadataLogEntryPaths(table);
    String endName = RewriteTablePathUtil.fileName(metadataPaths.get(2));

    RewriteTablePath.Result result = new RewriteTablePathOzoneAction(table)
        .rewriteLocationPrefix(sourcePrefix, targetPrefix)
        .stagingLocation(stagingDir.toString() + "/")
        .endVersion(endName)
        .execute();

    List<String> expectedPaths = new ArrayList<>();
    for (int i = 2; i >= 0; i--) {
      expectedPaths.add(metadataPaths.get(i));
    }

    Set<String> expectedTargets = new HashSet<>();
    for (String versionPath : expectedPaths) {
      expectedTargets.add(RewriteTablePathUtil.newPath(versionPath, sourcePrefix, targetPrefix));
    }

    Set<Pair<String, String>> csvPairs = readCsvPairs(table, result.fileListLocation());
    Set<String> actualTargets = csvPairs.stream().map(Pair::second).collect(Collectors.toSet());
    assertTrue(actualTargets.containsAll(expectedTargets),
        "Copy plan should contain all expected version file targets");

    // Verify all internal paths inside each staged metadata file are rewritten to target
    assertAllInternalPathsRewritten(csvPairs, targetPrefix);
  }

  @Test
  void tablePathRewriteForStartAndEndVersionProvided() throws Exception {
    List<String> metadataPaths = metadataLogEntryPaths(table);
    String startName = RewriteTablePathUtil.fileName(metadataPaths.get(1));
    String endName = RewriteTablePathUtil.fileName(metadataPaths.get(3));

    RewriteTablePath.Result result = new RewriteTablePathOzoneAction(table)
        .rewriteLocationPrefix(sourcePrefix, targetPrefix)
        .stagingLocation(stagingDir.toString() + "/")
        .startVersion(startName)
        .endVersion(endName)
        .execute();

    List<String> expectedPaths = new ArrayList<>();
    for (int i = 3; i >= 2; i--) {
      expectedPaths.add(metadataPaths.get(i));
    }

    Set<String> expectedTargets = new HashSet<>();
    for (String versionPath : expectedPaths) {
      expectedTargets.add(RewriteTablePathUtil.newPath(versionPath, sourcePrefix, targetPrefix));
    }

    Set<Pair<String, String>> csvPairs = readCsvPairs(table, result.fileListLocation());
    Set<String> actualTargets = csvPairs.stream().map(Pair::second).collect(Collectors.toSet());
    assertTrue(actualTargets.containsAll(expectedTargets),
        "Copy plan should contain all expected version file targets");

    // Verify all internal paths inside each staged metadata file are rewritten to target
    assertAllInternalPathsRewritten(csvPairs, targetPrefix);
  }

  @Test
  void statsFileCopyPlanReturnsEmptySetForEmptyStats() {
    Set<Pair<String, String>> copyPlan =
        RewriteTablePathOzoneUtils.statsFileCopyPlan(List.of(), List.of());

    assertTrue(copyPlan.isEmpty());
  }

  @Test
  void statsFileCopyPlanRejectsMismatchedStatsCount() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> RewriteTablePathOzoneUtils.statsFileCopyPlan(
            List.of(statisticsFile("before-1.stats", 100)),
            List.of()));

    assertThat(exception)
        .hasMessageContaining("Before and after path rewrite, statistic files count should be same");
  }

  @Test
  void statsFileCopyPlanRejectsMismatchedStatsFileSize() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> RewriteTablePathOzoneUtils.statsFileCopyPlan(
            List.of(statisticsFile("before-1.stats", 100)),
            List.of(statisticsFile("after-1.stats", 200))));

    assertThat(exception)
        .hasMessageContaining("Before and after path rewrite, statistic files size should be same");
  }

  @Test
  void statsFileCopyPlanReturnsBeforeToAfterPathPairs() {
    Set<Pair<String, String>> copyPlan = RewriteTablePathOzoneUtils.statsFileCopyPlan(
        List.of(
            statisticsFile("before-1.stats", 100),
            statisticsFile("before-2.stats", 200)),
        List.of(
            statisticsFile("after-1.stats", 100),
            statisticsFile("after-2.stats", 200)));

    assertEquals(Set.of(
        Pair.of("before-1.stats", "after-1.stats"),
        Pair.of("before-2.stats", "after-2.stats")), copyPlan);
  }

  /**
   * For every staged file in the CSV copy plan, asserts that internal paths are rewritten
   * to the target prefix:
   * <ul>
   *   <li><b>.metadata.json</b>: table location, metadata-log entries, and snapshot
   *       manifest-list references all start with target.</li>
   *   <li><b>snap-*.avro (manifest-list)</b>: target path starts with target, and every
   *       manifest entry path inside the staged file starts with target.</li>
   *   <li><b>*.avro (manifest)</b>: target path starts with target (content rewrite
   *       is not yet implemented).</li>
   * </ul>
   */
  private void assertAllInternalPathsRewritten(Set<Pair<String, String>> csvPairs, String target) throws Exception {

    for (Pair<String, String> pair : csvPairs) {
      String stagingPath = pair.first();
      String targetPath = pair.second();

      if (stagingPath.endsWith(".metadata.json")) {
        assertMetadataFileRewritten(stagingPath, targetPath, target);
      } else if (RewriteTablePathUtil.fileName(stagingPath).startsWith("snap-")) {
        assertManifestListRewritten(stagingPath, targetPath, target);
      } else if (RewriteTablePathUtil.fileName(stagingPath).endsWith(".avro")) {
        assertTrue(targetPath.startsWith(target),
            "Manifest file target path should start with target prefix: " + targetPath);
      }
    }
  }

  private void assertMetadataFileRewritten(String stagingPath, String targetPath, String target) {

    assertTrue(targetPath.startsWith(target),
        "Target path in CSV should start with target prefix: " + targetPath);
    assertEquals(RewriteTablePathUtil.fileName(stagingPath), RewriteTablePathUtil.fileName(targetPath),
        "original and target metadata file should have the same filename");

    TableMetadata rewritten = new StaticTableOperations(stagingPath, table.io()).current();
    TableMetadata original = new StaticTableOperations(
        targetPath.replace(targetPrefix, sourcePrefix), table.io()).current();
    Set<String> expectedMetadata = original.previousFiles().stream()
        .map(e -> RewriteTablePathUtil.fileName(e.file()))
        .collect(Collectors.toSet());
    Set<String> expectedManifestLists = original.snapshots().stream()
        .map(s -> RewriteTablePathUtil.fileName(s.manifestListLocation()))
        .collect(Collectors.toSet());
    Set<String> actualMetadata = new HashSet<>();
    Set<String> actualManifestLists = new HashSet<>();

    assertTrue(rewritten.location().startsWith(target),
        "Metadata location should start with target: " + rewritten.location());

    for (MetadataLogEntry entry : rewritten.previousFiles()) {
      assertTrue(entry.file().startsWith(target),
          "Metadata log entry should start with target: " + entry.file());
      actualMetadata.add(RewriteTablePathUtil.fileName(entry.file()));
    }

    assertEquals(expectedMetadata, actualMetadata, 
        "Rewritten metadata file should reference the same metadata files as the original");

    for (Snapshot snapshot : rewritten.snapshots()) {
      String manifestList = snapshot.manifestListLocation();
      assertTrue(manifestList.startsWith(target),
          "Snapshot's manifest-list should start with target: " + manifestList);
      actualManifestLists.add(RewriteTablePathUtil.fileName(manifestList));
    }
    assertEquals(expectedManifestLists, actualManifestLists,
        "Rewritten metadata file should reference the same manifest-lists as the original");
  }

  private void assertManifestListRewritten(String stagingPath, String targetPath, String target) throws Exception {

    assertTrue(targetPath.startsWith(target),
        "Manifest list target path should start with target prefix: " + targetPath);
    assertEquals(RewriteTablePathUtil.fileName(stagingPath), RewriteTablePathUtil.fileName(targetPath),
        "original and target manifest list should have the same filename");

    Set<String> expectedManifests = new HashSet<>();
    Set<String> actualManifests = new HashSet<>();
    for (Snapshot s : table.snapshots()) {
      if (RewriteTablePathUtil.fileName(s.manifestListLocation()).equals(RewriteTablePathUtil.fileName(stagingPath))) {
        expectedManifests = s.allManifests(table.io())
            .stream()
            .map(m -> RewriteTablePathUtil.fileName(m.path()))
            .collect(Collectors.toSet());
        break;
      }
    }

    try (CloseableIterable<ManifestFile> manifests =
             InternalData.read(FileFormat.AVRO, table.io().newInputFile(stagingPath))
                 .setRootType(GenericManifestFile.class)
                 .setCustomType(
                     ManifestFile.PARTITION_SUMMARIES_ELEMENT_ID,
                     GenericPartitionFieldSummary.class)
                 .project(ManifestFile.schema())
                 .build()) {
      for (ManifestFile manifest : manifests) {
        assertTrue(manifest.path().startsWith(target),
            "Manifest path inside staged manifest list should start with target prefix: " + manifest.path());
        actualManifests.add(RewriteTablePathUtil.fileName(manifest.path()));
      }
    }
    assertEquals(expectedManifests, actualManifests,
        "Rewritten manifest list should reference the same manifest files as the original");
  }

  private static List<String> metadataLogEntryPaths(Table tbl) {
    TableMetadata meta = ((HasTableOperations) tbl).operations().current();
    List<String> paths = new ArrayList<>();
    for (MetadataLogEntry e : meta.previousFiles()) {
      paths.add(e.file());
    }
    paths.add(meta.metadataFileLocation());
    return paths;
  }

  /** Returns all (stagingPath, targetPath) pairs from the CSV file list. */
  private static Set<Pair<String, String>> readCsvPairs(Table tbl, String fileListUri)
      throws Exception {
    Set<Pair<String, String>> pairs = new HashSet<>();
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(tbl.io().newInputFile(fileListUri).newStream(),
            StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        int comma = line.indexOf(',');
        if (comma < 0) {
          continue;
        }
        String first = line.substring(0, comma);
        String second = line.substring(comma + 1);
        pairs.add(Pair.of(first, second));
      }
    }
    return pairs;
  }

  private Table createTable(String location) {
    HadoopTables tables = new HadoopTables(new Configuration());
    Table tbl = tables.create(SCHEMA, PartitionSpec.unpartitioned(), new HashMap<>(), location);
    for (int i = 0; i < COMMITS; i++) {
      String dataPath = location + "/data/batch-" + i + ".parquet";
      tbl.newAppend().appendFile(dummyDataFile(dataPath)).commit();
    }
    return tables.load(location);
  }

  private DataFile dummyDataFile(String dataPath) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(dataPath)
        .withFileSizeInBytes(1024)
        .withRecordCount(1)
        .withFormat(FileFormat.PARQUET)
        .build();
  }

  private static StatisticsFile statisticsFile(String path, long fileSizeInBytes) {
    return new GenericStatisticsFile(1L, path, fileSizeInBytes, 0L, List.of());
  }
}
