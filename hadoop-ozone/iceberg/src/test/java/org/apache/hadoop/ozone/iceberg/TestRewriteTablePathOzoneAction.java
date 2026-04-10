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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
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
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.hadoop.HadoopTables;
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

  private String tableLocation = null;
  private String sourcePrefix = null;
  private String targetPrefix = null;
  private Table table = null;

  private final HadoopTables tables = new HadoopTables(new Configuration());

  @TempDir
  private java.nio.file.Path tableDir;
  @TempDir
  private java.nio.file.Path targetDir;
  @TempDir
  private java.nio.file.Path stagingDir;

  @BeforeEach
  public void setupTableLocation() {
    this.tableLocation = tableDir.toUri().toString().replaceFirst("^file:///", "file:/") + TABLE_NAME;
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
    assertEquals(expectedTargets, csvPairs.stream().map(Pair::second).collect(Collectors.toSet()));

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
    assertEquals(expectedTargets, csvPairs.stream().map(Pair::second).collect(Collectors.toSet()));

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
    assertEquals(expectedTargets, csvPairs.stream().map(Pair::second).collect(Collectors.toSet()));

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
    assertEquals(expectedTargets, csvPairs.stream().map(Pair::second).collect(Collectors.toSet()));

    // Verify all internal paths inside each staged metadata file are rewritten to target
    assertAllInternalPathsRewritten(csvPairs, targetPrefix);
  }

  /**
   * For every staged metadata JSON file in the CSV, parses the file and asserts that:
   * - The table location starts with target
   * - Every metadata-log entry path starts with target
   * - Every snapshot's manifest-list path starts with target
   * - Every statistics file path starts with target
   * - None of the above contain the source prefix
   */
  private void assertAllInternalPathsRewritten(Set<Pair<String, String>> csvPairs, String target) {
    for (Pair<String, String> pair : csvPairs) {
      String stagingPath = pair.first();
      String targetPath = pair.second();

      // Only inspect .metadata.json files, manifest/data files and snapshots are not yet rewritten
      if (!stagingPath.endsWith(".metadata.json")) {
        continue;
      }

      assertTrue(targetPath.startsWith(target),
          "Target path in CSV should start with target prefix: " + targetPath);

      TableMetadata rewritten = new StaticTableOperations(stagingPath, table.io()).current();

      assertTrue(rewritten.location().startsWith(target),
          "Metadata location should start with target: " + rewritten.location());

      for (MetadataLogEntry entry : rewritten.previousFiles()) {
        assertTrue(entry.file().startsWith(target),
            "Metadata log entry should start with target: " + entry.file());
      }

      for (Snapshot snapshot : rewritten.snapshots()) {
        String manifestList = snapshot.manifestListLocation();
        assertTrue(manifestList.startsWith(target),
            "Snapshot manifest-list should start with target: " + manifestList);
      }
    }
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
}
