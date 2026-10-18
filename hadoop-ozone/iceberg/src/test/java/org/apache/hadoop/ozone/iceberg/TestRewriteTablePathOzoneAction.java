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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.GenericPartitionFieldSummary;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

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

  private ByteArrayOutputStream outContent;
  private ByteArrayOutputStream errContent;
  private PrintStream originalOut;
  private PrintStream originalErr;

  @TempDir
  private Path tableDir;
  @TempDir
  private Path targetDir;
  @TempDir
  private Path stagingDir;

  @BeforeEach
  public void setupTableLocation() throws IOException {
    String tableLocation = tableDir.toUri().toString().replaceFirst("^file:///", "file:/") + TABLE_NAME;
    this.table = createTable(tableLocation + "/");
    this.sourcePrefix = tableLocation;
    this.targetPrefix = targetDir.toUri().toString().replaceFirst("^file:///", "file:/") + TABLE_NAME;

    outContent = new ByteArrayOutputStream();
    errContent = new ByteArrayOutputStream();
    originalOut = System.out;
    originalErr = System.err;
    System.setOut(new PrintStream(outContent, true, StandardCharsets.UTF_8));
    System.setErr(new PrintStream(errContent, true, StandardCharsets.UTF_8));
  }

  @AfterEach
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  void fullTablePathRewrite() throws Exception {
    String fileListLocation = executeRewriteCommand("--threads", "2");

    List<String> metadataPaths = metadataLogEntryPaths(table);
    Set<String> expectedTargets = new HashSet<>();
    for (String path : metadataPaths) {
      expectedTargets.add(RewriteTablePathUtil.newPath(path, sourcePrefix, targetPrefix));
    }

    Set<Pair<String, String>> csvPairs = readCsvPairs(table, fileListLocation);
    Set<String> actualTargets = csvPairs.stream().map(Pair::second)
        .filter(p -> p.endsWith(".metadata.json"))
        .collect(Collectors.toSet());
    assertEquals(expectedTargets, actualTargets,
        "Copy plan should contain all expected version file targets");

    // Verify all internal paths inside each staged metadata file are rewritten to target.
    assertAllInternalPathsRewritten(csvPairs, targetPrefix);
  }

  @Test
  void tablePathRewriteForStartAndNoEndVersionProvided() throws Exception {
    List<String> metadataPaths = metadataLogEntryPaths(table);
    String startName = RewriteTablePathUtil.fileName(metadataPaths.get(2));

    String fileListLocation = executeRewriteCommand("--start-version", startName);

    List<String> expectedPaths = new ArrayList<>();
    for (int i = metadataPaths.size() - 1; i >= 3; i--) {
      expectedPaths.add(metadataPaths.get(i));
    }

    Set<String> expectedTargets = new HashSet<>();
    for (String versionPath : expectedPaths) {
      expectedTargets.add(RewriteTablePathUtil.newPath(versionPath, sourcePrefix, targetPrefix));
    }

    Set<Pair<String, String>> csvPairs = readCsvPairs(table, fileListLocation);
    Set<String> actualTargets = csvPairs.stream().map(Pair::second)
        .filter(p -> p.endsWith(".metadata.json"))
        .collect(Collectors.toSet());
    assertEquals(expectedTargets, actualTargets,
        "Copy plan should contain all expected version file targets");

    // Verify all internal paths inside each staged metadata file are rewritten to target
    assertAllInternalPathsRewritten(csvPairs, targetPrefix);
  }

  @Test
  void tablePathRewriteForOnlyEndVersionProvided() throws Exception {
    List<String> metadataPaths = metadataLogEntryPaths(table);
    String endName = RewriteTablePathUtil.fileName(metadataPaths.get(2));

    String fileListLocation = executeRewriteCommand("--end-version", endName);

    List<String> expectedPaths = new ArrayList<>();
    for (int i = 2; i >= 0; i--) {
      expectedPaths.add(metadataPaths.get(i));
    }

    Set<String> expectedTargets = new HashSet<>();
    for (String versionPath : expectedPaths) {
      expectedTargets.add(RewriteTablePathUtil.newPath(versionPath, sourcePrefix, targetPrefix));
    }

    Set<Pair<String, String>> csvPairs = readCsvPairs(table, fileListLocation);
    Set<String> actualTargets = csvPairs.stream().map(Pair::second)
        .filter(p -> p.endsWith(".metadata.json"))
        .collect(Collectors.toSet());
    assertEquals(expectedTargets, actualTargets,
        "Copy plan should contain all expected version file targets");

    // Verify all internal paths inside each staged metadata file are rewritten to target
    assertAllInternalPathsRewritten(csvPairs, targetPrefix);
  }

  @Test
  void tablePathRewriteForStartAndEndVersionProvided() throws Exception {
    List<String> metadataPaths = metadataLogEntryPaths(table);
    String startName = RewriteTablePathUtil.fileName(metadataPaths.get(1));
    String endName = RewriteTablePathUtil.fileName(metadataPaths.get(3));

    String fileListLocation = executeRewriteCommand(
        "--start-version", startName,
        "--end-version", endName);

    List<String> expectedPaths = new ArrayList<>();
    for (int i = 3; i >= 2; i--) {
      expectedPaths.add(metadataPaths.get(i));
    }

    Set<String> expectedTargets = new HashSet<>();
    for (String versionPath : expectedPaths) {
      expectedTargets.add(RewriteTablePathUtil.newPath(versionPath, sourcePrefix, targetPrefix));
    }

    Set<Pair<String, String>> csvPairs = readCsvPairs(table, fileListLocation);
    Set<String> actualTargets = csvPairs.stream().map(Pair::second)
        .filter(p -> p.endsWith(".metadata.json"))
        .collect(Collectors.toSet());
    assertEquals(expectedTargets, actualTargets,
        "Copy plan should contain all expected version file targets");

    // Verify all internal paths inside each staged metadata file are rewritten to target
    assertAllInternalPathsRewritten(csvPairs, targetPrefix);
  }

  @Test
  void executeRejectsMissingLocationPrefix() {
    NullPointerException exception = assertThrows(NullPointerException.class,
        () -> new RewriteTablePathOzoneAction(table, 2)
            .stagingLocation(stagingDir.toString() + "/")
            .execute());

    assertEquals("Source prefix is null", exception.getMessage());
  }

  @Test
  void executeRejectsMissingTargetPrefix() {
    NullPointerException exception = assertThrows(NullPointerException.class,
        () -> new RewriteTablePathOzoneAction(table, 2)
            .rewriteLocationPrefix(sourcePrefix, null));

    assertEquals("Target prefix is null", exception.getMessage());
  }

  @Test
  void rewriteLocationPrefixRejectsSameSourceAndTarget() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> new RewriteTablePathOzoneAction(table, 2)
            .rewriteLocationPrefix(sourcePrefix, sourcePrefix)
            .execute());

    assertEquals("Source prefix cannot be the same as target prefix (" +
        sourcePrefix + ")", exception.getMessage());
  }

  @Test
  void startVersionRejectsUnknownVersion() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> new RewriteTablePathOzoneAction(table, 2)
            .rewriteLocationPrefix(sourcePrefix, targetPrefix)
            .startVersion("missing.metadata.json")
            .execute());

    assertEquals("Cannot find provided version file missing.metadata.json " +
        "in metadata log.", exception.getMessage());
  }

  @Test
  void startVersionRejectsDeletedVersionFile() {
    List<String> metadataPaths = metadataLogEntryPaths(table);
    String existingName = RewriteTablePathUtil.fileName(metadataPaths.get(0));
    table.io().deleteFile(metadataPaths.get(0));

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> new RewriteTablePathOzoneAction(table, 2)
            .rewriteLocationPrefix(sourcePrefix, targetPrefix)
            .startVersion(existingName)
            .execute());

    assertThat(exception).hasMessageContaining("does not exist");
  }

  @Test
  void endVersionRejectsUnknownVersion() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> new RewriteTablePathOzoneAction(table, 2)
            .rewriteLocationPrefix(sourcePrefix, targetPrefix)
            .endVersion("missing.metadata.json")
            .execute());

    assertEquals("Cannot find provided version file missing.metadata.json " +
        "in metadata log.", exception.getMessage());
  }

  @Test
  void endVersionRejectsDeletedVersionFile() {
    List<String> metadataPaths = metadataLogEntryPaths(table);
    String existingName = RewriteTablePathUtil.fileName(metadataPaths.get(0));
    table.io().deleteFile(metadataPaths.get(0));

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> new RewriteTablePathOzoneAction(table, 2)
            .rewriteLocationPrefix(sourcePrefix, targetPrefix)
            .endVersion(existingName)
            .execute());

    assertThat(exception).hasMessageContaining("does not exist");
  }

  @Test
  void usesCurrentMetadataIfEndVersionNotProvided() {
    String currentMetadata = ((HasTableOperations) table).operations().current().metadataFileLocation();
    RewriteTablePathOzoneAction action = new RewriteTablePathOzoneAction(table, 2);
    action.rewriteLocationPrefix(sourcePrefix, targetPrefix).stagingLocation(stagingDir + "/");
    RewriteTablePath.Result result = action.execute();
    assertThat(result.latestVersion()).isEqualTo(RewriteTablePathUtil.fileName(currentMetadata));
  }

  @Test
  void defaultStagingDirIsUnderTableMetadataLocation() {
    String metadataLocation = RewriteTablePathOzoneUtils.getMetadataLocation(table);
    RewriteTablePath.Result result = new RewriteTablePathOzoneAction(table, 2)
        .rewriteLocationPrefix(sourcePrefix, targetPrefix)
        .execute();

    String actualStagingDir = result.stagingLocation();
    assertTrue(actualStagingDir.startsWith(metadataLocation),
        "Auto-generated staging dir should be under the table's metadata location."
            + " Expected prefix: " + metadataLocation + ", actual: " + actualStagingDir);
    assertTrue(actualStagingDir.contains("copy-table-staging-"),
        "Auto-generated staging dir should contain 'copy-table-staging-': " + actualStagingDir);
    assertTrue(actualStagingDir.endsWith(RewriteTablePathUtil.FILE_SEPARATOR),
        "Auto-generated staging dir should end with FILE_SEPARATOR: " + actualStagingDir);
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

  @Test
  void rejectsTablesWithPartitionStatistics() {
    TableMetadata baseMetadata = ((HasTableOperations) table).operations().current();
    long snapshotId = baseMetadata.currentSnapshot().snapshotId();
    PartitionStatisticsFile statsFile = Mockito.mock(PartitionStatisticsFile.class);
    Mockito.when(statsFile.snapshotId()).thenReturn(snapshotId);
    Mockito.when(statsFile.path()).thenReturn(sourcePrefix + "/metadata/dummy.stats");
    Mockito.when(statsFile.fileSizeInBytes()).thenReturn(100L);
    TableMetadata metadataWithStats = TableMetadata.buildFrom(baseMetadata)
        .setPartitionStatistics(statsFile)
        .build();

    TableOperations ops = ((HasTableOperations) table).operations();
    ops.commit(baseMetadata, metadataWithStats);

    RewriteTablePath action = new RewriteTablePathOzoneAction(table, 2)
        .rewriteLocationPrefix(sourcePrefix, targetPrefix)
        .stagingLocation(stagingDir + "/");

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, action::execute);
    assertThat(exception).hasMessageContaining("Partition statistics files are not supported yet.");
  }

  @Test
  public void positionDeletesReaderUnsupportedFormat() {
    InputFile mockInput = Mockito.mock(InputFile.class);
    Mockito.when(mockInput.location()).thenReturn("s3://bucket/test.txt");
    PartitionSpec spec = PartitionSpec.unpartitioned();
    FileFormat mockUnsupportedFormat = Mockito.mock(FileFormat.class);
    Mockito.when(mockUnsupportedFormat.toString()).thenReturn("txt");

    UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
        () -> RewriteTablePathOzoneAction.positionDeletesReader(mockInput, mockUnsupportedFormat, spec));

    assertThat(exception).hasMessageContaining("Unsupported file format: txt");
  }

  @Test
  public void positionDeletesWriterUnsupportedFormat() {
    OutputFile mockOutput = Mockito.mock(OutputFile.class);
    Mockito.when(mockOutput.location()).thenReturn("s3://bucket/test.txt");
    PartitionSpec spec = PartitionSpec.unpartitioned();
    FileFormat mockUnsupportedFormat = Mockito.mock(FileFormat.class);
    Mockito.when(mockUnsupportedFormat.toString()).thenReturn("txt");

    UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
        () -> RewriteTablePathOzoneAction.positionDeletesWriter(
            mockOutput, mockUnsupportedFormat, spec, null, null));

    assertThat(exception).hasMessageContaining("Unsupported file format: txt");
  }

  @ParameterizedTest
  @EnumSource(value = FileFormat.class, names = {"AVRO", "ORC", "PARQUET"})
  void positionDeletesAvroAndOrcRoundTrip(FileFormat format, @TempDir Path temp) throws IOException {
    String extension = format.name().toLowerCase();
    String path = temp.resolve("test." + extension).toUri().toString();
    OutputFile outputFile = table.io().newOutputFile(path);
    PartitionSpec spec = table.spec();
    
    try (PositionDeleteWriter<Record> writer = RewriteTablePathOzoneAction.positionDeletesWriter(
        outputFile, format, spec, new PartitionData(spec.partitionType()), SCHEMA)) {

      GenericRecord row = GenericRecord.create(SCHEMA);
      row.setField("c1", 42);
      row.setField("c2", format.name() + "-test");

      writer.write(PositionDelete.<Record>create().set("data.parquet", 100L, row));
    }
    
    try (CloseableIterable<Record> reader = RewriteTablePathOzoneAction.positionDeletesReader(
        table.io().newInputFile(path), format, spec)) {

      List<Record> results = new ArrayList<>();
      reader.forEach(results::add);

      assertThat(results).hasSize(1);
      Record record = results.get(0);
      
      assertThat(record.getField("file_path").toString()).isEqualTo("data.parquet");
      assertThat(record.getField("pos")).isEqualTo(100L);
      
      Record rowResult = (Record) record.getField("row");
      assertThat(rowResult.getField("c1")).isEqualTo(42);
      assertThat(rowResult.getField("c2")).isEqualTo(format.name() + "-test");
    }
  }

  @Test
  void manifestsToRewriteRejectsMissingManifestList() {
    Snapshot snapshot = table.currentSnapshot();
    String manifestListLocation = snapshot.manifestListLocation();
    table.io().deleteFile(manifestListLocation);

    RewriteTablePath action = new RewriteTablePathOzoneAction(table, 2)
        .rewriteLocationPrefix(sourcePrefix, targetPrefix)
        .stagingLocation(stagingDir + "/");
    
    RuntimeException exception = assertThrows(RuntimeException.class, action::execute);
    assertThat(exception).hasMessageContaining("Failed to collect manifests to rewrite");
    assertThat(exception.getCause()).hasMessageContaining("Failed to read manifests for snapshot " +
        snapshot.snapshotId());
  }

  private String executeRewriteCommand(String... optionalArgs) {
    List<String> args = new ArrayList<>();
    args.add("rewrite-path");
    args.add("-l");
    args.add(table.location());
    args.add("-s");
    args.add(sourcePrefix);
    args.add("-t");
    args.add(targetPrefix);
    args.add("--staging");
    args.add(stagingDir + "/");
    args.addAll(Arrays.asList(optionalArgs));

    int exitCode = new IcebergCommand().getCmd().execute(args.toArray(new String[0]));
    assertEquals(0, exitCode,
        "Command failed.\nstdout:\n" + stdout() + "\nstderr:\n" + stderr());
    assertThat(stdout())
        .contains("Starting Iceberg table path rewrite")
        .contains("Table loaded: " + table.location())
        .contains("Staging location: " + stagingDir + "/")
        .contains("File list location:");
    return parseFileListLocation(stdout());
  }

  private String stdout() {
    return outContent.toString(StandardCharsets.UTF_8);
  }

  private String stderr() {
    return errContent.toString(StandardCharsets.UTF_8);
  }

  private static String parseFileListLocation(String output) {
    for (String line : output.split("\n")) {
      if (line.contains("File list location:")) {
        return line.substring(line.indexOf("File list location:") + "File list location:".length())
            .trim();
      }
    }
    throw new IllegalStateException("File list location not found in command output: " + output);
  }

  /**
   * For every staged file in the CSV copy plan, asserts that internal paths are rewritten
   * to the target prefix:
   * <ul>
   *   <li><b>.metadata.json</b>: table location, metadata-log entries, and snapshot
   *       manifest-list references all start with target.</li>
   *   <li><b>snap-*.avro (manifest-list)</b>: target path starts with target, and every
   *       manifest entry path inside the staged file starts with target.</li>
   *   <li><b>*.avro (manifest)</b>: target path starts with target and the content inside it.</li>
   *   <li><b>deletes.parquet(position delete file)</b>: target path starts with target and the content inside it.</li>
   * </ul>
   */
  private void assertAllInternalPathsRewritten(Set<Pair<String, String>> csvPairs, String target) throws Exception {

    for (Pair<String, String> pair : csvPairs) {
      String stagingPath = pair.first();
      String targetPath = pair.second();

      if (stagingPath.endsWith(".metadata.json")) {
        assertMetadataFileRewritten(stagingPath, targetPath, target);
      } else if (RewriteTablePathUtil.fileName(stagingPath).startsWith("snap-")) {
        assertManifestListRewritten(stagingPath, targetPath, target, csvPairs);
      } else if (RewriteTablePathUtil.fileName(stagingPath).endsWith(".avro")) {
        assertTrue(targetPath.startsWith(target),
            "Manifest file target path should start with target prefix: " + targetPath);
      } else if (stagingPath.endsWith("deletes.parquet")) {
        assertStagedDeleteFileInternalPathsRewritten(table, stagingPath, target);
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

  private void assertManifestListRewritten(String stagingPath, String targetPath, String target,
                                           Set<Pair<String, String>> csvPairs) throws Exception {

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
        Optional<String> manifestStagingPath = csvPairs.stream()
            .filter(p -> p.second().equals(manifest.path()))
            .map(Pair::first)
            .findFirst();
        if (manifestStagingPath.isPresent()) {
          String originalPath = manifest.path().replace(targetPrefix, sourcePrefix);
          ManifestFile original = Mockito.spy(manifest);
          Mockito.doReturn(originalPath).when(original).path();
          
          ManifestFile staged = Mockito.spy(manifest);
          Mockito.doReturn(manifestStagingPath.get()).when(staged).path();

          if (manifest.content() == ManifestContent.DATA) {
            assertDataManifestPathsRewritten(staged, original, target);
          } else if (manifest.content() == ManifestContent.DELETES) {
            assertDeleteManifestPathsRewritten(staged, original, target);
          }
        }
      }
    }
    assertEquals(expectedManifests, actualManifests,
        "Rewritten manifest list should reference the same manifest files as the original");
  }

  private void assertDataManifestPathsRewritten(ManifestFile staged, ManifestFile original,
      String target) throws IOException {
    Set<String> expectedFileNames = new HashSet<>();
    try (ManifestReader<DataFile> reader = ManifestFiles.read(original, table.io())) {
      for (DataFile df : reader) {
        expectedFileNames.add(RewriteTablePathUtil.fileName(df.location()));
      }
    }

    Set<String> actualFileNames = new HashSet<>();
    try (ManifestReader<DataFile> reader = ManifestFiles.read(staged, table.io())) {
      for (DataFile dataPath : reader) {
        assertTrue(dataPath.location().startsWith(target),
            "Data file path inside staged data manifest should start with target prefix: " + dataPath);
        actualFileNames.add(RewriteTablePathUtil.fileName(dataPath.location()));
      }
    }

    assertEquals(expectedFileNames, actualFileNames,
        "Rewritten data manifest should reference the same data files as the original");
  }

  private void assertDeleteManifestPathsRewritten(ManifestFile staged, ManifestFile original, 
      String target) throws IOException {
    Set<String> expectedFileNames = new HashSet<>();
    try (ManifestReader<DeleteFile> reader = ManifestFiles.readDeleteManifest(original, table.io(), table.specs())) {
      for (DeleteFile df : reader) {
        expectedFileNames.add(RewriteTablePathUtil.fileName(df.location()));
      }
    }

    Set<String> actualFileNames = new HashSet<>();
    try (ManifestReader<DeleteFile> reader = ManifestFiles.readDeleteManifest(staged, table.io(), table.specs())) {
      for (DeleteFile df : reader) {
        assertTrue(df.location().startsWith(target),
            "Delete file path inside staged delete manifest should start with target prefix: "
                + df.location());
        actualFileNames.add(RewriteTablePathUtil.fileName(df.location()));
      }
    }

    assertEquals(expectedFileNames, actualFileNames,
        "Rewritten delete manifest should reference the same delete files (by name) as the original");
  }

  private static void assertStagedDeleteFileInternalPathsRewritten(
      Table tbl, String stagedPath, String targetPrefix) throws IOException {
    Schema readSchema = DeleteSchemaUtil.pathPosSchema();
    String pathColumn = MetadataColumns.DELETE_FILE_PATH.name();
    int rowCount = 0;
    try (CloseableIterable<Record> rows =
             Parquet.read(tbl.io().newInputFile(stagedPath))
                 .project(readSchema)
                 .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(readSchema, fileSchema))
                 .build()) {
      for (Record row : rows) {
        Object path = row.getField(pathColumn);
        assertTrue(
            path.toString().startsWith(targetPrefix),
            stagedPath + " row " + rowCount + ": path '" + path
                + "' must start with '" + targetPrefix + "'");
        rowCount++;
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

  private Table createTable(String location) throws IOException {
    HadoopTables tables = new HadoopTables(new OzoneConfiguration());
    Table tbl = tables.create(SCHEMA, PartitionSpec.unpartitioned(), new HashMap<>(), location);
    for (int i = 0; i < COMMITS; i++) {
      String dataPath = location + "data/batch-" + i + ".parquet";
      tbl.newAppend().appendFile(dummyDataFile(dataPath)).commit();
    }

    for (int i = 0; i < 2; i++) {
      String dataPath = location + "data/batch-" + i + ".parquet";
      DeleteFile df = writePositionDeleteFile(tbl, dataPath);
      tbl.newRowDelta().addDeletes(df).commit();
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

  private DeleteFile writePositionDeleteFile(Table tbl, String referencedDataPath)
      throws IOException {
    String deleteUri = RewriteTablePathUtil.combinePaths(
        tbl.location(), "data/" + UUID.randomUUID() + "-deletes.parquet");
    PositionDeleteWriter<Record> writer =
        Parquet.writeDeletes(tbl.io().newOutputFile(deleteUri))
            .createWriterFunc(GenericParquetWriter::create)
            .withSpec(tbl.spec())
            .withPartition(new PartitionData(tbl.spec().partitionType()))
            .metricsConfig(MetricsConfig.forPositionDelete(tbl))
            .overwrite()
            .buildPositionWriter();
    try {
      writer.write(PositionDelete.<Record>create().set(referencedDataPath, 0L));
    } finally {
      writer.close();
    }
    return writer.toDeleteFile();
  }

  private static StatisticsFile statisticsFile(String path, long fileSizeInBytes) {
    return new GenericStatisticsFile(1L, path, fileSizeInBytes, 0L, List.of());
  }
}
