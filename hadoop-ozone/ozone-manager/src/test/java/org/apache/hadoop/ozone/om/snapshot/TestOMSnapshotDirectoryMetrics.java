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

import static org.apache.hadoop.ozone.OzoneConsts.ROCKSDB_SST_SUFFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Tests for OMSnapshotDirectoryMetrics.
 */
public class TestOMSnapshotDirectoryMetrics {

  @TempDir
  private Path tempDir;

  private OMMetadataManager metadataManager;
  private RDBStore store;
  private RocksDBCheckpointDiffer differ;
  private OMSnapshotDirectoryMetrics metrics;

  @BeforeEach
  public void setup() throws IOException {
    metadataManager = Mockito.mock(OMMetadataManager.class);
    store = Mockito.mock(RDBStore.class);
    differ = Mockito.mock(RocksDBCheckpointDiffer.class);

    when(metadataManager.getStore()).thenReturn(store);
    when(store.getRocksDBCheckpointDiffer()).thenReturn(differ);

    Path snapshotsDir = tempDir.resolve("snapshots");
    Files.createDirectories(snapshotsDir);
    when(store.getSnapshotsParentDir()).thenReturn(snapshotsDir.toString());

    Path backupDir = tempDir.resolve("backup");
    Files.createDirectories(backupDir);
    when(differ.getSSTBackupDir()).thenReturn(backupDir.toString());

    OzoneConfiguration conf = new OzoneConfiguration();
    metrics = new OMSnapshotDirectoryMetrics(conf, metadataManager);
  }

  @Test
  public void testMetrics() throws IOException {
    Path snapshotsDir = Paths.get(metadataManager.getStore().getSnapshotsParentDir());
    Path backupDir = Paths.get(metadataManager.getStore().getRocksDBCheckpointDiffer().getSSTBackupDir());

    // Create a snapshot directory with some files
    Path snap1 = snapshotsDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(snap1);
    Path file1 = snap1.resolve("file1" + ROCKSDB_SST_SUFFIX);
    Files.write(file1, "data1".getBytes(StandardCharsets.UTF_8));
    long size1 = Files.size(file1);

    // Create backup directory with some files
    Path backupFile1 = backupDir.resolve("backup1" + ROCKSDB_SST_SUFFIX);
    Files.write(backupFile1, "backupData1".getBytes(StandardCharsets.UTF_8));
    long backupSize1 = Files.size(backupFile1);

    Path backupFile2 = backupDir.resolve("backup2" + ROCKSDB_SST_SUFFIX);
    Files.write(backupFile2, "backupData22".getBytes(StandardCharsets.UTF_8));
    long backupSize2 = Files.size(backupFile2);

    metrics.updateMetrics();

    assertEquals(1, metrics.getNumSnapshots());
    assertEquals(1, metrics.getTotalSstFilesCount());
    assertEquals(size1, metrics.getDbSnapshotsDirSize());
    assertEquals(2, metrics.getSstBackupSstFilesCount());
    assertEquals(backupSize1 + backupSize2, metrics.getSstBackupDirSize());

    // Add another snapshot with a hardlink
    Path snap2 = snapshotsDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(snap2);
    Path file2 = snap2.resolve("file1" + ROCKSDB_SST_SUFFIX);
    try {
      Files.createLink(file2, file1);
    } catch (UnsupportedOperationException e) {
      // Fallback for systems that don't support hardlinks in temp dir
      Files.write(file2, "data1".getBytes(StandardCharsets.UTF_8));
    }

    metrics.updateMetrics();

    assertEquals(2, metrics.getNumSnapshots());
    // Total SST count should still be 1 if hardlink is counted once, 
    // but the implementation uses visitedInodes per snapshot directory? 
    // Wait, let's check the implementation again.
    // calculateAndUpdateMetrics: visitedSnapshotsInodes is used for ALL snapshots.
    assertEquals(1, metrics.getTotalSstFilesCount());
    assertEquals(size1, metrics.getDbSnapshotsDirSize());

    // Test backup dir update
    Path backupFile3 = backupDir.resolve("notSst.txt");
    Files.write(backupFile3, "notSstData".getBytes(StandardCharsets.UTF_8));

    metrics.updateMetrics();
    assertEquals(2, metrics.getSstBackupSstFilesCount());
    assertEquals(backupSize1 + backupSize2 + Files.size(backupFile3), metrics.getSstBackupDirSize());
  }
}
