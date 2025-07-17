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

package org.apache.hadoop.ozone.container.keyvalue;

import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for verifying the race condition between container scanner
 * and export process for schema v2 containers.
 */
public class TestContainerExportWithScanner {

  @TempDir
  private File folder;

  private String scmId;
  private VolumeSet volumeSet;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private KeyValueContainerData containerData;
  private KeyValueContainer container;
  private UUID datanodeId;
  private List<HddsVolume> hddsVolumes;
  private OzoneConfiguration conf;

  @BeforeEach
  public void setup() throws Exception {
    scmId = UUID.randomUUID().toString();
    datanodeId = UUID.randomUUID();
    conf = new OzoneConfiguration();

    // Set schema version to V2
    ContainerTestVersionInfo.setTestSchemaVersion(SCHEMA_V2, conf);

    hddsVolumes = new ArrayList<>();
    hddsVolumes.add(new HddsVolume.Builder(folder.toString())
        .conf(conf).datanodeUuid(datanodeId.toString()).build());

    volumeSet = mock(MutableVolumeSet.class);
    volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);

    when(volumeSet.getVolumesList())
        .thenAnswer(i -> hddsVolumes.stream()
            .map(v -> (StorageVolume) v)
            .collect(Collectors.toList()));

    when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenAnswer(invocation -> {
          List<HddsVolume> volumes = invocation.getArgument(0);
          return volumes.get(0);
        });

    containerData = new KeyValueContainerData(1L,
        ContainerLayoutVersion.FILE_PER_BLOCK,
        (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
        datanodeId.toString());

    container = new KeyValueContainer(containerData, conf);
  }

  @AfterEach
  public void tearDown() {
    // Clean up resources
    BlockUtils.shutdownCache(conf);
  }

  /**
   * Test that verifies the scanner can hold a DB reference without a lock.
   */
  @Test
  public void testScannerHoldsDbReferenceWithoutLock() throws Exception {
    // Create container
    container.create(volumeSet, volumeChoosingPolicy, scmId);

    // Close container (required for export)
    containerData.setState(ContainerProtos.ContainerDataProto.State.CLOSED);

    // Get a DB reference (simulating scanner behavior)
    ReferenceCountedDB db = (ReferenceCountedDB) BlockUtils.getDB(containerData, conf);

    // Verify reference count is 1
    assertEquals(1, db.getReferenceCount());

    // Verify container write lock is not held
    // Note: hasReadLock() actually tries to acquire the lock, so we expect it to return true
    // and we need to release it afterward
    assertFalse(container.hasWriteLock());

    // Clean up
    db.close();
  }

  /**
   * Test that verifies the export process fails to evict DB when scanner holds a reference.
   */
  @Test
  public void testExportFailsToEvictDbWithScannerReference() throws Exception {
    // Create container
    container.create(volumeSet, volumeChoosingPolicy, scmId);

    // Close container (required for export)
    containerData.setState(ContainerProtos.ContainerDataProto.State.CLOSED);

    // Simulate scanner getting DB reference without lock
    ReferenceCountedDB db = (ReferenceCountedDB) BlockUtils.getDB(containerData, conf);

    // Verify initial reference count
    assertEquals(1, db.getReferenceCount());

    // Now try to export the container (which will try to remove DB from cache)
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    try {
      // This will throw an IllegalArgumentException because it can't evict the DB
      // with a non-zero reference count
      try {
        container.exportContainerData(outputStream, null);
        // If we get here, the test should fail
        fail("Expected IllegalArgumentException was not thrown");
      } catch (IllegalArgumentException e) {
        // Expected exception - the export process failed to evict the DB
        assertTrue(e.getMessage().contains("refCount"));
      }

      // The DB should still be in cache with reference count 1
      ReferenceCountedDB cachedDB = (ReferenceCountedDB) BlockUtils.getDB(containerData, conf);
      assertEquals(2, cachedDB.getReferenceCount());

      // Clean up
      cachedDB.close();
    } finally {
      db.close();
    }
  }

  /**
   * Test that simulates the race condition between scanner and export process.
   */
  @Test
  public void testRaceConditionBetweenScannerAndExport() throws Exception {
    // Create container
    container.create(volumeSet, volumeChoosingPolicy, scmId);

    // Close container (required for export)
    containerData.setState(ContainerProtos.ContainerDataProto.State.CLOSED);

    final AtomicBoolean scannerFinished = new AtomicBoolean(false);
    final AtomicBoolean exporterFailed = new AtomicBoolean(false);
    final CountDownLatch scannerStarted = new CountDownLatch(1);
    final CountDownLatch exporterReady = new CountDownLatch(1);

    ExecutorService executor = Executors.newFixedThreadPool(2);

    // Scanner thread
    executor.submit(() -> {
      try {
        // Get DB reference without container lock (like scanner does)
        ReferenceCountedDB scannerDb = (ReferenceCountedDB) BlockUtils.getDB(containerData, conf);
        scannerStarted.countDown();

        // Wait for exporter to be ready
        exporterReady.await();

        // Simulate scanner using DB for a while
        Thread.sleep(1000);

        // Close DB reference
        scannerDb.close();
        scannerFinished.set(true);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    // Export thread
    executor.submit(() -> {
      try {
        // Wait for scanner to get DB reference
        scannerStarted.await();
        exporterReady.countDown();

        // Try to export container
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
          container.exportContainerData(outputStream, null);
        } catch (IllegalArgumentException e) {
          // Expected exception - the export process failed to evict the DB
          if (e.getMessage().contains("refCount")) {
            exporterFailed.set(true);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    // Wait for both operations to complete
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    // Verify scanner completed and exporter failed with the expected exception
    assertTrue(scannerFinished.get(), "Scanner should have finished");
    assertTrue(exporterFailed.get(), "Exporter should have failed with refCount exception");
  }

  /**
   * Test that verifies the DB reference counting mechanism works correctly.
   */
  @Test
  public void testDbReferenceCountingMechanism() throws Exception {
    // Create container
    container.create(volumeSet, volumeChoosingPolicy, scmId);

    // Get first reference
    ReferenceCountedDB db1 = (ReferenceCountedDB) BlockUtils.getDB(containerData, conf);
    assertEquals(1, db1.getReferenceCount());

    // Get second reference
    ReferenceCountedDB db2 = (ReferenceCountedDB) BlockUtils.getDB(containerData, conf);
    assertEquals(2, db1.getReferenceCount());
    assertEquals(2, db2.getReferenceCount());

    // Close first reference
    db1.close();
    assertEquals(1, db2.getReferenceCount());

    // Close second reference
    db2.close();

    // Now the reference count should be 0, so we can get a new reference
    ReferenceCountedDB db3 = (ReferenceCountedDB) BlockUtils.getDB(containerData, conf);
    assertEquals(1, db3.getReferenceCount());
    db3.close();
  }
}
