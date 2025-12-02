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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager.WritableOmSnapshotLocalDataProvider;
import org.apache.hadoop.ozone.om.snapshot.diff.delta.CompositeDeltaDiffComputer;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for SnapshotDefragService.
 */
public class TestSnapshotDefragService {

  @Mock
  private OzoneManager ozoneManager;

  @Mock
  private OmSnapshotManager omSnapshotManager;

  @Mock
  private OmSnapshotLocalDataManager snapshotLocalDataManager;

  @Mock
  private OmMetadataManagerImpl metadataManager;

  @Mock
  private IOzoneManagerLock omLock;

  @Mock
  private OMLayoutVersionManager versionManager;

  @TempDir
  private Path tempDir;
  private SnapshotDefragService defragService;
  private AutoCloseable mocks;

  @BeforeEach
  public void setup() throws IOException {
    mocks = MockitoAnnotations.openMocks(this);
    OzoneConfiguration configuration = new OzoneConfiguration();

    // Setup basic mocks
    when(ozoneManager.getOmSnapshotManager()).thenReturn(omSnapshotManager);
    when(ozoneManager.getMetadataManager()).thenReturn(metadataManager);
    when(ozoneManager.getThreadNamePrefix()).thenReturn("TestOM");
    when(ozoneManager.isRunning()).thenReturn(true);
    when(ozoneManager.getVersionManager()).thenReturn(versionManager);
    when(ozoneManager.getOmRatisServer()).thenReturn(mock(OzoneManagerRatisServer.class));

    when(omSnapshotManager.getSnapshotLocalDataManager()).thenReturn(snapshotLocalDataManager);
    when(metadataManager.getLock()).thenReturn(omLock);
    when(metadataManager.getSnapshotParentDir()).thenReturn(tempDir);
    when(versionManager.isAllowed(any(LayoutFeature.class))).thenReturn(true);
    try (MockedConstruction<CompositeDeltaDiffComputer> compositeDeltaDiffComputer =
             mockConstruction(CompositeDeltaDiffComputer.class)) {
      // Initialize service
      defragService = new SnapshotDefragService(
          10000, // interval
          TimeUnit.MILLISECONDS,
          60000, // timeout
          ozoneManager,
          configuration
      );
      assertEquals(1, compositeDeltaDiffComputer.constructed().size());
    }

  }

  @AfterEach
  public void tearDown() throws Exception {
    if (defragService != null) {
      defragService.shutdown();
    }
    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  public void testServiceStartAndPause() {
    defragService.start();
    assertTrue(defragService.getSnapshotsDefraggedCount().get() >= 0);

    defragService.pause();
    assertNotNull(defragService);

    defragService.resume();
    assertNotNull(defragService);
  }

  @Test
  public void testNeedsDefragmentationAlreadyDefragmented() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(snapshotId, "vol1", "bucket1", "snap1");

    WritableOmSnapshotLocalDataProvider provider = mock(WritableOmSnapshotLocalDataProvider.class);
    OmSnapshotLocalData localData = mock(OmSnapshotLocalData.class);
    OmSnapshotLocalData previousLocalData = mock(OmSnapshotLocalData.class);

    when(snapshotLocalDataManager.getWritableOmSnapshotLocalData(snapshotInfo)).thenReturn(provider);
    when(provider.needsDefrag()).thenReturn(false);
    when(provider.getSnapshotLocalData()).thenReturn(localData);
    when(provider.getPreviousSnapshotLocalData()).thenReturn(previousLocalData);
    when(localData.getVersion()).thenReturn(1);
    when(previousLocalData.getVersion()).thenReturn(0);


    OmSnapshotLocalData.VersionMeta versionInfo = mock(OmSnapshotLocalData.VersionMeta.class);
    when(versionInfo.getPreviousSnapshotVersion()).thenReturn(0);
    Map<Integer, OmSnapshotLocalData.VersionMeta> versionMap = ImmutableMap.of(1, versionInfo);
    when(localData.getVersionSstFileInfos()).thenReturn(versionMap);

    Pair<Boolean, Integer> result = defragService.needsDefragmentation(snapshotInfo);

    assertFalse(result.getLeft());
    assertEquals(1, result.getRight());
    verify(provider).commit();
    verify(provider).close();
  }

  @Test
  public void testNeedsDefragmentationRequiresDefrag() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(snapshotId, "vol1", "bucket1", "snap1");

    WritableOmSnapshotLocalDataProvider provider = mock(WritableOmSnapshotLocalDataProvider.class);
    OmSnapshotLocalData localData = mock(OmSnapshotLocalData.class);
    AtomicInteger commit = new AtomicInteger(0);
    when(snapshotLocalDataManager.getWritableOmSnapshotLocalData(snapshotInfo)).thenReturn(provider);
    when(provider.getSnapshotLocalData()).thenReturn(localData);
    doAnswer(invocationOnMock -> {
      commit.incrementAndGet();
      return null;
    }).when(provider).commit();
    when(provider.needsDefrag()).thenAnswer(i -> commit.get() == 1);
    int version = ThreadLocalRandom.current().nextInt(100);
    when(localData.getVersion()).thenReturn(version);

    Pair<Boolean, Integer> result = defragService.needsDefragmentation(snapshotInfo);

    assertTrue(result.getLeft());
    assertEquals(version, result.getRight());
    verify(provider).close();
  }

  /**
   * Helper method to create a mock SnapshotInfo.
   */
  private SnapshotInfo createMockSnapshotInfo(UUID snapshotId, String volume,
                                                String bucket, String name) {
    SnapshotInfo.Builder builder = SnapshotInfo.newBuilder();
    builder.setSnapshotId(snapshotId);
    builder.setVolumeName(volume);
    builder.setBucketName(bucket);
    builder.setName(name);
    builder.setSnapshotStatus(SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE);
    builder.setCreationTime(System.currentTimeMillis());
    return builder.build();
  }
}
