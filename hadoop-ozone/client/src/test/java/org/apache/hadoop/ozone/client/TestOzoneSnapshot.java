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

package org.apache.hadoop.ozone.client;

import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test class for OzoneSnapshot class.
 */
public class TestOzoneSnapshot {

  private SnapshotInfo getMockedSnapshotInfo(UUID snapshotId) {
    SnapshotInfo snapshotInfo = Mockito.mock(SnapshotInfo.class);
    when(snapshotInfo.getVolumeName()).thenReturn("volume");
    when(snapshotInfo.getBucketName()).thenReturn("bucket");
    when(snapshotInfo.getName()).thenReturn("snap");
    when(snapshotInfo.getCreationTime()).thenReturn(1000L);
    when(snapshotInfo.getSnapshotStatus()).thenReturn(SNAPSHOT_ACTIVE);
    when(snapshotInfo.getSnapshotId()).thenReturn(snapshotId);
    when(snapshotInfo.getSnapshotPath()).thenReturn("volume/bucket");
    when(snapshotInfo.getCheckpointDirName(eq(0))).thenReturn("checkpointDir");
    when(snapshotInfo.getReferencedSize()).thenReturn(1000L);
    when(snapshotInfo.getReferencedReplicatedSize()).thenReturn(3000L);
    when(snapshotInfo.getExclusiveSize()).thenReturn(4000L);
    when(snapshotInfo.getExclusiveReplicatedSize()).thenReturn(12000L);
    when(snapshotInfo.getExclusiveSizeDeltaFromDirDeepCleaning()).thenReturn(2000L);
    when(snapshotInfo.getExclusiveReplicatedSizeDeltaFromDirDeepCleaning()).thenReturn(6000L);
    return snapshotInfo;
  }

  @Test
  public void testOzoneSnapshotFromSnapshotInfo() {
    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapshotInfo = getMockedSnapshotInfo(snapshotId);
    OzoneSnapshot ozoneSnapshot = OzoneSnapshot.fromSnapshotInfo(snapshotInfo);
    OzoneSnapshot expectedOzoneSnapshot = new OzoneSnapshot(
        "volume", "bucket", "snap", 1000L, SNAPSHOT_ACTIVE, snapshotId,
        "volume/bucket", "checkpointDir", 1000L, 3000L, 6000L, 18000L);
    assertEquals(expectedOzoneSnapshot, ozoneSnapshot);
  }
}
