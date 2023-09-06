/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotStatusProto;

import org.apache.hadoop.util.Time;
import org.junit.Test;
import org.junit.Assert;

import java.util.UUID;

import static org.apache.hadoop.hdds.HddsUtils.toProtobuf;

/**
 * Tests SnapshotInfo metadata data structure holding state info for
 * object storage snapshots.
 */
public class TestOmSnapshotInfo {

  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final String NAME = "snapshot1";
  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME = "bucket1";
  private static final SnapshotStatus SNAPSHOT_STATUS =
      SnapshotStatus.SNAPSHOT_ACTIVE;
  private static final long CREATION_TIME = Time.now();
  private static final long DELETION_TIME = -1;
  private static final UUID PATH_PREVIOUS_SNAPSHOT_ID = UUID.randomUUID();
  private static final UUID GLOBAL_PREVIOUS_SNAPSHOT_ID =
      PATH_PREVIOUS_SNAPSHOT_ID;
  private static final String SNAPSHOT_PATH = "test/path";
  private static final String CHECKPOINT_DIR = "checkpoint.testdir";
  private static final long DB_TX_SEQUENCE_NUMBER = 12345L;

  private SnapshotInfo createSnapshotInfo() {
    return new SnapshotInfo.Builder()
        .setSnapshotId(SNAPSHOT_ID)
        .setName(NAME)
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setSnapshotStatus(SNAPSHOT_STATUS)
        .setCreationTime(CREATION_TIME)
        .setDeletionTime(DELETION_TIME)
        .setPathPreviousSnapshotId(PATH_PREVIOUS_SNAPSHOT_ID)
        .setGlobalPreviousSnapshotId(GLOBAL_PREVIOUS_SNAPSHOT_ID)
        .setSnapshotPath(SNAPSHOT_PATH)
        .setCheckpointDir(CHECKPOINT_DIR)
        .setDbTxSequenceNumber(DB_TX_SEQUENCE_NUMBER)
        .setDeepClean(true)
        .setSstFiltered(false)
        .setReferencedSize(2000L)
        .setReferencedReplicatedSize(6000L)
        .setExclusiveSize(1000L)
        .setExclusiveReplicatedSize(3000L)
        .build();
  }

  private OzoneManagerProtocolProtos.SnapshotInfo createSnapshotInfoProto() {
    return OzoneManagerProtocolProtos.SnapshotInfo.newBuilder()
        .setSnapshotID(toProtobuf(SNAPSHOT_ID))
        .setName(NAME)
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setSnapshotStatus(SnapshotStatusProto.SNAPSHOT_ACTIVE)
        .setCreationTime(CREATION_TIME)
        .setDeletionTime(DELETION_TIME)
        .setPathPreviousSnapshotID(toProtobuf(PATH_PREVIOUS_SNAPSHOT_ID))
        .setGlobalPreviousSnapshotID(toProtobuf(GLOBAL_PREVIOUS_SNAPSHOT_ID))
        .setSnapshotPath(SNAPSHOT_PATH)
        .setCheckpointDir(CHECKPOINT_DIR)
        .setDbTxSequenceNumber(DB_TX_SEQUENCE_NUMBER)
        .setDeepClean(true)
        .setSstFiltered(false)
        .setReferencedSize(2000L)
        .setReferencedReplicatedSize(6000L)
        .setExclusiveSize(1000L)
        .setExclusiveReplicatedSize(3000L)
        .build();
  }

  @Test
  public void testSnapshotStatusProtoToObject() {
    OzoneManagerProtocolProtos.SnapshotInfo snapshotInfoEntry =
        createSnapshotInfoProto();
    Assert.assertEquals(SNAPSHOT_STATUS,
        SnapshotStatus.valueOf(snapshotInfoEntry.getSnapshotStatus()));
  }

  @Test
  public void testSnapshotInfoToProto() {
    SnapshotInfo snapshotInfo = createSnapshotInfo();
    OzoneManagerProtocolProtos.SnapshotInfo snapshotInfoEntryExpected =
        createSnapshotInfoProto();

    OzoneManagerProtocolProtos.SnapshotInfo snapshotInfoEntryActual =
        snapshotInfo.getProtobuf();
    Assert.assertEquals(snapshotInfoEntryExpected.getSnapshotID(),
        snapshotInfoEntryActual.getSnapshotID());
    Assert.assertEquals(snapshotInfoEntryExpected.getName(),
        snapshotInfoEntryActual.getName());
    Assert.assertEquals(snapshotInfoEntryExpected.getVolumeName(),
        snapshotInfoEntryActual.getVolumeName());
    Assert.assertEquals(snapshotInfoEntryExpected.getBucketName(),
        snapshotInfoEntryActual.getBucketName());
    Assert.assertEquals(snapshotInfoEntryExpected.getSnapshotStatus(),
        snapshotInfoEntryActual.getSnapshotStatus());
    Assert.assertEquals(snapshotInfoEntryExpected.getDbTxSequenceNumber(),
        snapshotInfoEntryActual.getDbTxSequenceNumber());
    Assert.assertEquals(snapshotInfoEntryExpected.getDeepClean(),
        snapshotInfoEntryActual.getDeepClean());
    Assert.assertEquals(snapshotInfoEntryExpected.getSstFiltered(),
        snapshotInfoEntryActual.getSstFiltered());
    Assert.assertEquals(snapshotInfoEntryExpected.getReferencedSize(),
        snapshotInfoEntryActual.getReferencedSize());
    Assert.assertEquals(snapshotInfoEntryExpected.getReferencedReplicatedSize(),
        snapshotInfoEntryActual.getReferencedReplicatedSize());
    Assert.assertEquals(snapshotInfoEntryExpected.getExclusiveSize(),
        snapshotInfoEntryActual.getExclusiveSize());
    Assert.assertEquals(snapshotInfoEntryExpected.getExclusiveReplicatedSize(),
        snapshotInfoEntryActual.getExclusiveReplicatedSize());

    Assert.assertEquals(snapshotInfoEntryExpected, snapshotInfoEntryActual);
  }

  @Test
  public void testSnapshotInfoProtoToSnapshotInfo() {
    SnapshotInfo snapshotInfoExpected = createSnapshotInfo();
    OzoneManagerProtocolProtos.SnapshotInfo snapshotInfoEntry =
        createSnapshotInfoProto();

    SnapshotInfo snapshotInfoActual = SnapshotInfo
        .getFromProtobuf(snapshotInfoEntry);
    Assert.assertEquals(snapshotInfoExpected.getSnapshotId(),
        snapshotInfoActual.getSnapshotId());
    Assert.assertEquals(snapshotInfoExpected.getName(),
        snapshotInfoActual.getName());
    Assert.assertEquals(snapshotInfoExpected.getVolumeName(),
        snapshotInfoActual.getVolumeName());
    Assert.assertEquals(snapshotInfoExpected.getBucketName(),
        snapshotInfoActual.getBucketName());
    Assert.assertEquals(snapshotInfoExpected.getSnapshotStatus(),
        snapshotInfoActual.getSnapshotStatus());
    Assert.assertEquals(snapshotInfoExpected.getDbTxSequenceNumber(),
        snapshotInfoActual.getDbTxSequenceNumber());
    Assert.assertEquals(snapshotInfoExpected.getDeepClean(),
        snapshotInfoActual.getDeepClean());
    Assert.assertEquals(snapshotInfoExpected.isSstFiltered(),
        snapshotInfoActual.isSstFiltered());
    Assert.assertEquals(snapshotInfoExpected.getReferencedSize(),
        snapshotInfoActual.getReferencedSize());
    Assert.assertEquals(snapshotInfoExpected.getReferencedReplicatedSize(),
        snapshotInfoActual.getReferencedReplicatedSize());
    Assert.assertEquals(snapshotInfoExpected.getExclusiveSize(),
        snapshotInfoActual.getExclusiveSize());
    Assert.assertEquals(snapshotInfoExpected.getExclusiveReplicatedSize(),
        snapshotInfoActual.getExclusiveReplicatedSize());

    Assert.assertEquals(snapshotInfoExpected, snapshotInfoActual);
  }

  @Test
  public void testGenerateName() {
    // GMT: Sunday, July 10, 2022 7:56:55.001 PM
    long millis = 1657483015001L;
    String name = SnapshotInfo.generateName(millis);
    Assert.assertEquals("s20220710-195655.001", name);
  }
}
