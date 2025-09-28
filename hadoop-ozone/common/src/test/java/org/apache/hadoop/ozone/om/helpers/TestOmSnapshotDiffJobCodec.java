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

package org.apache.hadoop.ozone.om.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus;
import org.junit.jupiter.api.Test;

/**
 * Testing serialization of SnapshotDiffJobCodec objects to/from RocksDB.
 */
public class TestOmSnapshotDiffJobCodec {
  private final OldSnapshotDiffJobCodecForTesting oldCodec
      = new OldSnapshotDiffJobCodecForTesting();
  private final Codec<SnapshotDiffJob> newCodec = SnapshotDiffJob.getCodec();

  @Test
  public void testOldJsonSerializedDataCanBeReadByNewCodec() throws Exception {
    // Step 1: Construct a SnapshotDiffJob instance
    SnapshotDiffJob original = new SnapshotDiffJob(
        123456789L,
        "job-001",
        JobStatus.IN_PROGRESS,
        "volA",
        "buckB",
        "snap1",
        "snap2",
        true,
        false,
        100L,
        SubStatus.SST_FILE_DELTA_DAG_WALK,
        0.0
    );

    // Step 2: Serialize using the old Jackson-based codec
    byte[] oldFormatData = oldCodec.toPersistedFormatImpl(original);

    // Step 3: Deserialize using the new default codec (with Protobuf + JSON fallback)
    SnapshotDiffJob parsed = newCodec.fromPersistedFormatImpl(oldFormatData);

    // Step 4: Verify critical fields remain consistent after round-trip
    assertEquals(original.getJobId(), parsed.getJobId());
    assertEquals(original.getStatus(), parsed.getStatus());
    assertEquals(original.getVolume(), parsed.getVolume());
    assertEquals(original.getBucket(), parsed.getBucket());
    assertEquals(original.getFromSnapshot(), parsed.getFromSnapshot());
    assertEquals(original.getToSnapshot(), parsed.getToSnapshot());
    assertEquals(original.isForceFullDiff(), parsed.isForceFullDiff());
    assertEquals(original.isNativeDiffDisabled(), parsed.isNativeDiffDisabled());
    assertEquals(original.getSubStatus(), parsed.getSubStatus());
    assertEquals(original.getTotalDiffEntries(), parsed.getTotalDiffEntries());

    assertEquals(0.0, parsed.getKeysProcessedPct());
  }

  @Test
  public void testCodecBufferSupport() throws Exception {
    assertTrue(newCodec.supportCodecBuffer());

    SnapshotDiffJob original = new SnapshotDiffJob(
        System.currentTimeMillis(),
        "test-job-buffer",
        JobStatus.DONE,
        "testVol",
        "testBucket",
        "fromSnap",
        "toSnap",
        false,
        true,
        500L,
        SubStatus.OBJECT_ID_MAP_GEN_FSO,
        75.5
    );

    // Test with direct allocator
    try (CodecBuffer buffer = newCodec.toCodecBuffer(original, CodecBuffer.Allocator.getDirect())) {
      SnapshotDiffJob decoded = newCodec.fromCodecBuffer(buffer);
      assertSnapshotDiffJobEquals(original, decoded);
    }

    // Test with heap allocator
    try (CodecBuffer buffer = newCodec.toCodecBuffer(original, CodecBuffer.Allocator.getHeap())) {
      SnapshotDiffJob decoded = newCodec.fromCodecBuffer(buffer);
      assertSnapshotDiffJobEquals(original, decoded);
    }
  }

  @Test
  public void testCodecBufferBackwardCompatibility() throws Exception {

    SnapshotDiffJob original = new SnapshotDiffJob(
        987654321L,
        "compat-job",
        JobStatus.FAILED,
        "volX",
        "buckY",
        "oldSnap",
        "newSnap",
        true,
        true,
        0L,
        null,
        0.0
    );
    original.setReason("Test failure reason");


    byte[] jsonData = oldCodec.toPersistedFormatImpl(original);

    // Create a CodecBuffer from the JSON data
    // This simulates reading old format data from storage
    try (CodecBuffer buffer = CodecBuffer.Allocator.getHeap().apply(jsonData.length)) {
      buffer.put(ByteBuffer.wrap(jsonData));

      // The new codec should handle JSON fallback in fromCodecBuffer
      SnapshotDiffJob decoded = newCodec.fromCodecBuffer(buffer);

      assertEquals(original.getJobId(), decoded.getJobId());
      assertEquals(original.getStatus(), decoded.getStatus());
      assertEquals(original.getReason(), decoded.getReason());
    }
  }

  private void assertSnapshotDiffJobEquals(SnapshotDiffJob expected, SnapshotDiffJob actual) {
    assertEquals(expected.getCreationTime(), actual.getCreationTime());
    assertEquals(expected.getJobId(), actual.getJobId());
    assertEquals(expected.getStatus(), actual.getStatus());
    assertEquals(expected.getVolume(), actual.getVolume());
    assertEquals(expected.getBucket(), actual.getBucket());
    assertEquals(expected.getFromSnapshot(), actual.getFromSnapshot());
    assertEquals(expected.getToSnapshot(), actual.getToSnapshot());
    assertEquals(expected.isForceFullDiff(), actual.isForceFullDiff());
    assertEquals(expected.isNativeDiffDisabled(), actual.isNativeDiffDisabled());
    assertEquals(expected.getTotalDiffEntries(), actual.getTotalDiffEntries());
    assertEquals(expected.getSubStatus(), actual.getSubStatus());
    assertEquals(expected.getKeysProcessedPct(), actual.getKeysProcessedPct(), 0.001);
    assertEquals(expected.getReason(), actual.getReason());
  }
}
