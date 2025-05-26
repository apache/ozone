package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


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
    byte[] oldFormatData = oldCodec.toPersistedFormat(original);

    // Step 3: Deserialize using the new default codec (with Protobuf + JSON fallback)
    SnapshotDiffJob parsed = newCodec.fromPersistedFormat(oldFormatData);

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
}
