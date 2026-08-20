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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.helpers.PinnedFirstVersionIdGenerator.FIRST_VERSION_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.PinnedFirstVersionIdGenerator;
import org.apache.hadoop.ozone.om.helpers.UniqueIdVersionIdGenerator;
import org.apache.hadoop.ozone.om.helpers.VersionIdGenerator;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link VersionIdAllocator}: a proposal is a floor, and the id a version
 * is applied with always comes after the key's current version, however the
 * clock behaved.
 */
public class TestVersionIdAllocator {

  private static final String VOLUME = "vol1";
  private static final String BUCKET = "bucket1";
  private static final String KEY = "key1";

  /** Hands out the readings it was given, standing in for a wall clock. */
  private static VersionIdGenerator clockReading(long... readings) {
    final Deque<Long> queue = new ArrayDeque<>();
    Arrays.stream(readings).forEach(queue::add);
    return () -> queue.size() > 1 ? queue.poll() : queue.peek();
  }

  private static VersionIdAllocator allocator() {
    return new VersionIdAllocator(new UniqueIdVersionIdGenerator());
  }

  private static OmKeyInfo versionWithId(Long versionId) {
    OmKeyInfo.Builder builder = new OmKeyInfo.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(KEY)
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE));
    if (versionId != null) {
      builder.setVersionId(versionId);
    }
    return builder.build();
  }

  @Test
  void theProposalComesFromTheGenerator() {
    VersionIdAllocator allocator =
        new VersionIdAllocator(clockReading(1234L, 5678L));

    assertEquals(1234L, allocator.propose());
    assertEquals(5678L, allocator.propose());
  }

  @Test
  void theFirstVersionOfAKeyTakesTheProposedId() {
    assertEquals(5000L, allocator().allocate(5000L, null));
  }

  @Test
  void aLaterVersionTakesTheProposedIdWhenTheClockMovedOn() {
    assertEquals(5000L, allocator().allocate(5000L, versionWithId(4000L)));
  }

  /**
   * Two versions of one key proposed inside the same millisecond can exhaust
   * the counter that separates them; the second still has to come after the
   * first.
   */
  @Test
  void aRepeatedProposalStillMovesTheKeyForward() {
    assertEquals(4001L, allocator().allocate(4000L, versionWithId(4000L)));
  }

  /**
   * The case no wall clock handles on its own: a leader change onto a node
   * whose clock lags, which would otherwise sort the new version before the one
   * it supersedes.
   */
  @Test
  void aClockThatWentBackwardsDoesNotReorderTheKey() {
    assertEquals(9001L, allocator().allocate(3000L, versionWithId(9000L)));
  }

  @Test
  void everyAllocationComesAfterTheCurrentVersion() {
    VersionIdAllocator allocator = allocator();
    long currentId = 1_000_000L;

    for (long proposed : new long[] {2_000_000L, 1L, 1_000_000L, 2_000_001L}) {
      long allocated = allocator.allocate(proposed, versionWithId(currentId));
      assertTrue(allocated > currentId,
          "allocated " + allocated + " does not come after " + currentId);
      currentId = allocated;
    }
  }

  /**
   * A current version written before versioning was enabled carries no id, and
   * a key in that state has no other versions to order against.
   */
  @Test
  void aVersionPredatingVersioningDoesNotConstrainTheId() {
    assertEquals(7000L, allocator().allocate(7000L, versionWithId(null)));
  }

  /**
   * A request that reaches apply without a proposal would otherwise take the
   * reserved unset value as its id, and read back as a version that predates
   * versioning.
   */
  @Test
  void aVersionCannotBeAppliedWithoutAProposal() {
    assertThrows(IllegalArgumentException.class,
        () -> allocator().allocate(VersionIdGenerator.UNSET_VERSION_ID, null));
  }

  /**
   * The floor comes from the current version the write path already holds, so
   * the allocator reads nothing of its own - versionedKeyTable included. It
   * holds no table to read one from.
   */
  @Test
  void theAllocatorReadsNoTable() {
    for (Field field : VersionIdAllocator.class.getDeclaredFields()) {
      if (field.isSynthetic()) {
        continue;
      }
      assertEquals(VersionIdGenerator.class, field.getType(),
          "the allocator should hold nothing but its generator, but holds "
              + field.getType().getName() + " " + field.getName());
    }
  }

  /**
   * Whether the key has a current version is settled under the write's lock, so
   * a generator that numbers the first one specially decides it here.
   */
  @Test
  void thePinnedGeneratorGivesTheFirstVersionTheSentinel() {
    VersionIdAllocator allocator =
        new VersionIdAllocator(new PinnedFirstVersionIdGenerator());

    assertEquals(FIRST_VERSION_ID, allocator.allocate(5000L, null));
  }

  @Test
  void thePinnedGeneratorLeavesLaterVersionsAlone() {
    VersionIdAllocator allocator =
        new VersionIdAllocator(new PinnedFirstVersionIdGenerator());

    assertEquals(5000L,
        allocator.allocate(5000L, versionWithId(FIRST_VERSION_ID)));
  }

  @Test
  void theDefaultGeneratorDoesNotPinTheFirstVersion() {
    assertEquals(5000L, allocator().allocate(5000L, null));
  }

  @Test
  void theAllocatorTakesItsGeneratorFromConfiguration() {
    assertInstanceOf(UniqueIdVersionIdGenerator.class,
        new VersionIdAllocator(new OzoneConfiguration()).getGenerator());
  }
}
