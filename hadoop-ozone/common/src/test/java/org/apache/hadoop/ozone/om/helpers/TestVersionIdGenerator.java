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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests the generators that propose a versionId, and the configuration that
 * selects one.
 */
public class TestVersionIdGenerator {

  /** Every generator, so the contract below is checked against all of them. */
  static Stream<VersionIdGenerator> generators() {
    return Stream.of(new UniqueIdVersionIdGenerator(),
        new PinnedFirstVersionIdGenerator());
  }

  /**
   * The contract every generator owes: ids come out strictly increasing, and
   * above the reserved unset value. A clock can still break this across OMs,
   * which is what the allocator's floor is for.
   */
  @ParameterizedTest
  @MethodSource("generators")
  void proposedIdsStrictlyIncrease(VersionIdGenerator generator) {
    long previous = VersionIdGenerator.UNSET_VERSION_ID;

    for (int i = 0; i < 10000; i++) {
      final long versionId = generator.generateVersionId();
      assertTrue(versionId > previous,
          "proposed " + versionId + " after " + previous);
      previous = versionId;
    }
  }

  @Test
  void theProposedIdReadsAsTheTimeOfTheWrite() {
    final long before = System.currentTimeMillis();
    final long versionId = new UniqueIdVersionIdGenerator().generateVersionId();
    final long after = System.currentTimeMillis();

    final long millis = versionId >> Short.SIZE;
    assertTrue(millis >= before && millis <= after,
        "versionId " + versionId + " does not read as a millisecond between "
            + before + " and " + after);
  }

  /**
   * The default generator keeps nothing of its own, so a fresh instance picks
   * up where the last one left off and no OM has to carry allocator state.
   */
  @Test
  void theDefaultGeneratorHoldsNoState() {
    final long first = new UniqueIdVersionIdGenerator().generateVersionId();
    final long second = new UniqueIdVersionIdGenerator().generateVersionId();

    assertTrue(second > first, "expected " + second + " to come after " + first);
  }

  /** By default the applied id is simply the one that was proposed. */
  @Test
  void theProposalStandsUnlessAGeneratorSaysOtherwise() {
    final VersionIdGenerator generator = new UniqueIdVersionIdGenerator();

    assertEquals(5000L, generator.versionIdFor(5000L, false));
    assertEquals(5000L, generator.versionIdFor(5000L, true));
  }

  /** Only a key's first version is special; later ones are numbered normally. */
  @Test
  void onlyTheFirstVersionIsPinned() {
    final VersionIdGenerator generator = new PinnedFirstVersionIdGenerator();

    assertEquals(PinnedFirstVersionIdGenerator.FIRST_VERSION_ID,
        generator.versionIdFor(5000L, false));
    assertEquals(5000L, generator.versionIdFor(5000L, true));
  }

  /**
   * versionedKeyTable orders versions by Long.MAX_VALUE - versionId, so the
   * sentinel has to stay below every proposed id to sort at the old end of the
   * key.
   */
  @Test
  void theSentinelSortsBeforeEveryProposedId() {
    final long sentinel = PinnedFirstVersionIdGenerator.FIRST_VERSION_ID;
    final long proposed =
        new PinnedFirstVersionIdGenerator().generateVersionId();

    assertTrue(sentinel < proposed,
        "sentinel " + sentinel + " does not sort before " + proposed);
    assertTrue(Long.MAX_VALUE - sentinel > Long.MAX_VALUE - proposed,
        "the sentinel does not sort at the old end of the key");
  }

  /**
   * The null version is marked by isNullVersion and carries a proposed id like
   * any other version, so nothing of it is reserved; the sentinel only has to
   * stay clear of the unset value a pre-versioning record carries.
   */
  @Test
  void theSentinelDoesNotCollideWithTheNullSlot() {
    assertTrue(PinnedFirstVersionIdGenerator.FIRST_VERSION_ID
        > VersionIdGenerator.UNSET_VERSION_ID);
  }

  @Test
  void timeBasedIdsAreTheClusterDefault() {
    assertInstanceOf(UniqueIdVersionIdGenerator.class,
        VersionIdGenerator.fromConfiguration(new OzoneConfiguration()));
  }

  @Test
  void theGeneratorIsSelectedByClassName() {
    assertInstanceOf(FixedVersionIdGenerator.class,
        VersionIdGenerator.fromConfiguration(
            configuredWith(FixedVersionIdGenerator.class.getName())));
  }

  @Test
  void thePinnedGeneratorIsSelectedByClassName() {
    assertInstanceOf(PinnedFirstVersionIdGenerator.class,
        VersionIdGenerator.fromConfiguration(configuredWith(
            PinnedFirstVersionIdGenerator.class.getName())));
  }

  @Test
  void anUnknownGeneratorClassIsRejected() {
    assertThrows(RuntimeException.class,
        () -> VersionIdGenerator.fromConfiguration(
            configuredWith("org.apache.hadoop.ozone.om.helpers.NoSuchThing")));
  }

  @Test
  void aClassThatIsNotAGeneratorIsRejected() {
    assertThrows(RuntimeException.class,
        () -> VersionIdGenerator.fromConfiguration(
            configuredWith(String.class.getName())));
  }

  private static OzoneConfiguration configuredWith(String generatorClass) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_VERSIONING_VERSION_ID_GENERATOR,
        generatorClass);
    return conf;
  }

  /** A generator that exists only to be named in configuration. */
  public static class FixedVersionIdGenerator implements VersionIdGenerator {
    @Override
    public long generateVersionId() {
      return 1L;
    }
  }
}
