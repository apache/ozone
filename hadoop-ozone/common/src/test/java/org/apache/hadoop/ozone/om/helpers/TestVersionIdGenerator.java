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

import static org.apache.hadoop.ozone.om.helpers.VersionIdGenerator.FIRST_VERSION_ID;
import static org.apache.hadoop.ozone.om.helpers.VersionIdGenerator.NULL_VERSION_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests the constraints every {@link VersionIdGenerator} has to satisfy, and
 * how the cluster-wide generator is selected.
 */
public class TestVersionIdGenerator {

  /** The first transaction index that is not a reserved versionId. */
  private static final long FIRST_USABLE_INDEX = FIRST_VERSION_ID + 1;

  /** Every generator shipped with Ozone; extended as generators are added. */
  static Stream<VersionIdGenerator> generators() {
    return Stream.of(new TransactionIndexVersionIdGenerator(),
        new PinnedFirstVersionIdGenerator());
  }

  @ParameterizedTest
  @MethodSource("generators")
  void generatedIdsStrictlyIncreaseWithinAKey(VersionIdGenerator generator) {
    // The contract every generator owes VersionIdAllocator: over the life of a
    // key, ids only ever go up, starting from the key's first version.
    long previous = generator.generateVersionId(FIRST_USABLE_INDEX, false);
    for (long index = FIRST_USABLE_INDEX + 1; index < 100; index++) {
      long current = generator.generateVersionId(index, true);
      assertTrue(previous < current,
          "versionId " + current + " generated for transaction " + index
              + " does not exceed " + previous);
      previous = current;
    }
  }

  @ParameterizedTest
  @MethodSource("generators")
  void generatedIdsNeverCollideWithReservedIds(VersionIdGenerator generator) {
    for (long index = FIRST_USABLE_INDEX; index < 100; index++) {
      assertNotEquals(NULL_VERSION_ID, generator.generateVersionId(index, true));
      assertNotEquals(NULL_VERSION_ID, generator.generateVersionId(index, false));
    }
    // A transaction index that lands on a reserved id is a misconfiguration of
    // the Ratis log rather than something to silently work around.
    assertThrows(IllegalArgumentException.class,
        () -> generator.generateVersionId(NULL_VERSION_ID, true));
    assertThrows(IllegalArgumentException.class,
        () -> generator.generateVersionId(FIRST_VERSION_ID, true));
  }

  @ParameterizedTest
  @MethodSource("generators")
  void generationIsDeterministic(VersionIdGenerator generator) {
    assertEquals(generator.generateVersionId(4242, true),
        generator.generateVersionId(4242, true));
    assertEquals(generator.generateVersionId(4242, false),
        generator.generateVersionId(4242, false));
  }

  @Test
  void reservedIdsDoNotCollide() {
    assertNotEquals(NULL_VERSION_ID, FIRST_VERSION_ID);
  }

  @Test
  void transactionIndexGeneratorIsTheDefault() {
    assertInstanceOf(TransactionIndexVersionIdGenerator.class,
        VersionIdGenerator.fromConfiguration(new OzoneConfiguration()));
  }

  @Test
  void transactionIndexIgnoresWhetherTheKeyHasACurrentVersion() {
    VersionIdGenerator generator = new TransactionIndexVersionIdGenerator();

    assertEquals(7, generator.generateVersionId(7, false));
    assertEquals(7, generator.generateVersionId(7, true));
  }

  @Test
  void pinnedFirstPinsOnlyTheFirstVersionOfAKey() {
    VersionIdGenerator generator = new PinnedFirstVersionIdGenerator();

    assertEquals(FIRST_VERSION_ID, generator.generateVersionId(7, false));
    assertEquals(7, generator.generateVersionId(7, true));
  }

  @Test
  void pinnedFirstSentinelIsOlderThanEveryTransactionIndex() {
    VersionIdGenerator generator = new PinnedFirstVersionIdGenerator();
    long first = generator.generateVersionId(FIRST_USABLE_INDEX, false);

    for (long index = FIRST_USABLE_INDEX; index < 100; index++) {
      assertTrue(first < generator.generateVersionId(index, true),
          "sentinel " + first + " is not older than the version at transaction " + index);
    }
  }

  @Test
  void pinnedFirstSentinelIsNotTheNullVersion() {
    assertNotEquals(NULL_VERSION_ID,
        new PinnedFirstVersionIdGenerator().generateVersionId(7, false));
  }

  @Test
  void pinnedFirstGeneratorIsSelectableByConfiguration() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_VERSIONING_VERSION_ID_GENERATOR,
        PinnedFirstVersionIdGenerator.class.getName());

    assertInstanceOf(PinnedFirstVersionIdGenerator.class,
        VersionIdGenerator.fromConfiguration(conf));
  }

  @Test
  void generatorClassIsReadFromConfiguration() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_VERSIONING_VERSION_ID_GENERATOR,
        TransactionIndexVersionIdGenerator.class.getName());

    assertInstanceOf(TransactionIndexVersionIdGenerator.class,
        VersionIdGenerator.fromConfiguration(conf));
  }

  @Test
  void unknownGeneratorClassIsRejected() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_VERSIONING_VERSION_ID_GENERATOR,
        "org.apache.hadoop.ozone.om.helpers.NoSuchVersionIdGenerator");

    assertThrows(RuntimeException.class, () -> VersionIdGenerator.fromConfiguration(conf));
  }

  @Test
  void generatorClassNotImplementingTheInterfaceIsRejected() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_VERSIONING_VERSION_ID_GENERATOR,
        String.class.getName());

    assertThrows(RuntimeException.class, () -> VersionIdGenerator.fromConfiguration(conf));
  }
}
