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

package org.apache.hadoop.hdds.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.StorageType;
import org.junit.jupiter.api.Test;

/**
 * Provides {@link StorageTierUtil} factory methods for testing.
 */
class StorageTierUtilTest {
  @Test
  void testFindSupportedStorageTiers() {
    assertStorageTiers(createDnStorageTypes(
            setOf(StorageType.SSD),
            setOf(StorageType.SSD),
            setOf(StorageType.SSD)),
        1, setOf(StorageTier.SSD));

    assertStorageTiers(createDnStorageTypes(
            setOf(StorageType.SSD, StorageType.DISK),
            setOf(StorageType.SSD, StorageType.DISK),
            setOf(StorageType.SSD)),
        1, setOf(StorageTier.SSD));

    assertStorageTiers(createDnStorageTypes(
            setOf(StorageType.SSD, StorageType.DISK),
            setOf(StorageType.SSD, StorageType.DISK),
            setOf(StorageType.SSD, StorageType.DISK)),
        2, setOf(StorageTier.SSD, StorageTier.DISK));

    assertStorageTiers(createDnStorageTypes(
            setOf(StorageType.SSD, StorageType.DISK, StorageType.ARCHIVE),
            setOf(StorageType.SSD, StorageType.DISK),
            setOf(StorageType.SSD, StorageType.DISK)),
        2, setOf(StorageTier.SSD, StorageTier.DISK));

    assertStorageTiers(createDnStorageTypes(
            setOf(StorageType.SSD, StorageType.DISK, StorageType.ARCHIVE),
            setOf(StorageType.SSD, StorageType.DISK, StorageType.ARCHIVE),
            setOf(StorageType.SSD, StorageType.DISK, StorageType.ARCHIVE)),
        3, setOf(StorageTier.SSD, StorageTier.DISK, StorageTier.ARCHIVE));

    assertStorageTiers(createDnStorageTypes(
            setOf(StorageType.SSD, StorageType.DISK)),
        2, setOf(StorageTier.SSD, StorageTier.DISK));

    assertStorageTiers(createDnStorageTypes(
            setOf(StorageType.SSD)),
        1, setOf(StorageTier.SSD));

    assertStorageTiers(createDnStorageTypes(
            setOf(StorageType.DISK)),
        1, setOf(StorageTier.DISK));

    assertStorageTiers(createDnStorageTypes(
            setOf(StorageType.ARCHIVE)),
        1, setOf(StorageTier.ARCHIVE));

    assertStorageTiers(createDnStorageTypes(
            setOf(StorageType.SSD, StorageType.DISK),
            setOf(StorageType.SSD, StorageType.DISK),
            setOf(StorageType.SSD, StorageType.DISK),
            setOf(StorageType.SSD, StorageType.DISK),
            setOf(StorageType.SSD, StorageType.DISK)),
        2, setOf(StorageTier.SSD, StorageTier.DISK));
  }

  @Test
  void testFindInvalidStorageTiers() {
    assertStorageTiers(createDnStorageTypes(),  0, new HashSet<>());

    assertStorageTiers(createDnStorageTypes(
            setOf(StorageType.DISK),
            setOf(StorageType.SSD),
            setOf(StorageType.SSD)),
        0, new HashSet<>());

    assertStorageTiers(createDnStorageTypes(
            setOf(StorageType.DISK),
            setOf(StorageType.DISK),
            setOf(StorageType.SSD)),
        0, new HashSet<>());

    assertStorageTiers(createDnStorageTypes(
            setOf(StorageType.ARCHIVE),
            setOf(StorageType.DISK),
            setOf(StorageType.SSD)),
        0, new HashSet<>());

    assertStorageTiers(createDnStorageTypes(
            setOf(StorageType.RAM_DISK),
            setOf(StorageType.SSD),
            setOf(StorageType.SSD)),
        0, new HashSet<>());

    assertStorageTiers(createDnStorageTypes(
            setOf(StorageType.RAM_DISK),
            setOf(StorageType.RAM_DISK),
            setOf(StorageType.RAM_DISK)),
        0, new HashSet<>());

    assertStorageTiers(createDnStorageTypes(
            setOf(StorageType.PROVIDED),
            setOf(StorageType.PROVIDED),
            setOf(StorageType.PROVIDED)),
        0, new HashSet<>());
  }

  private void assertStorageTiers(List<Set<StorageType>> dnStorageTypes,
      int expectedSize, Set<StorageTier> expectedTiers) {
    List<StorageTier> supportedTiers =
        StorageTierUtil.findSupportedStorageTiers(dnStorageTypes);
    HashSet<StorageTier> tiers = new HashSet<>(supportedTiers);
    assertEquals(expectedSize, tiers.size());
    assertEquals(expectedTiers, tiers);
  }

  private List<Set<StorageType>> createDnStorageTypes(Set<StorageType>... storageSets) {
    return Arrays.asList(storageSets);
  }

  private Set<StorageType> setOf(StorageType... types) {
    return new HashSet<>(Arrays.asList(types));
  }

  private Set<StorageTier> setOf(StorageTier... tiers) {
    return new HashSet<>(Arrays.asList(tiers));
  }
}
