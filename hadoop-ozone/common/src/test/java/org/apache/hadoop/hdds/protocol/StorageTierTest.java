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

package org.apache.hadoop.hdds.protocol;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.client.StorageTier;
import org.junit.jupiter.api.Test;

class StorageTierTest {

  @Test
  void testUniformStorageType() {
    assertTrue(StorageTier.SSD.isUniformStorageType());
    assertTrue(StorageTier.DISK.isUniformStorageType());
    assertTrue(StorageTier.ARCHIVE.isUniformStorageType());
  }

  @Test
  void testGetStorageTypesWithReplicationConfig() {
    ReplicationConfig ratisOne = RatisReplicationConfig.getInstance(ONE);
    ReplicationConfig ratisThree = RatisReplicationConfig.getInstance(THREE);
    ReplicationConfig standaloneOne =
        StandaloneReplicationConfig.getInstance(ONE);
    ReplicationConfig standaloneThree =
        StandaloneReplicationConfig.getInstance(THREE);

    // Assert uniform storage types
    Arrays.asList(StorageTier.SSD, StorageTier.DISK, StorageTier.ARCHIVE, StorageTier.EMPTY)
        .forEach(tier -> assertTrue(tier.isUniformStorageType()));

    for (StorageTier tier : StorageTier.values()) {
      if (!tier.isUniformStorageType()) {
        return;
      }
      for (ReplicationConfig replicationConfig : Arrays.asList(ratisOne, ratisThree,
          standaloneOne, standaloneThree)) {
        List<StorageType> storageTypes = tier.getStorageTypes(replicationConfig);
        if (tier.equals(StorageTier.EMPTY)) {
          assertEquals(0, storageTypes.size());
        } else {
          assertStorageTypes(tier, storageTypes, replicationConfig.getRequiredNodes());
        }
      }
    }
  }

  private void assertStorageTypes(StorageTier tier, List<StorageType> storageTypes,
      int expectedSize) {
    assertEquals(expectedSize, storageTypes.size());
    StorageType expectedType = null;
    switch (tier) {
    case SSD:
      expectedType = StorageType.SSD;
      break;
    case DISK:
      expectedType = StorageType.DISK;
      break;
    case ARCHIVE:
      expectedType = StorageType.ARCHIVE;
      break;
    default:
      break;
    }
    if (expectedType != null) {
      for (StorageType storageType : storageTypes) {
        assertEquals(expectedType, storageType);
      }
    }
  }
}
