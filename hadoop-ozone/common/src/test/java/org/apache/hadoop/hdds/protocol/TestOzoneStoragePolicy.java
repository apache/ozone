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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.StoragePolicyProto;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OzoneStoragePolicy}.
 */
public class TestOzoneStoragePolicy {

  @Test
  public void defaultPolicyIsWarm() {
    assertEquals(OzoneStoragePolicy.WARM, OzoneStoragePolicy.getDefault());
  }

  @Test
  public void primaryStorageTypes() {
    assertEquals(StorageType.SSD,
        OzoneStoragePolicy.HOT.getPrimaryStorageType());
    assertEquals(StorageType.DISK,
        OzoneStoragePolicy.WARM.getPrimaryStorageType());
    assertEquals(StorageType.ARCHIVE,
        OzoneStoragePolicy.COLD.getPrimaryStorageType());
  }

  @Test
  public void fallbackStorageTypes() {
    assertEquals(StorageType.DISK,
        OzoneStoragePolicy.HOT.getFallbackStorageType());
    assertNull(OzoneStoragePolicy.WARM.getFallbackStorageType());
    assertNull(OzoneStoragePolicy.COLD.getFallbackStorageType());
  }

  @Test
  public void toProtoRoundTrip() {
    for (OzoneStoragePolicy policy : OzoneStoragePolicy.values()) {
      StoragePolicyProto proto = policy.toProto();
      OzoneStoragePolicy recovered = OzoneStoragePolicy.fromProto(proto);
      assertEquals(policy, recovered);
    }
  }

  @Test
  public void fromProtoUnsetReturnsNull() {
    assertNull(OzoneStoragePolicy.fromProto(
        StoragePolicyProto.STORAGE_POLICY_UNSET));
  }

  @Test
  public void fromProtoNullReturnsNull() {
    assertNull(OzoneStoragePolicy.fromProto(null));
  }

  @Test
  public void fromStringCaseInsensitive() {
    assertEquals(OzoneStoragePolicy.HOT,
        OzoneStoragePolicy.fromString("HOT"));
    assertEquals(OzoneStoragePolicy.HOT,
        OzoneStoragePolicy.fromString("hot"));
    assertEquals(OzoneStoragePolicy.WARM,
        OzoneStoragePolicy.fromString("Warm"));
    assertEquals(OzoneStoragePolicy.COLD,
        OzoneStoragePolicy.fromString("cold"));
  }

  @Test
  public void fromStringInvalidThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> OzoneStoragePolicy.fromString("INVALID"));
  }
}
