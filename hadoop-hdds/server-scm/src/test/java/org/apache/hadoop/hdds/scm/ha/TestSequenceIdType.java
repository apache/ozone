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

package org.apache.hadoop.hdds.scm.ha;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SequenceIdType}.
 */
public class TestSequenceIdType {

  @Test
  @SuppressWarnings("deprecation")
  public void testStringSyncWithEnumConstants() {
    // Even though we changed to an Enum, these underlying strings are
    // persisted in RocksDB. Do not change them.
    assertEquals("localId", SequenceIdType.LOCAL_ID.getDbKey());
    assertEquals("delTxnId", SequenceIdType.DEL_TXN_ID.getDbKey());
    assertEquals("containerId", SequenceIdType.CONTAINER_ID.getDbKey());
    assertEquals("CertificateId", SequenceIdType.CERTIFICATE_ID.getDbKey());
    assertEquals("rootCertificateId", SequenceIdType.ROOT_CERTIFICATE_ID.getDbKey());
  }

  @Test
  public void testIfNewEnumConstantGetsAdded() {
    Set<String> expectedNames = new HashSet<>(Arrays.asList(
        "LOCAL_ID", "DEL_TXN_ID", "CONTAINER_ID",
        "CERTIFICATE_ID", "ROOT_CERTIFICATE_ID"));

    Set<String> actualNames = new HashSet<>();
    for (SequenceIdType type : SequenceIdType.values()) {
      actualNames.add(type.name());
    }

    // Filter exactly what changed to make the failure message extremely clear
    Set<String> added = new HashSet<>(actualNames);
    added.removeAll(expectedNames);

    Set<String> removed = new HashSet<>(expectedNames);
    removed.removeAll(actualNames);

    assertTrue(added.isEmpty() && removed.isEmpty(),
        () -> "SequenceIdType constants changed!\n" +
            "Unexpectedly Added: " + added + "\n" +
            "Unexpectedly Removed: " + removed + "\n" +
            "ACTION REQUIRED: If this change is intentional, you MUST verify " +
            "RocksDB backward compatibility and update this test's expectedNames.");
  }
}
