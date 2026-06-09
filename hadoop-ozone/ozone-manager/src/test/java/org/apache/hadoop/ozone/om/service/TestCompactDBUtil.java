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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_COMPACTION_SERVICE_BOTTOMMOSTLEVELCOMPACTION;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_COMPACTION_SERVICE_BOTTOMMOSTLEVELCOMPACTION_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link CompactDBUtil}.
 */
class TestCompactDBUtil {

  private OMMetadataManager omMetadataManager;

  @BeforeEach
  void setup(@TempDir File tempDir) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_DB_DIRS, tempDir.getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(conf, null);
  }

  @Test
  void testCompactWithKSkip() {
    assertDoesNotThrow(() ->
        CompactDBUtil.compactTable(omMetadataManager, "keyTable",
            ManagedCompactRangeOptions.BottommostLevelCompaction.kSkip));
  }

  @Test
  void testCompactWithKForce() {
    assertDoesNotThrow(() ->
        CompactDBUtil.compactTable(omMetadataManager, "keyTable",
            ManagedCompactRangeOptions.BottommostLevelCompaction.kForce));
  }

  @Test
  void testCompactWithKIfHaveCompactionFilter() {
    assertDoesNotThrow(() ->
        CompactDBUtil.compactTable(omMetadataManager, "keyTable",
            ManagedCompactRangeOptions.BottommostLevelCompaction.kIfHaveCompactionFilter));
  }

  @Test
  void testCompactInvalidColumnFamily() {
    assertThrows(IOException.class, () ->
        CompactDBUtil.compactTable(omMetadataManager, "nonExistentTable",
            ManagedCompactRangeOptions.BottommostLevelCompaction.kSkip));
  }

  @Test
  void testDefaultConfigValueMapsToKSkip() {
    int defaultValue = OZONE_OM_COMPACTION_SERVICE_BOTTOMMOSTLEVELCOMPACTION_DEFAULT;
    assertEquals(0, defaultValue);
    assertEquals(ManagedCompactRangeOptions.BottommostLevelCompaction.kSkip,
        ManagedCompactRangeOptions.BottommostLevelCompaction.fromRocksId(defaultValue));
  }

  @Test
  void testConfigValueMapsToCorrectEnum() {
    assertEquals(ManagedCompactRangeOptions.BottommostLevelCompaction.kSkip,
        ManagedCompactRangeOptions.BottommostLevelCompaction.fromRocksId(0));
    assertEquals(ManagedCompactRangeOptions.BottommostLevelCompaction.kIfHaveCompactionFilter,
        ManagedCompactRangeOptions.BottommostLevelCompaction.fromRocksId(1));
    assertEquals(ManagedCompactRangeOptions.BottommostLevelCompaction.kForce,
        ManagedCompactRangeOptions.BottommostLevelCompaction.fromRocksId(2));
  }

  @Test
  void testInvalidConfigValueReturnsNull() {
    assertNull(ManagedCompactRangeOptions.BottommostLevelCompaction.fromRocksId(99));
  }

  @Test
  void testConfigKeyIsReadFromOzoneConfiguration() {
    OzoneConfiguration conf = new OzoneConfiguration();
    // Default - not set, should use the default value
    assertEquals(0, conf.getInt(OZONE_OM_COMPACTION_SERVICE_BOTTOMMOSTLEVELCOMPACTION,
        OZONE_OM_COMPACTION_SERVICE_BOTTOMMOSTLEVELCOMPACTION_DEFAULT));

    // Override to kForce
    conf.setInt(OZONE_OM_COMPACTION_SERVICE_BOTTOMMOSTLEVELCOMPACTION, 2);
    int compactionType = conf.getInt(
        OZONE_OM_COMPACTION_SERVICE_BOTTOMMOSTLEVELCOMPACTION,
        OZONE_OM_COMPACTION_SERVICE_BOTTOMMOSTLEVELCOMPACTION_DEFAULT);
    assertEquals(ManagedCompactRangeOptions.BottommostLevelCompaction.kForce,
        ManagedCompactRangeOptions.BottommostLevelCompaction.fromRocksId(compactionType));
  }
}
