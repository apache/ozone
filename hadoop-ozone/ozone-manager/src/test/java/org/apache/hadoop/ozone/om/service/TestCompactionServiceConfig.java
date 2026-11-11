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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for compaction service configuration.
 */
class TestCompactionServiceConfig {

  @Test
  void testDefaults() {
    OzoneConfiguration conf = new OzoneConfiguration();
    assertEquals("false", conf.get("ozone.om.compaction.service.enabled"));
    assertEquals("6h", conf.get("ozone.om.compaction.service.run.interval"));
    assertEquals("10m", conf.get("ozone.om.compaction.service.timeout"));
    assertEquals("kSkip", conf.get("ozone.om.compaction.service.bottommost-level-compaction"));
    CompactionServiceConfig config = conf.getObject(CompactionServiceConfig.class);
    assertFalse(config.isEnabled());
    assertEquals(TimeUnit.HOURS.toMillis(6), config.getRunInterval());
    assertEquals(TimeUnit.MINUTES.toMillis(10), config.getTimeout());
    assertEquals("keyTable,fileTable,directoryTable,deletedTable,deletedDirectoryTable,multipartInfoTable",
        config.getColumnFamilies());
    assertEquals(config.getColumnFamilies(), conf.get("ozone.om.compaction.service.columnfamilies"));
    assertEquals(ManagedCompactRangeOptions.BottommostLevelCompaction.kSkip, config.getBottommostLevelCompaction());
  }

  @Test
  void testConfiguredValues() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean("ozone.om.compaction.service.enabled", true);
    conf.set("ozone.om.compaction.service.run.interval", "30s");
    conf.set("ozone.om.compaction.service.timeout", "2m");
    conf.set("ozone.om.compaction.service.columnfamilies", "keyTable,fileTable");
    CompactionServiceConfig config = conf.getObject(CompactionServiceConfig.class);
    assertTrue(config.isEnabled());
    assertEquals(30000, config.getRunInterval());
    assertEquals(120000, config.getTimeout());
    assertEquals("keyTable,fileTable", config.getColumnFamilies());
  }

  @ParameterizedTest
  @ValueSource(strings = {"kForce", " kForce", "kForce ", " kForce "})
  void testBottommostLevelCompaction(String value) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.om.compaction.service.bottommost-level-compaction", value);
    assertEquals(ManagedCompactRangeOptions.BottommostLevelCompaction.kForce,
        conf.getObject(CompactionServiceConfig.class).getBottommostLevelCompaction());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "kForceeee"})
  void testInvalidBottommostLevelCompaction(String value) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.om.compaction.service.bottommost-level-compaction", value);
    assertEquals(ManagedCompactRangeOptions.BottommostLevelCompaction.kSkip,
        conf.getObject(CompactionServiceConfig.class).getBottommostLevelCompaction());
  }

  @Test
  void testEmptyColumnFamiliesAndZeroTimeout() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.om.compaction.service.columnfamilies", "");
    conf.set("ozone.om.compaction.service.timeout", "0ms");
    CompactionServiceConfig config = conf.getObject(CompactionServiceConfig.class);
    assertEquals("", config.getColumnFamilies());
    assertEquals(0, config.getTimeout());
  }
}
