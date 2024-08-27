/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Test OzoneFsUtils.
 */
public class TestOzoneFsUtils {

  @Test
  public void testPaths() {
    assertTrue(OzoneFSUtils.isValidName("/a/b"));
    assertFalse(OzoneFSUtils.isValidName("../../../a/b"));
    assertFalse(OzoneFSUtils.isValidName("/./."));
    assertFalse(OzoneFSUtils.isValidName("/:/"));
    assertFalse(OzoneFSUtils.isValidName("a/b"));
    assertFalse(OzoneFSUtils.isValidName("/a:/b"));
    assertFalse(OzoneFSUtils.isValidName("/a//b"));
  }

  /**
   * Check that OzoneFSUtils.getClientConfig() will correct copy the value of:
   * 1. ozone.hbase.enhancements.enabled to ozone.client.hbase.enhancements.enabled
   * 2. ozone.fs.hsync.enabled           to ozone.client.fs.hsync.enabled
   */
  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void testGetClientConfigConvertConfigs(boolean hbaseEnhancementsEnabled, boolean fsHsyncEnabled) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ENABLED, hbaseEnhancementsEnabled);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, fsHsyncEnabled);

    OzoneClientConfig clientConfig = OzoneFSUtils.getClientConfig(conf);
    assertEquals(hbaseEnhancementsEnabled, clientConfig.getHBaseEnhancementsEnabled());
    assertEquals(fsHsyncEnabled, clientConfig.getFsHsyncEnabled());
  }
}
