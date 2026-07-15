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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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
   * In these scenarios below, OzoneFSUtils.canEnableHsync() should return false:
   * 1. ozone.hbase.enhancements.allowed = false, ozone.fs.hsync.enabled = false
   * 2. ozone.hbase.enhancements.allowed = false, ozone.fs.hsync.enabled = true
   * 3. ozone.hbase.enhancements.allowed = true,  ozone.fs.hsync.enabled = false
   * <p>
   * The only case where OzoneFSUtils.canEnableHsync() would return true:
   * 4. ozone.hbase.enhancements.allowed = true, ozone.fs.hsync.enabled = true
   */
  @ParameterizedTest
  @CsvSource({"false,false,false,false", "false,false,true,false", "false,true,false,false", "true,true,true,false",
              "false,false,false,true",  "false,false,true,true",  "false,true,false,true",  "true,true,true,true"})
  void testCanEnableHsync(boolean canEnableHsync,
                          boolean hbaseEnhancementsEnabled, boolean fsHsyncEnabled,
                          boolean isClient) {
    OzoneConfiguration conf = new OzoneConfiguration();
    final String confKey = isClient ?
        "ozone.client.hbase.enhancements.allowed" :
        OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED;
    conf.setBoolean(confKey, hbaseEnhancementsEnabled);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, fsHsyncEnabled);

    assertEquals(canEnableHsync, OzoneFSUtils.canEnableHsync(conf, isClient));
  }

  @Test
  public void testIsValidKeyPath() throws OMException {
    // Valid paths
    assertEquals("a/b/c", OzoneFSUtils.isValidKeyPath("a/b/c", true));
    assertEquals("a/b/c", OzoneFSUtils.isValidKeyPath("a/b/c", false));
    assertEquals("file", OzoneFSUtils.isValidKeyPath("file", true));
    assertEquals("file.txt", OzoneFSUtils.isValidKeyPath("file.txt", true));
    assertEquals("dir/subdir/file", OzoneFSUtils.isValidKeyPath("dir/subdir/file", true));

    // Empty path - throwOnEmpty=true should throw exception
    assertThrows(OMException.class, () -> OzoneFSUtils.isValidKeyPath("", true));

    // Empty path - throwOnEmpty=false should return empty string
    assertEquals("", OzoneFSUtils.isValidKeyPath("", false));

    // Invalid paths - leading slash
    assertThrows(OMException.class, () -> OzoneFSUtils.isValidKeyPath("/a/b", true));
    assertThrows(OMException.class, () -> OzoneFSUtils.isValidKeyPath("/a/b", false));

    // Invalid paths - contains ".."
    assertThrows(OMException.class, () -> OzoneFSUtils.isValidKeyPath("a/../b", true));
    assertThrows(OMException.class, () -> OzoneFSUtils.isValidKeyPath("../a/b", true));

    // Invalid paths - contains "."
    assertThrows(OMException.class, () -> OzoneFSUtils.isValidKeyPath("a/./b", true));
    assertThrows(OMException.class, () -> OzoneFSUtils.isValidKeyPath("./file", true));

    // Invalid paths - contains ":"
    assertThrows(OMException.class, () -> OzoneFSUtils.isValidKeyPath("a:b", true));
    assertThrows(OMException.class, () -> OzoneFSUtils.isValidKeyPath("a/b:c", true));

    // Invalid paths - contains "//" in the middle
    assertThrows(OMException.class, () -> OzoneFSUtils.isValidKeyPath("a//b", true));

    // Valid path ending with "/"
    assertEquals("a/b/", OzoneFSUtils.isValidKeyPath("a/b/", true));
  }
}
