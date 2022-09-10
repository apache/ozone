/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import java.io.File;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.ozone.test.GenericTestUtils;

import org.apache.commons.io.FileUtils;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Testing OMStorage class.
 */
public class TestOMStorage {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  /**
   * Test {@link OMStorage#getOmDbDir}.
   */
  @Test
  public void testGetOmDbDir() {
    final File testDir = createTestDir();
    final File dbDir = new File(testDir, "omDbDir");
    final File metaDir = new File(testDir, "metaDir");   // should be ignored.
    final MutableConfigurationSource conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, dbDir.getPath());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      assertEquals(dbDir, OMStorage.getOmDbDir(conf));
      assertTrue(dbDir.exists());          // should have been created.
    } finally {
      FileUtils.deleteQuietly(dbDir);
    }
  }

  /**
   * Test {@link OMStorage#getOmDbDir} with fallback to OZONE_METADATA_DIRS
   * when OZONE_OM_DB_DIRS is undefined.
   */
  @Test
  public void testGetOmDbDirWithFallback() {
    final File testDir = createTestDir();
    final File metaDir = new File(testDir, "metaDir");
    final MutableConfigurationSource conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      assertEquals(metaDir, OMStorage.getOmDbDir(conf));
      assertTrue(metaDir.exists());        // should have been created.
    } finally {
      FileUtils.deleteQuietly(metaDir);
    }
  }

  @Test
  public void testNoOmDbDirConfigured() {
    thrown.expect(IllegalArgumentException.class);
    OMStorage.getOmDbDir(new OzoneConfiguration());
  }

  public File createTestDir() {
    File dir = new File(GenericTestUtils.getRandomizedTestDir(),
        TestOMStorage.class.getSimpleName());
    dir.mkdirs();
    return dir;
  }
}
