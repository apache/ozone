/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.CleanUpManager;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.hadoop.util.ServicePlugin;

import org.junit.After;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;

/**
 * Test class for {@link HddsDatanodeService}.
 */
public class TestHddsDatanodeService {
  private File testDir;
  private OzoneConfiguration conf;
  private HddsDatanodeService service;
  private String[] args = new String[] {};

  @Before
  public void setUp() {
    testDir = GenericTestUtils.getRandomizedTestDir();
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getPath());
    conf.setClass(OzoneConfigKeys.HDDS_DATANODE_PLUGINS_KEY, MockService.class,
        ServicePlugin.class);

    // Tokens only work if security is enabled.  Here we're testing that a
    // misconfig in unsecure cluster does not prevent datanode from starting up.
    // see HDDS-7055
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, false);
    conf.setBoolean(HDDS_BLOCK_TOKEN_ENABLED, true);
    conf.setBoolean(HDDS_CONTAINER_TOKEN_ENABLED, true);

    String volumeDir = testDir + "/disk1";
    conf.set(DFSConfigKeysLegacy.DFS_DATANODE_DATA_DIR_KEY, volumeDir);
  }

  @After
  public void tearDown() throws IOException {
    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void testStartup() throws IOException {
    service = HddsDatanodeService.createHddsDatanodeService(args);
    HddsDatanodeService datanodeService = spy(service);
    datanodeService.start(conf);

    assertNotNull(datanodeService.getDatanodeDetails());
    assertNotNull(datanodeService.getDatanodeDetails().getHostName());
    assertFalse(datanodeService.getDatanodeStateMachine().isDaemonStopped());
    assertNotNull(datanodeService.getCRLStore());

    datanodeService.stop();
    verify(datanodeService, times(2)).cleanTmpDir();
    // CRL store must be stopped when the service stops
    assertNull(datanodeService.getCRLStore().getStore());
    datanodeService.join();
    datanodeService.close();
  }

  static class MockService implements ServicePlugin {

    @Override
    public void close() throws IOException {
      // Do nothing
    }

    @Override
    public void start(Object arg0) {
      // Do nothing
    }

    @Override
    public void stop() {
      // Do nothing
    }
  }
}
