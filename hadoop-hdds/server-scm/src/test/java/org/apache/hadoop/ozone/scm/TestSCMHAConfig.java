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
package org.apache.hadoop.ozone.scm;

import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.DefaultConfigManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestSCMHAConfig {

  @TempDir
  private File tempDir;

  @BeforeEach
  void setup() {
    DefaultConfigManager.clearDefaultConfigs();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHAConfig(boolean ratisEnabled) throws IOException {
    OzoneConfiguration conf = newConfiguration(ratisEnabled);
    SCMStorageConfig scmStorageConfig = newStorageConfig(ratisEnabled, conf);
    StorageContainerManager.scmInit(conf, scmStorageConfig.getClusterID());
    assertEquals(ratisEnabled, DefaultConfigManager.getValue(
        ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, !ratisEnabled));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testInvalidHAConfig(boolean ratisEnabled) throws IOException {
    OzoneConfiguration conf = newConfiguration(ratisEnabled);
    SCMStorageConfig scmStorageConfig = newStorageConfig(!ratisEnabled, conf);
    String clusterID = scmStorageConfig.getClusterID();
    assertThrows(ConfigurationException.class,
        () -> StorageContainerManager.scmInit(conf, clusterID));
  }

  private OzoneConfiguration newConfiguration(boolean isRatisEnabled) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS, tempDir.getAbsolutePath());
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, isRatisEnabled);
    return conf;
  }

  private static SCMStorageConfig newStorageConfig(
      boolean ratisEnabled, OzoneConfiguration conf) throws IOException {
    final SCMStorageConfig scmStorageConfig = new SCMStorageConfig(conf);
    scmStorageConfig.setClusterId(UUID.randomUUID().toString());
    scmStorageConfig.setSCMHAFlag(ratisEnabled);
    scmStorageConfig.initialize();
    return scmStorageConfig;
  }

}
