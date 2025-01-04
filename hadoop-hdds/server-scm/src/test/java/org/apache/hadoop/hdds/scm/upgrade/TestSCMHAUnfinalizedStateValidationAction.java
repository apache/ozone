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
package org.apache.hadoop.hdds.scm.upgrade;

import org.apache.hadoop.hdds.conf.DefaultConfigManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that the SCM HA pre-finalize validation action is only triggered in
 * pre-finalize startup if SCM HA was not already being used in the cluster,
 * but has been turned on after.
 *
 * Starting a new SCM HA cluster finalized should not trigger the action. This
 * is tested by all other tests that use SCM HA from the latest version of the
 * code.
 *
 * Starting a new cluster finalized without SCM HA enabled should not trigger
 * the action. This is tested by all other tests that run non-HA clusters.
 */
public class TestSCMHAUnfinalizedStateValidationAction {

  private static final String CLUSTER_ID = UUID.randomUUID().toString();

  @BeforeAll
  public static void setup() {
    ExitUtils.disableSystemExit();
  }

  @Test
  public void testUpgrade(@TempDir Path dataPath) throws Exception {
    // Write version file for original version.
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ScmConfig.ConfigStrings.HDDS_SCM_INIT_DEFAULT_LAYOUT_VERSION,
        HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    conf.set(ScmConfigKeys.OZONE_SCM_DB_DIRS, dataPath.toString());
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, dataPath.toString());
    // This init should always succeed, since SCM is not pre-finalized yet.
    DefaultConfigManager.clearDefaultConfigs();
    boolean initResult1 = StorageContainerManager.scmInit(conf, CLUSTER_ID);
    assertTrue(initResult1);

    StorageContainerManager scm = HddsTestUtils.getScm(conf);

    assertEquals(UpgradeFinalizer.Status.FINALIZATION_REQUIRED,
        scm.getFinalizationManager().getUpgradeFinalizer().getStatus());


    DefaultConfigManager.clearDefaultConfigs();
    boolean initResult2 = StorageContainerManager.scmInit(conf, CLUSTER_ID);
    assertTrue(initResult2);
    scm.start();
    scm.stop();

  }
}
