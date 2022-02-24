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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

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
@RunWith(Parameterized.class)
public class TestSCMHAUnfinalizedStateValidationAction {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final boolean haEnabledBefore;
  private final boolean haEnabledPreFinalized;
  private final boolean shouldFail;
  private final String dataPath;
  private static final String CLUSTER_ID = UUID.randomUUID().toString();

  @Parameterized.Parameters(name = "haEnabledBefore={0} " +
      "haEnabledPreFinalized={1}")
  public static Collection<Object[]> cases() {
    List<Object[]> params = new ArrayList<>();

    for (boolean haBefore: Arrays.asList(true, false)) {
      for (boolean haAfter: Arrays.asList(true, false)) {
        params.add(new Object[]{haBefore, haAfter});
      }
    }

    return params;
  }

  public TestSCMHAUnfinalizedStateValidationAction(
      boolean haEnabledBefore, boolean haEnabledPreFinalized)
      throws Exception {
    this.haEnabledBefore = haEnabledBefore;
    this.haEnabledPreFinalized = haEnabledPreFinalized;

    shouldFail = !this.haEnabledBefore && this.haEnabledPreFinalized;

    temporaryFolder.create();
    dataPath = temporaryFolder.newFolder().getAbsolutePath();
  }

  @Test
  public void testUpgrade() throws Exception {
    // Write version file for original version.
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ScmConfig.ConfigStrings.HDDS_SCM_INIT_DEFAULT_LAYOUT_VERSION,
        HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, haEnabledBefore);
    conf.set(ScmConfigKeys.OZONE_SCM_DB_DIRS, dataPath);
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, dataPath);
    // This init should always succeed, since SCM is not pre-finalized yet.
    boolean initResult1 = StorageContainerManager.scmInit(conf, CLUSTER_ID);
    Assert.assertTrue(initResult1);

    // Set up new pre-finalized SCM.
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY,
        haEnabledPreFinalized);
    StorageContainerManager scm = HddsTestUtils.getScm(conf);

    Assert.assertEquals(UpgradeFinalizer.Status.FINALIZATION_REQUIRED,
        scm.getUpgradeFinalizer().getStatus());

    if (shouldFail) {
      // Start on its own should fail.
      LambdaTestUtils.intercept(UpgradeException.class, scm::start);

      // Init followed by start should both fail.
      // Init is not necessary here, but is allowed to be run.
      LambdaTestUtils.intercept(UpgradeException.class,
          () -> StorageContainerManager.scmInit(conf, CLUSTER_ID));
      LambdaTestUtils.intercept(UpgradeException.class, scm::start);
    } else {
      boolean initResult2 = StorageContainerManager.scmInit(conf, CLUSTER_ID);
      Assert.assertTrue(initResult2);
      scm.start();
      scm.stop();
    }
  }
}
