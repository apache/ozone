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
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.hadoop.test.LambdaTestUtils;
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

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;

@RunWith(Parameterized.class)
public class TestSCMHAUnfinalizedStateValidationAction {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final boolean initAfterUpgrade;
  private final boolean haEnabledBefore;
  private final boolean haEnabledAfter;
  private final boolean shouldFail;
  private final String dataPath;

  @Parameterized.Parameters(name = "habefore={0} haafter={1} init={2}")
  public static Collection<Object[]> cases() {
    List<Object[]> params = new ArrayList<>();


    for (boolean haBefore: Arrays.asList(true, false)) {
      for (boolean haAfter: Arrays.asList(true, false)) {
        for (boolean initAfter: Arrays.asList(true, false)) {
          params.add(new Object[]{haBefore, haAfter, initAfter});

        }
      }
    }

    return params;
  }

  public TestSCMHAUnfinalizedStateValidationAction(
      boolean haEnabledBefore, boolean haEnabledAfter, boolean initAfterUpgrade)
      throws Exception {
    this.initAfterUpgrade = initAfterUpgrade;
    this.haEnabledBefore = haEnabledBefore;
    this.haEnabledAfter = haEnabledAfter;

    shouldFail = !this.haEnabledBefore && this.haEnabledAfter;

    temporaryFolder.create();
    dataPath = temporaryFolder.newFolder().getAbsolutePath();
  }

  /*
  ha/no upgrade-pre-f+ha/no noinit+start/init+start(if init passes)
  haEnabledBefore, haEnabledAfter, initAfterUpgrade

  Do not need to test when action runs, only conditions around it running.
  Do not need to test fresh install, as other tests do that.
  fresh init ha/no start
   */

  @Test
  public void testUpgrade() throws Exception {
    // Write version file for original version.
    OzoneConfiguration conf = new OzoneConfiguration();
    // Write the lowest layout version to the file, since clusters using
    // scm ha before upgrades was introduced will not have a version
    // associated with it.
    conf.setInt(ScmConfig.ConfigStrings.HDDS_SCM_INIT_DEFAULT_LAYOUT_VERSION,
        HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, haEnabledBefore);
    conf.set(ScmConfigKeys.OZONE_SCM_DB_DIRS, dataPath);
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, dataPath);
    StorageContainerManager.scmInit(conf, UUID.randomUUID().toString());


    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, haEnabledAfter);

    if (initAfterUpgrade) {
      if (shouldFail) {
        LambdaTestUtils.intercept(UpgradeException.class,
            () -> StorageContainerManager.scmInit(conf, ""));
      } else {
        StorageContainerManager.scmInit(conf, UUID.randomUUID().toString());
      }
    }

    StorageContainerManager scm = TestUtils.getScm(conf);

    if (shouldFail) {
      LambdaTestUtils.intercept(UpgradeException.class,
          scm::start);
    } else {
      scm.start();
      scm.stop();
    }
  }
}
