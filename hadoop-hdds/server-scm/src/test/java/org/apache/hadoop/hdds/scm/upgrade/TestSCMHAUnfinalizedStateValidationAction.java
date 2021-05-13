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

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;

@RunWith(Parameterized.class)
public class TestSCMHAUnfinalizedStateValidationAction {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final int layoutVersion;
  private final boolean scmHaAlreadyEnabled;
  private final boolean shouldFail;

  @Parameterized.Parameters(name = "LayoutVersion={0} scmHaAlreadyEnabled={1}")
  public static Collection<Object[]> cases() {
    return Arrays.asList(
        new Object[]{HDDSLayoutFeature.SCM_HA.layoutVersion(), true},
        new Object[]{HDDSLayoutFeature.INITIAL_VERSION.layoutVersion(), true},
        new Object[]{HDDSLayoutFeature.SCM_HA.layoutVersion(), false},
        new Object[]{HDDSLayoutFeature.INITIAL_VERSION.layoutVersion(), false}
    );
  }

  public TestSCMHAUnfinalizedStateValidationAction(
      int layoutVersion, boolean scmHaAlreadyEnabled) {
    this.layoutVersion = layoutVersion;
    this.scmHaAlreadyEnabled = scmHaAlreadyEnabled;

    shouldFail =
        !this.scmHaAlreadyEnabled &&
            this.layoutVersion < HDDSLayoutFeature.SCM_HA.layoutVersion();
  }

  @Test
  public void testSCMHAValidationAction() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    conf.setInt(ScmConfig.ConfigStrings.HDDS_SCM_INIT_DEFAULT_LAYOUT_VERSION,
        layoutVersion);
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        temporaryFolder.newFolder().getAbsolutePath());

    StorageContainerManager.scmInit(conf, UUID.randomUUID().toString());
    StorageContainerManager scm = new StorageContainerManager(conf);

    if (scmHaAlreadyEnabled) {
      scm.getScmMetadataStore().getTransactionInfoTable()
          .put(TRANSACTION_INFO_KEY, TransactionInfo.builder().build());
    }

    if (shouldFail) {
      LambdaTestUtils.intercept(UpgradeException.class, scm::start);
    } else {
      // no exception should be thrown.
      scm.start();
      scm.stop();
    }
  }
}
