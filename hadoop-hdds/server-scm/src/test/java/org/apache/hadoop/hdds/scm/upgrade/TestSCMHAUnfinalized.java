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
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.UUID;

public class TestSCMHAUnfinalized {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private StorageContainerManager scm;

  @Before
  public void setup() {
  }

  @Test
  public void testSCMHAConfigsUsedUnfinalized() throws Exception {
    // Build unfinalized SCM with HA configuration enabled.
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    conf.setInt(ScmConfig.ConfigStrings.HDDS_SCM_INIT_DEFAULT_LAYOUT_VERSION,
        HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        temporaryFolder.newFolder().getAbsolutePath());

    StorageContainerManager.scmInit(conf, UUID.randomUUID().toString());
    scm = new StorageContainerManager(conf);

    LambdaTestUtils.intercept(UpgradeException.class, scm::start);
  }
}
