/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.apache.hadoop.ozone.om;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_METADATA_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_METADATA_LAYOUT_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_METADATA_LAYOUT_DEFAULT;

/**
 * Test that FSO configurations can only be used after finalizing OM for the
 * FSO layout feature.
 */
@RunWith(Parameterized.class)
public class TestFSOUnfinalizedStateValidationAction {
  private static OzoneConfiguration scmConf;
  private OzoneConfiguration omConf;
  private final boolean fsoEnabled;
  private static StorageContainerManager scm;
  private OzoneManager om;
  private static Path scmDir;

  @Rule
  public TemporaryFolder omDir =
      TemporaryFolder.builder().assureDeletion().build();

  @BeforeClass
  public static void startScm() throws Exception {
    // Use one SCM for all tests to save on startup time.
    DefaultMetricsSystem.setMiniClusterMode(true);
    scmConf = new OzoneConfiguration();
    scmDir =  Files.createTempDirectory("test");
    scmConf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, scmDir.toString());
    scm = TestUtils.getScm(scmConf);
    scm.start();
  }

  @AfterClass
  public static void stopScm() throws Exception {
    if (scm != null) {
      scm.stop();
    }
    FileUtils.deleteDirectory(new File(scmDir.toString()));
  }

  @Parameterized.Parameters(name = "FSO Enabled={0}")
  public static Collection<Object[]> cases() {
    List<Object[]> params = new ArrayList<>();
    params.add(new Boolean[]{true});
    params.add(new Boolean[]{false});

    return params;
  }

  public TestFSOUnfinalizedStateValidationAction(boolean fsoEnabled) {
    this.fsoEnabled = fsoEnabled;
  }

  @Before
  public void newOMDataDir() throws Exception {
    // Use a new OM and clean disk state for each test.
    File temp = omDir.newFolder(UUID.randomUUID().toString());
    omConf = new OzoneConfiguration(scmConf);
    omConf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, temp.toString());
    setFsoEnabled(this.fsoEnabled);
  }

  @After
  public void stopOm() {
    if (om != null) {
      om.stop();
    }
  }

  public void setFsoEnabled(boolean fsoEnabled) {
    String layout = OZONE_OM_METADATA_LAYOUT_DEFAULT;
    if (fsoEnabled) {
      layout = OZONE_OM_METADATA_LAYOUT_PREFIX;
    }
    omConf.set(OZONE_OM_METADATA_LAYOUT, layout);
    omConf.setBoolean(OZONE_OM_ENABLE_FILESYSTEM_PATHS, fsoEnabled);
  }

  @Test
  public void testPreFinalized() throws Exception {
    omConf.setInt(
        OmUpgradeConfig.ConfigStrings.OZONE_OM_INIT_DEFAULT_LAYOUT_VERSION,
        OMLayoutFeature.INITIAL_VERSION.layoutVersion());
    OzoneManager.omInit(omConf);
    om = OzoneManager.createOm(omConf);
    Assert.assertFalse(om.getVersionManager().isAllowed(OMLayoutFeature.FSO));

    try {
      // If OM started pre-finalized, FSO should not have been enabled.
      om.start();
      Assert.assertFalse(fsoEnabled);
    } catch (UpgradeException ex) {
      Assert.assertTrue(fsoEnabled);
    }
  }

  @Test
  public void testFinalized() throws Exception {
    // Init pre-finalized with FSO off.
    setFsoEnabled(false);
    omConf.setInt(
        OmUpgradeConfig.ConfigStrings.OZONE_OM_INIT_DEFAULT_LAYOUT_VERSION,
        OMLayoutFeature.INITIAL_VERSION.layoutVersion());
    OzoneManager.omInit(omConf);
    om = OzoneManager.createOm(omConf);
    Assert.assertFalse(om.getVersionManager().isAllowed(OMLayoutFeature.FSO));
    om.start();

    // Finalize and restart.
    om.finalizeUpgrade("client");
    Assert.assertTrue(om.getVersionManager().isAllowed(OMLayoutFeature.FSO));
    Assert.assertFalse(om.getVersionManager().needsFinalization());
    om.stop();

    setFsoEnabled(this.fsoEnabled);
    om.restart();
    Assert.assertEquals(this.fsoEnabled, om.getEnableFileSystemPaths());
    Assert.assertTrue(om.getVersionManager().isAllowed(OMLayoutFeature.FSO));
    Assert.assertFalse(om.getVersionManager().needsFinalization());
  }
}
