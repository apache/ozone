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

package org.apache.hadoop.hdds.scm.upgrade;

import static org.apache.hadoop.hdds.HDDSVersion.SOFTWARE_VERSION;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.ozone.OzoneConsts.APPARENT_VERSION_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_HA;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.upgrade.UpgradeTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that the real SCM startup path is wired to the version manager. The version manager logic itself is
 * covered by {@link org.apache.hadoop.hdds.scm.server.upgrade.TestScmVersionManager}, which calls it directly and
 * would still pass if these calls were removed from SCM startup.
 */
public class TestScmStartupVersionReconciliation {

  @TempDir
  private Path folder;

  /**
   * An SCM that crashed after replacing its DB with a finalized checkpoint, but before updating its VERSION file, must
   * finalize from the DB in {@link StorageContainerManager#start()}.
   */
  @Test
  public void testStartCallsFinalizeFromDB() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(folder.toFile());
    // VERSION file starts at INITIAL_VERSION, simulating an SCM that has not yet been finalized.
    conf.setInt(SCMStorageConfig.TESTING_INIT_APPARENT_VERSION_KEY, INITIAL_VERSION.serialize());

    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setSCMHAManager(SCMHAManagerStub.getInstance(true));
    configurator.setScmContext(SCMContext.emptyContext());
    configurator.setScmSafeModeManager(mock(SCMSafeModeManager.class));
    StorageContainerManager scm = HddsTestUtils.getScm(conf, configurator);
    try {
      // Simulate the crash state: DB is at SOFTWARE_VERSION, VERSION file is at INITIAL_VERSION.
      assertThat(scm.getVersionManager().getApparentVersion()).isEqualTo(INITIAL_VERSION);
      scm.getScmMetadataStore().getMetaTable().put(APPARENT_VERSION_KEY, String.valueOf(SOFTWARE_VERSION.serialize()));

      scm.start();

      assertThat(scm.getVersionManager().getApparentVersion()).isEqualTo(SOFTWARE_VERSION);
      assertThat(new SCMStorageConfig(conf).getApparentVersion()).isEqualTo(SOFTWARE_VERSION.serialize());
    } finally {
      scm.stop();
      scm.join();
    }
  }

  @Test
  public void testStartupRejectsUnknownApparentVersion() throws Exception {
    int serializedApparentVersion = SOFTWARE_VERSION.serialize() + 1;
    OzoneConfiguration conf = createScmStorage(serializedApparentVersion);

    String expectedMessage = "Initialization failed. Disk contains unknown apparent version "
        + serializedApparentVersion + " for software version " + SOFTWARE_VERSION
        + ". Make sure this component was not downgraded after finalization";

    IOException ioException = assertThrows(IOException.class, () -> new StorageContainerManager(conf));
    assertEquals(expectedMessage, ioException.getMessage());
  }

  private OzoneConfiguration createScmStorage(int serializedApparentVersion) throws IOException {
    File scmSubdir = folder.resolve("scm").resolve("current").toFile();
    assertTrue(scmSubdir.mkdirs());

    File ratisDir = folder.resolve("scm.ratis").toFile();
    File snapshotDir = folder.resolve("scm.ratis.snapshot").toFile();
    assertTrue(ratisDir.mkdirs());
    assertTrue(snapshotDir.mkdirs());

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_DB_DIRS, folder.toAbsolutePath().toString());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, folder.toAbsolutePath().toString());

    Properties properties = new Properties();
    properties.setProperty(SCM_ID, "scm");
    properties.setProperty(SCM_HA, "true");

    UpgradeTestUtils.createVersionFile(scmSubdir, HddsProtos.NodeType.SCM, serializedApparentVersion, properties);
    return conf;
  }
}
