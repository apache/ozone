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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.OzoneConsts.APPARENT_VERSION_KEY;
import static org.apache.hadoop.ozone.OzoneManagerVersion.SOFTWARE_VERSION;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.INITIAL_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.client.ScmTopologyClient;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationInternalInterface;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.ozone.test.HddsWhiteboxTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that the real OM startup path is wired to the version manager. The version manager logic itself is covered
 * by {@link org.apache.hadoop.ozone.om.upgrade.TestOMVersionManager}, which calls it directly and would still pass if
 * these calls were removed from OM startup.
 */
public class TestOMStartupVersionReconciliation {

  @TempDir
  private Path folder;

  /**
   * An OM that crashed after replacing its DB with a finalized checkpoint, but before updating its VERSION file, must
   * finalize from the DB in {@link OzoneManager#start()}.
   */
  @Test
  public void testStartCallsFinalizeFromDB() throws Exception {
    // VERSION file starts at INITIAL_VERSION, simulating an OM that has not yet been finalized.
    OzoneConfiguration conf = createOMStorage(INITIAL_VERSION.serialize());
    OzoneManager om = OzoneManager.createOm(conf, OzoneManager.StartupOption.REGUALR);
    try {
      // Inject mock SCM dependencies (same pattern as OmTestManagers, split to allow DB seeding before start).
      Object keyManager = HddsWhiteboxTestUtils.getInternalState(om, "keyManager");
      ScmBlockLocationProtocol scmBlockClient = new ScmBlockLocationTestingClient(null, null, 0);
      ScmClient scmClient = new ScmClient(scmBlockClient, mock(StorageContainerLocationInternalInterface.class), conf);
      HddsWhiteboxTestUtils.setInternalState(om, "scmClient", scmClient);
      HddsWhiteboxTestUtils.setInternalState(keyManager, "scmClient", scmClient);
      HddsWhiteboxTestUtils.setInternalState(keyManager, "secretManager", mock(OzoneBlockTokenSecretManager.class));
      HddsWhiteboxTestUtils.setInternalState(om, "scmTopologyClient", new ScmTopologyClient(scmBlockClient));

      // Simulate the crash state: DB is at SOFTWARE_VERSION, VERSION file is at INITIAL_VERSION.
      assertThat(om.getVersionManager().getApparentVersion()).isEqualTo(INITIAL_VERSION);
      om.getMetadataManager().getMetaTable().put(APPARENT_VERSION_KEY, String.valueOf(SOFTWARE_VERSION.serialize()));

      om.start();

      // Finalization in start() is synchronous, so check before Ratis has a chance to do anything else.
      assertThat(om.getVersionManager().getApparentVersion()).isEqualTo(SOFTWARE_VERSION);
      assertThat(new OMStorage(conf).getApparentVersion()).isEqualTo(SOFTWARE_VERSION.serialize());
    } finally {
      om.stop();
    }
  }

  @Test
  public void testStartupRejectsUnknownApparentVersion() throws Exception {
    int serializedApparentVersion = SOFTWARE_VERSION.serialize() + 1;
    OzoneConfiguration conf = createOMStorage(serializedApparentVersion);

    String expectedMessage = "Initialization failed. Disk contains unknown apparent version "
        + serializedApparentVersion + " for software version " + SOFTWARE_VERSION
        + ". Make sure OM was not downgraded after finalization";

    IOException ioException =
        assertThrows(IOException.class, () -> OzoneManager.createOm(conf, OzoneManager.StartupOption.REGUALR));
    assertEquals(expectedMessage, ioException.getMessage());
  }

  private OzoneConfiguration createOMStorage(int serializedApparentVersion) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    ServerUtils.setOzoneMetaDirPath(conf, folder.toAbsolutePath().toString());
    conf.setInt(OMStorage.TESTING_INIT_APPARENT_VERSION_KEY, serializedApparentVersion);
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    conf.setIfUnset(OMConfigKeys.OZONE_OM_S3_GPRC_SERVER_ENABLED, "false");
    DefaultMetricsSystem.setMiniClusterMode(true);
    OzoneManager.setTestSecureOmFlag(true);

    OMStorage omStorage = new OMStorage(conf);
    omStorage.setClusterId("omtest");
    omStorage.setOmId("omtest");
    omStorage.initialize();
    return conf;
  }
}
