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
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.client.ScmTopologyClient;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationInternalInterface;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus;
import org.apache.ozone.test.HddsWhiteboxTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proves that OzoneManager.start() is wired to call finalizeFromDBIfRequired so that an OM which
 * crashed after replacing its DB with a finalized checkpoint (but before updating the VERSION file)
 * correctly finalizes on the next restart.
 *
 * The unit tests in TestOMStartupWithDifferentApparentVersions call finalizeFromDBIfRequired
 * directly and would still pass if the call were removed from start(). This test goes through the
 * full startup path to catch that regression.
 */
public class TestOMStartupVersionReconciliation {

  @TempDir
  private Path folder;

  @Test
  public void testStartCallsFinalizeFromDB() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    ServerUtils.setOzoneMetaDirPath(conf, folder.resolve(UUID.randomUUID().toString()).toAbsolutePath().toString());
    // VERSION file starts at INITIAL_VERSION, simulating an OM that has not yet been finalized.
    conf.setInt(OMStorage.TESTING_INIT_APPARENT_VERSION_KEY, INITIAL_VERSION.serialize());
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    conf.setIfUnset(OMConfigKeys.OZONE_OM_S3_GPRC_SERVER_ENABLED, "false");
    DefaultMetricsSystem.setMiniClusterMode(true);

    OMStorage omStorage = new OMStorage(conf);
    omStorage.setClusterId("omtest");
    omStorage.setOmId("omtest");
    omStorage.initialize();

    OzoneManager.setTestSecureOmFlag(true);
    OzoneManager om = OzoneManager.createOm(conf, OzoneManager.StartupOption.REGUALR);

    // Inject mock SCM dependencies (same pattern as OmTestManagers, split to allow DB seeding before start).
    Object keyManager = HddsWhiteboxTestUtils.getInternalState(om, "keyManager");
    ScmBlockLocationProtocol scmBlockClient = new ScmBlockLocationTestingClient(null, null, 0);
    ScmClient scmClient = new ScmClient(scmBlockClient, mock(StorageContainerLocationInternalInterface.class), conf);
    HddsWhiteboxTestUtils.setInternalState(om, "scmClient", scmClient);
    HddsWhiteboxTestUtils.setInternalState(keyManager, "scmClient", scmClient);
    HddsWhiteboxTestUtils.setInternalState(keyManager, "secretManager", mock(OzoneBlockTokenSecretManager.class));
    HddsWhiteboxTestUtils.setInternalState(om, "scmTopologyClient", new ScmTopologyClient(scmBlockClient));

    // Simulate the crash state left by an OM that stopped after swapping in a finalized checkpoint
    // but before writing the updated VERSION file: DB is at SOFTWARE_VERSION, VERSION file is at INITIAL_VERSION.
    assertThat(om.getVersionManager().getApparentVersion()).isEqualTo(INITIAL_VERSION);
    om.getMetadataManager().getMetaTable().put(APPARENT_VERSION_KEY, String.valueOf(SOFTWARE_VERSION.serialize()));

    try {
      om.start();
      waitFor(() -> om.getOmRatisServer().getLeaderStatus() == RaftServerStatus.LEADER_AND_READY, 10, 10_000);
      // start() must have called finalizeFromDBIfRequired: the in-memory apparent version now matches the DB.
      assertThat(om.getVersionManager().getApparentVersion()).isEqualTo(SOFTWARE_VERSION);
    } finally {
      om.stop();
    }
  }
}
