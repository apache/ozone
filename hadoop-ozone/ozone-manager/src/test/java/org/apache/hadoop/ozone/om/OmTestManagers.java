/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.client.ScmTopologyClient;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Test utility for creating a dummy OM, the associated
 * managers, and writeClient.
 */
public final class OmTestManagers {

  private final OzoneManagerProtocol writeClient;
  private final OzoneManager om;
  private final KeyManager keyManager;
  private final OMMetadataManager metadataManager;
  private KeyProviderCryptoExtension kmsProvider;
  private final VolumeManager volumeManager;
  private final BucketManager bucketManager;
  private final PrefixManager prefixManager;
  private final ScmBlockLocationProtocol scmBlockClient;

  public OzoneManager getOzoneManager() {
    return om;
  }
  public OzoneManagerProtocol getWriteClient() {
    return writeClient;
  }
  public BucketManager getBucketManager() {
    return bucketManager;
  }
  public VolumeManager getVolumeManager() {
    return volumeManager;
  }
  public PrefixManager getPrefixManager() {
    return prefixManager;
  }
  public OMMetadataManager getMetadataManager() {
    return metadataManager;
  }
  public KeyManager getKeyManager() {
    return keyManager;
  }
  public ScmBlockLocationProtocol getScmBlockClient() {
    return scmBlockClient;
  }

  public OmTestManagers(OzoneConfiguration conf)
      throws AuthenticationException, IOException, InterruptedException, TimeoutException {
    this(conf, null, null);
  }

  public OmTestManagers(OzoneConfiguration conf,
                        ScmBlockLocationProtocol blockClient,
                        StorageContainerLocationProtocol containerClient)
      throws AuthenticationException, IOException, InterruptedException, TimeoutException {
    if (containerClient == null) {
      containerClient = mock(StorageContainerLocationProtocol.class);
    }
    scmBlockClient = blockClient != null ? blockClient :
        new ScmBlockLocationTestingClient(null, null, 0);

    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    DefaultMetricsSystem.setMiniClusterMode(true);
    OMStorage omStorage = new OMStorage(conf);
    omStorage.setClusterId("omtest");
    omStorage.setOmId("omtest");
    omStorage.initialize();
    OzoneManager.setTestSecureOmFlag(true);
    om = OzoneManager.createOm(conf,
        OzoneManager.StartupOption.REGUALR);

    keyManager = (KeyManagerImpl) HddsWhiteboxTestUtils
        .getInternalState(om, "keyManager");
    ScmClient scmClient = new ScmClient(scmBlockClient, containerClient, conf);
    ScmTopologyClient scmTopologyClient =
        new ScmTopologyClient(scmBlockClient);
    HddsWhiteboxTestUtils.setInternalState(om,
        "scmClient", scmClient);
    HddsWhiteboxTestUtils.setInternalState(keyManager,
        "scmClient", scmClient);
    HddsWhiteboxTestUtils.setInternalState(keyManager,
        "secretManager", mock(OzoneBlockTokenSecretManager.class));
    HddsWhiteboxTestUtils.setInternalState(om,
        "scmTopologyClient", scmTopologyClient);

    om.start();
    waitFor(() -> om.getOmRatisServer().checkLeaderStatus() == RaftServerStatus.LEADER_AND_READY,
        10, 10_000);

    writeClient = OzoneClientFactory.getRpcClient(conf)
        .getObjectStore().getClientProxy().getOzoneManagerClient();
    metadataManager = (OmMetadataManagerImpl) HddsWhiteboxTestUtils
        .getInternalState(om, "metadataManager");
    volumeManager = (VolumeManagerImpl)HddsWhiteboxTestUtils
        .getInternalState(om, "volumeManager");
    bucketManager = (BucketManagerImpl)HddsWhiteboxTestUtils
        .getInternalState(om, "bucketManager");
    prefixManager = (PrefixManagerImpl)HddsWhiteboxTestUtils
        .getInternalState(om, "prefixManager");
  }

  //initializing and returning a mock kmsProvider
  public KeyProviderCryptoExtension kmsProviderInit() {
    kmsProvider = mock(KeyProviderCryptoExtension.class);

    HddsWhiteboxTestUtils.setInternalState(om,
            "kmsProvider", kmsProvider);

    return kmsProvider;
  }

}
