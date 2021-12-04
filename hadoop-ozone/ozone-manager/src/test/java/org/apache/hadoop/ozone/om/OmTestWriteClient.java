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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * Test utility for creating a write client to the OM.
 */
public final class OmTestWriteClient {

  private OzoneManagerProtocol writeClient;
  private OzoneManager om;
  private KeyManager keyManager;
  private OMMetadataManager metadataManager;
  private VolumeManager volumeManager;
  private BucketManager bucketManager;
  private PrefixManager prefixManager;

  private OmTestWriteClient() {
  }
  public OzoneManager getTestOm() {
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

  public OmTestWriteClient(OzoneConfiguration conf,
      ScmBlockLocationProtocol blockClient,
      StorageContainerLocationProtocol containerClient)
      throws AuthenticationException, IOException {
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    DefaultMetricsSystem.setMiniClusterMode(true);
    OMStorage omStorage = new OMStorage(conf);
    omStorage.setClusterId("omtest");
    omStorage.setOmId("omtest");
    omStorage.initialize();
    OzoneManager.setTestSecureOmFlag(true);
    om = OzoneManager.createOm(conf,
        OzoneManager.StartupOption.REGUALR, containerClient, blockClient);

    keyManager = (KeyManagerImpl) HddsWhiteboxTestUtils
        .getInternalState(om, "keyManager");
    ScmClient scmClient = new ScmClient(blockClient, containerClient);
    HddsWhiteboxTestUtils.setInternalState(keyManager,
        "scmClient", scmClient);
    HddsWhiteboxTestUtils.setInternalState(keyManager,
        "secretManager", Mockito.mock(OzoneBlockTokenSecretManager.class));

    om.start();
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

}
