/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.om.helpers.*;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test key creation.
 * This file hosts tests purely focused on key creation.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestKeyCreation {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneConfiguration createConfAndInitValues() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    conf.setBoolean(HDDS_BLOCK_TOKEN_ENABLED, true);
    conf.setQuietMode(false);
    return conf;
  }

  /**
   * checkSkippingOfBlockTokens tests if block security tokens are not persisted
   * along with the key.
   *
   * @throws IOException
   */
  @Test(timeout = 5000)
  public void checkSkippingOfBlockTokens()
      throws IOException {
    // 1. Create an object with security manager and token set
    // 2. Read the object back and any token should be generated new
    // and not persisted.
    OzoneConfiguration conf = createConfAndInitValues();
    OmMetadataManagerImpl metaMgr = new OmMetadataManagerImpl(conf);
    OzoneBlockTokenSecretManager secretManager =
        mock(OzoneBlockTokenSecretManager.class);
    Token<OzoneBlockTokenIdentifier> token = new Token<>();
    token.setKind(new Text("to-be-persisted"));
    when(secretManager.
        generateToken(anyString(), any(), any(), anyLong())).
        thenReturn(token);
    KeyManager keyManager =
        new KeyManagerImpl(
            new ScmBlockLocationTestingClient(null, null, 0),
            metaMgr, conf, UUID.randomUUID().toString(), secretManager);
    keyManager.start(conf);
    String volumeName = String.format("volume%s",
        RandomStringUtils.randomAlphanumeric(5));
    String bucketName = String.format("bucket%s",
        RandomStringUtils.randomAlphanumeric(5));
    String keyName = String.format("key%s",
        RandomStringUtils.randomAlphanumeric(5));
    TestOMRequestUtils.addVolumeToOM(keyManager.getMetadataManager(),
        OmVolumeArgs.newBuilder()
            .setOwnerName("o")
            .setAdminName("a")
            .setVolume(volumeName)
            .build());

    TestOMRequestUtils.addBucketToOM(keyManager.getMetadataManager(),
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .build());

    OmKeyArgs arg =
        new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setAcls(Collections.emptyList())
            .setReplicationConfig(new StandaloneReplicationConfig(
                HddsProtos.ReplicationFactor.ONE))
            .setLocationInfoList(new ArrayList<>())
            .build();
    OpenKeySession session = keyManager.openKey(arg);
    for (int i = 0; i < 100; i++) {
      arg.addLocationInfo(
          keyManager.allocateBlock(arg, session.getId(), new ExcludeList()));
    }
    keyManager.commitKey(arg, session.getId());
    // Reset the security token setting
    token.setKind(new Text("generated"));
    when(secretManager.
        generateToken(anyString(), any(), any(), anyLong())).
        thenReturn(token);
    OpenKeySession omKeySession = keyManager.openKey(arg);
    for (OmKeyLocationInfoGroup locationInfoGroup : omKeySession.getKeyInfo()
        .getKeyLocationVersions()) {
      for (OmKeyLocationInfo keyLocationInfo : locationInfoGroup
          .getLocationList()) {
        if (keyLocationInfo.getToken() != null) {
          Assert.assertTrue(
              keyLocationInfo.getToken().getKind() == token.getKind());
        }
      }
    }
  }
}
