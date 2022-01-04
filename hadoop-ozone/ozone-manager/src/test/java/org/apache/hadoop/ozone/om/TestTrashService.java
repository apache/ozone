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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om;


import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Test Key Trash Service.
 * <p>
 * This test does the things including:
 * 1. UTs for list trash.
 * 2. UTs for recover trash.
 * 3. UTs for empty trash.
 * <p>
 */
public class TestTrashService {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private KeyManager keyManager;
  private OzoneManagerProtocol writeClient;
  private OzoneManager om;
  private String volumeName;
  private String bucketName;

  @Before
  public void setup() throws IOException, AuthenticationException {
    OzoneConfiguration configuration = new OzoneConfiguration();

    File folder = tempFolder.newFolder();
    if (!folder.exists()) {
      Assert.assertTrue(folder.mkdirs());
    }
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(configuration, folder.toString());

    StorageContainerLocationProtocol containerClient =
        Mockito.mock(StorageContainerLocationProtocol.class);
    ScmBlockLocationProtocol blockClient =
        new ScmBlockLocationTestingClient(null, null, 0);
    OmTestManagers omTestWriteClient
        = new OmTestManagers(configuration, blockClient, containerClient);
    keyManager = omTestWriteClient.getKeyManager();
    writeClient = omTestWriteClient.getWriteClient();
    om = omTestWriteClient.getTestOm();
    volumeName = "volume";
    bucketName = "bucket";
  }

  @After
  public void cleanup() throws Exception {
    om.stop();
  }

  @Test
  public void testRecoverTrash() throws IOException {
    String keyName = "testKey";
    String destinationBucket = "destBucket";
    createAndDeleteKey(keyName);

    boolean recoverOperation = keyManager.getMetadataManager()
        .recoverTrash(volumeName, bucketName, keyName, destinationBucket);
    Assert.assertTrue(recoverOperation);
  }

  private void createAndDeleteKey(String keyName) throws IOException {

    TestOMRequestUtils.addVolumeToOM(keyManager.getMetadataManager(),
        OmVolumeArgs.newBuilder()
            .setOwnerName("owner")
            .setAdminName("admin")
            .setVolume(volumeName)
            .build());

    TestOMRequestUtils.addBucketToOM(keyManager.getMetadataManager(),
        OmBucketInfo.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .build());

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setAcls(Collections.emptyList())
        .setLocationInfoList(new ArrayList<>())
        .setReplicationConfig(
            new StandaloneReplicationConfig(HddsProtos.ReplicationFactor.ONE))
        .build();

    /* Create and delete key in the Key Manager. */
    OpenKeySession session = writeClient.openKey(keyArgs);
    writeClient.commitKey(keyArgs, session.getId());
    writeClient.deleteKey(keyArgs);
  }

}
