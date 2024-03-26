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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;

import org.apache.commons.lang3.RandomStringUtils;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * This class tests the versioning of blocks from OM side.
 */
@Timeout(300)
public class TestOmBlockVersioning {

  private static MiniOzoneCluster cluster = null;
  private static OzoneClient client;
  private static OzoneConfiguration conf;
  private static OzoneManager ozoneManager;
  private static OzoneManagerProtocol writeClient;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    ozoneManager = cluster.getOzoneManager();
    writeClient = client.getObjectStore()
        .getClientProxy().getOzoneManagerClient();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testAllocateCommit() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String keyName = "key" + RandomStringUtils.randomNumeric(5);

    OzoneBucket bucket =
        TestDataUtil.createVolumeAndBucket(client, volumeName, bucketName);
    // Versioning isn't supported currently, but just preserving old behaviour
    bucket.setVersioning(true);

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(1000)
        .setAcls(new ArrayList<>())
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
        .setOwnerName("user" + RandomStringUtils.randomNumeric(5))
        .build();

    // 1st update, version 0
    OpenKeySession openKey = writeClient.openKey(keyArgs);
    // explicitly set the keyLocation list before committing the key.
    keyArgs.setLocationInfoList(openKey.getKeyInfo().getLatestVersionLocations()
        .getBlocksLatestVersionOnly());
    writeClient.commitKey(keyArgs, openKey.getId());

    OmKeyInfo keyInfo = ozoneManager.lookupKey(keyArgs);
    OmKeyLocationInfoGroup highestVersion =
        checkVersions(keyInfo.getKeyLocationVersions());
    assertEquals(0, highestVersion.getVersion());
    assertEquals(1, highestVersion.getLocationList().size());

    // 2nd update, version 1
    openKey = writeClient.openKey(keyArgs);
    //OmKeyLocationInfo locationInfo =
    //    writeClient.allocateBlock(keyArgs, openKey.getId());
    // explicitly set the keyLocation list before committing the key.
    keyArgs.setLocationInfoList(openKey.getKeyInfo().getLatestVersionLocations()
        .getBlocksLatestVersionOnly());
    writeClient.commitKey(keyArgs, openKey.getId());

    keyInfo = ozoneManager.lookupKey(keyArgs);
    highestVersion = checkVersions(keyInfo.getKeyLocationVersions());
    assertEquals(1, highestVersion.getVersion());
    assertEquals(1, highestVersion.getLocationList().size());

    // 3rd update, version 2
    openKey = writeClient.openKey(keyArgs);

    // this block will be appended to the latest version of version 2.
    OmKeyLocationInfo locationInfo =
        writeClient.allocateBlock(keyArgs, openKey.getId(),
            new ExcludeList());
    List<OmKeyLocationInfo> locationInfoList =
        openKey.getKeyInfo().getLatestVersionLocations()
            .getBlocksLatestVersionOnly();
    assertEquals(1, locationInfoList.size());
    locationInfoList.add(locationInfo);
    keyArgs.setLocationInfoList(locationInfoList);
    writeClient.commitKey(keyArgs, openKey.getId());

    keyInfo = ozoneManager.lookupKey(keyArgs);
    highestVersion = checkVersions(keyInfo.getKeyLocationVersions());
    assertEquals(2, highestVersion.getVersion());
    assertEquals(2, highestVersion.getLocationList().size());
  }

  private OmKeyLocationInfoGroup checkVersions(
      List<OmKeyLocationInfoGroup> versions) {
    OmKeyLocationInfoGroup currentVersion = null;
    for (OmKeyLocationInfoGroup version : versions) {
      if (currentVersion != null) {
        assertEquals(currentVersion.getVersion() + 1, version.getVersion());
      }
      currentVersion = version;
    }
    return currentVersion;
  }

  @Test
  public void testReadLatestVersion() throws Exception {

    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String keyName = "key" + RandomStringUtils.randomNumeric(5);

    OzoneBucket bucket =
        TestDataUtil.createVolumeAndBucket(client, volumeName, bucketName);

    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(1000)
        .build();

    String dataString = RandomStringUtils.randomAlphabetic(100);

    TestDataUtil.createKey(bucket, keyName, dataString);
    assertEquals(dataString, TestDataUtil.getKey(bucket, keyName));
    OmKeyInfo keyInfo = ozoneManager.lookupKey(omKeyArgs);
    assertEquals(0, keyInfo.getLatestVersionLocations().getVersion());
    assertEquals(1,
        keyInfo.getLatestVersionLocations().getLocationList().size());

    // When bucket versioning is disabled, overwriting a key doesn't increment
    // its version count. Rather it always resets the version to 0
    TestDataUtil.createKey(bucket, keyName, dataString);

    keyInfo = ozoneManager.lookupKey(omKeyArgs);
    assertEquals(dataString, TestDataUtil.getKey(bucket, keyName));
    assertEquals(0, keyInfo.getLatestVersionLocations().getVersion());
    assertEquals(1,
        keyInfo.getLatestVersionLocations().getLocationList().size());

    dataString = RandomStringUtils.randomAlphabetic(200);
    TestDataUtil.createKey(bucket, keyName, dataString);

    keyInfo = ozoneManager.lookupKey(omKeyArgs);
    assertEquals(dataString, TestDataUtil.getKey(bucket, keyName));
    assertEquals(0, keyInfo.getLatestVersionLocations().getVersion());
    assertEquals(1,
        keyInfo.getLatestVersionLocations().getLocationList().size());
  }
}
