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

package org.apache.hadoop.ozone.web.client;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rest.RestClient;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test Ozone Volumes Lifecycle.
 */
@RunWith(value = Parameterized.class)
public class TestVolume {
  private static MiniOzoneCluster cluster = null;
  private static ClientProtocol client = null;
  private static OzoneConfiguration conf;

  @Parameterized.Parameters
  public static Collection<Object[]> clientProtocol() {
    Object[][] params = new Object[][] {
        {RpcClient.class},
        {RestClient.class}};
    return Arrays.asList(params);
  }

  @Parameterized.Parameter
  public Class clientProtocol;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true and
   * OZONE_HANDLER_TYPE_KEY = "local" , which uses a local directory to
   * emulate Ozone backend.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();

    String path = GenericTestUtils
        .getTempPath(TestVolume.class.getSimpleName());
    path += conf.getTrimmed(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT,
        OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT_DEFAULT);
    FileUtils.deleteDirectory(new File(path));

    conf.set(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT, path);
    Logger.getLogger("log4j.logger.org.apache.http").setLevel(Level.DEBUG);

    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
  }

  @Before
  public void setup() throws Exception {
    if (clientProtocol.equals(RestClient.class)) {
      client = new RestClient(conf);
    } else {
      client = new RpcClient(conf);
    }
  }

  /**
   * shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testCreateVolume() throws Exception {
    runTestCreateVolume(client);
  }

  static void runTestCreateVolume(ClientProtocol client)
      throws OzoneException, IOException, ParseException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();

    long currentTime = Time.now();

    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .setAdmin("hdfs")
        .build();
    client.createVolume(volumeName, volumeArgs);
    OzoneVolume vol = client.getVolumeDetails(volumeName);

    assertEquals(vol.getName(), volumeName);
    assertEquals(vol.getAdmin(), "hdfs");
    assertEquals(vol.getOwner(), "bilbo");
    assertEquals(vol.getQuota(), OzoneQuota.parseQuota("100TB").sizeInBytes());

    // verify the key creation time
    assertTrue((vol.getCreationTime()
        / 1000) >= (currentTime / 1000));

    // Test create a volume with invalid volume name,
    // not use Rule here because the test method is static.
    try {
      String invalidVolumeName = "#" + OzoneUtils.getRequestID().toLowerCase();
      client.createVolume(invalidVolumeName);
      /*
      //TODO: RestClient and RpcClient should use HddsClientUtils to verify name
      fail("Except the volume creation be failed because the"
          + " volume name starts with an invalid char #");*/
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Bucket or Volume name"
          + " has an unsupported character : #"));
    }
  }

  @Test
  public void testCreateDuplicateVolume() throws OzoneException, IOException {
    runTestCreateDuplicateVolume(client);
  }

  static void runTestCreateDuplicateVolume(ClientProtocol client)
      throws OzoneException, IOException {
    try {
      client.createVolume("testvol");
      client.createVolume("testvol");
      assertFalse(true);
    } catch (IOException ioe) {
      Assert.assertTrue(ioe.getMessage()
          .contains("Volume creation failed, error:VOLUME_ALREADY_EXISTS"));
    }
  }

  @Test
  public void testDeleteVolume() throws OzoneException, IOException {
    runTestDeleteVolume(client);
  }

  static void runTestDeleteVolume(ClientProtocol client)
      throws OzoneException, IOException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    client.createVolume(volumeName);
    client.deleteVolume(volumeName);
  }

  @Test
  public void testChangeOwnerOnVolume() throws Exception {
    runTestChangeOwnerOnVolume(client);
  }

  static void runTestChangeOwnerOnVolume(ClientProtocol client)
      throws OzoneException, ParseException, IOException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    client.createVolume(volumeName);
    client.getVolumeDetails(volumeName);
    client.setVolumeOwner(volumeName, "frodo");
    OzoneVolume newVol = client.getVolumeDetails(volumeName);
    assertEquals(newVol.getOwner(), "frodo");
    // verify if the creation time is missing after setting owner operation
    assertTrue(newVol.getCreationTime() > 0);
  }

  @Test
  public void testChangeQuotaOnVolume() throws Exception {
    runTestChangeQuotaOnVolume(client);
  }

  static void runTestChangeQuotaOnVolume(ClientProtocol client)
      throws OzoneException, IOException, ParseException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    client.createVolume(volumeName);
    client.setVolumeQuota(volumeName, OzoneQuota.parseQuota("1000MB"));
    OzoneVolume newVol = client.getVolumeDetails(volumeName);
    assertEquals(newVol.getQuota(), OzoneQuota.parseQuota("1000MB").sizeInBytes());
    // verify if the creation time is missing after setting quota operation
    assertTrue(newVol.getCreationTime() > 0);
  }

  @Test
  public void testListVolume() throws OzoneException, IOException {
    runTestListVolume(client);
  }

  static void runTestListVolume(ClientProtocol client)
      throws OzoneException, IOException {
    for (int x = 0; x < 10; x++) {
      String volumeName = OzoneUtils.getRequestID().toLowerCase();
      client.createVolume(volumeName);
    }

    List<OzoneVolume> ovols = client.listVolumes(null, null, 100);
    assertTrue(ovols.size() >= 10);
  }

  // TODO: remove @Ignore below once the problem has been resolved.
  @Ignore("Takes 3m to run, disable for now.")
  @Test
  public void testListVolumePagination() throws OzoneException, IOException {
    runTestListVolumePagination(client);
  }

  static void runTestListVolumePagination(ClientProtocol client)
      throws OzoneException, IOException {
    final int volCount = 2000;
    final int step = 100;
    for (int x = 0; x < volCount; x++) {
      String volumeName = OzoneUtils.getRequestID().toLowerCase();
      client.createVolume(volumeName);
    }
    String prevKey = null;
    int count = 0;
    int pagecount = 0;
    while (count < volCount) {
      List<OzoneVolume> ovols = client.listVolumes(null, prevKey, step);
      count += ovols.size();
      prevKey = ovols.get(ovols.size() - 1).getName();
      pagecount++;
    }
    assertEquals(volCount / step, pagecount);
  }

  // TODO: remove @Ignore below once the problem has been resolved.
  @Ignore
  @Test
  public void testListAllVolumes() throws OzoneException, IOException {
    runTestListAllVolumes(client);
  }

  static void runTestListAllVolumes(ClientProtocol client)
      throws OzoneException, IOException {
    final int volCount = 200;
    final int step = 10;
    for (int x = 0; x < volCount; x++) {
      String userName =
          "frodo" + RandomStringUtils.randomAlphabetic(5).toLowerCase();
      String volumeName =
          "vol" + RandomStringUtils.randomAlphabetic(5).toLowerCase();
      VolumeArgs volumeArgs = VolumeArgs.newBuilder()
          .setOwner(userName)
          .setQuota("100TB")
          .setAdmin("hdfs")
          .build();
      client.createVolume(volumeName, volumeArgs);
      OzoneVolume vol = client.getVolumeDetails(volumeName);
      assertNotNull(vol);
    }
    String prevKey = null;
    int count = 0;
    int pagecount = 0;
    while (count < volCount) {
      List<OzoneVolume> ovols = client.listVolumes(null, prevKey, step);
      count += ovols.size();
      if (ovols.size() > 0) {
        prevKey = ovols.get(ovols.size() - 1).getName();
      }
      pagecount++;
    }
    // becasue we are querying an existing ozone store, there will
    // be volumes created by other tests too. So we should get more page counts.
    assertEquals(volCount / step, pagecount);
  }

  @Test
  public void testListVolumes() throws Exception {
    runTestListVolumes(client);
  }

  static void runTestListVolumes(ClientProtocol client)
      throws OzoneException, IOException, ParseException {
    final int volCount = 20;
    final String user1 = "test-user-a";
    final String user2 = "test-user-b";

    long currentTime = Time.now();
    // Create 20 volumes, 10 for user1 and another 10 for user2.
    for (int x = 0; x < volCount; x++) {
      String volumeName;
      String userName;

      if (x % 2 == 0) {
        // create volume [test-vol0, test-vol2, ..., test-vol18] for user1
        userName = user1;
        volumeName = "test-vol" + x;
      } else {
        // create volume [test-vol1, test-vol3, ..., test-vol19] for user2
        userName = user2;
        volumeName = "test-vol" + x;
      }
      VolumeArgs volumeArgs = VolumeArgs.newBuilder()
          .setOwner(userName)
          .setQuota("100TB")
          .setAdmin("hdfs")
          .build();
      client.createVolume(volumeName, volumeArgs);
      OzoneVolume vol = client.getVolumeDetails(volumeName);
      assertNotNull(vol);
    }

    // list all the volumes belong to user1
    List<OzoneVolume> volumeList = client.listVolumes(user1, null, null, 100);
    assertEquals(10, volumeList.size());
    // verify the owner name and creation time of volume
    for (OzoneVolume vol : volumeList) {
      assertTrue(vol.getOwner().equals(user1));
      assertTrue((vol.getCreationTime()
          / 1000) >= (currentTime / 1000));
    }

    // test max key parameter of listing volumes
    volumeList = client.listVolumes(user1, null, null, 2);
    assertEquals(2, volumeList.size());

    // test prefix parameter of listing volumes
    volumeList = client.listVolumes(user1, "test-vol10", null, 10);
    assertTrue(volumeList.size() == 1
        && volumeList.get(0).getName().equals("test-vol10"));

    volumeList = client.listVolumes(user1, "test-vol1", null, 10);
    assertEquals(5, volumeList.size());

    // test start key parameter of listing volumes
    volumeList = client.listVolumes(user2, null, "test-vol15", 10);
    assertEquals(2, volumeList.size());

    String volumeName;
    for (int x = 0; x < volCount; x++) {
      volumeName = "test-vol" + x;
      client.deleteVolume(volumeName);
    }
  }
}
