/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_KEY_LATEST_VERSION_LOCATION;


/**
 * Main purpose of this test is with OZONE_CLIENT_KEY_LATEST_VERSION_LOCATION
 * set/unset key create/read works properly or not for buckets
 * with/with out versioning.
 */
@RunWith(Parameterized.class)
public class TestOzoneRpcClientWithKeyLatestVersion {

  private static MiniOzoneCluster cluster;

  private ObjectStore objectStore;

  private OzoneClient ozClient;

  private final boolean getLatestVersion;

  @Parameterized.Parameters
  public static Collection<Object> data() {
    return Arrays.asList(true, false);
  }

  public TestOzoneRpcClientWithKeyLatestVersion(boolean latestVersion) {
    getLatestVersion = latestVersion;
  }

  @BeforeClass
  public static void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setScmId(UUID.randomUUID().toString())
        .setClusterId(UUID.randomUUID().toString())
        .build();
    cluster.waitForClusterToBeReady();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setupClient() throws Exception {
    OzoneConfiguration conf = cluster.getConf();
    conf.setBoolean(OZONE_CLIENT_KEY_LATEST_VERSION_LOCATION, getLatestVersion);
    ozClient = OzoneClientFactory.getRpcClient(conf);
    objectStore = ozClient.getObjectStore();
  }

  @After
  public void closeClient() throws Exception {
    if (ozClient != null) {
      ozClient.close();
    }
  }


  @Test
  public void testWithGetLatestVersion() throws Exception {
    testOverrideAndReadKey();
  }



  public void testOverrideAndReadKey() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    // Test checks key override and read are working with
    // bucket versioning false.
    String value = "sample value";
    createRequiredForVersioningTest(volumeName, bucketName, keyName,
        false, value);

    // read key and test
    testReadKey(volumeName, bucketName, keyName, value);

    testListStatus(volumeName, bucketName, keyName, false);


    // Bucket versioning turned on
    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    keyName = UUID.randomUUID().toString();

    // Test checks key override and read are working with
    // bucket versioning true.
    createRequiredForVersioningTest(volumeName, bucketName, keyName,
        true, value);

    // read key and test
    testReadKey(volumeName, bucketName, keyName, value);


    testListStatus(volumeName, bucketName, keyName, true);
  }

  private void createRequiredForVersioningTest(String volumeName,
      String bucketName, String keyName, boolean versioning,
      String value) throws Exception {

    ReplicationConfig replicationConfig = ReplicationConfig
        .fromProtoTypeAndFactor(RATIS, HddsProtos.ReplicationFactor.THREE);

    objectStore.createVolume(volumeName);
    OzoneVolume volume = objectStore.getVolume(volumeName);

    // Bucket created with versioning false.
    volume.createBucket(bucketName,
        BucketArgs.newBuilder().setVersioning(versioning).build());
    OzoneBucket bucket = volume.getBucket(bucketName);

    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(UTF_8).length, replicationConfig, new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();

    // Override key
    out = bucket.createKey(keyName,
        value.getBytes(UTF_8).length, replicationConfig, new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();
  }

  private void testReadKey(String volumeName, String bucketName,
      String keyName, String value) throws Exception {
    OzoneVolume volume = objectStore.getVolume(volumeName);
    OzoneBucket ozoneBucket = volume.getBucket(bucketName);
    OzoneInputStream is = ozoneBucket.readKey(keyName);
    byte[] fileContent = new byte[value.getBytes(UTF_8).length];
    is.read(fileContent);
    Assert.assertEquals(value, new String(fileContent, UTF_8));
  }

  private void testListStatus(String volumeName, String bucketName,
      String keyName, boolean versioning) throws Exception{
    OzoneVolume volume = objectStore.getVolume(volumeName);
    OzoneBucket ozoneBucket = volume.getBucket(bucketName);
    List<OzoneFileStatus> ozoneFileStatusList = ozoneBucket.listStatus(keyName,
        false, "", 1);
    Assert.assertNotNull(ozoneFileStatusList);
    Assert.assertEquals(1, ozoneFileStatusList.size());
    if (!getLatestVersion && versioning) {
      Assert.assertEquals(2, ozoneFileStatusList.get(0).getKeyInfo()
          .getKeyLocationVersions().size());
    } else {
      Assert.assertEquals(1, ozoneFileStatusList.get(0).getKeyInfo()
          .getKeyLocationVersions().size());
    }
  }
}
