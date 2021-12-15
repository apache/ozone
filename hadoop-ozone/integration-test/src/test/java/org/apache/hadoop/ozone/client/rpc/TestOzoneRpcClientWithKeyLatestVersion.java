package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
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
import org.junit.Assert;
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
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_KEY_LATEST_VERSION_LOCATION;
import static org.junit.Assert.fail;

/**
 * Main purpose of this test is with OZONE_CLIENT_KEY_LATEST_VERSION_LOCATION
 * set/unset key create/read works properly or not for buckets
 * with/with out versioning.
 */
@RunWith(Parameterized.class)
public class TestOzoneRpcClientWithKeyLatestVersion {

  private MiniOzoneCluster cluster;

  private ObjectStore objectStore;

  private OzoneClient ozClient;

  private boolean getLatestVersion;


  @Parameterized.Parameters
  public static Collection<Object> data() {
    return Arrays.asList(true, false);
  }

  public TestOzoneRpcClientWithKeyLatestVersion(boolean getLatestVersion) {
    try {
      setup(getLatestVersion);
    } catch (Exception ex) {
      fail("Unexpected exception during setup:" + ex);
    }
  }


  public void setup(boolean latestVersion) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    conf.setBoolean(OZONE_CLIENT_KEY_LATEST_VERSION_LOCATION, latestVersion);
    this.getLatestVersion = latestVersion;
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setScmId(UUID.randomUUID().toString())
        .setClusterId(UUID.randomUUID().toString())
        .build();
    cluster.waitForClusterToBeReady();
    ozClient = OzoneClientFactory.getRpcClient(conf);
    objectStore = ozClient.getObjectStore();
  }

  @After
  public void tearDown() throws Exception {
    if(ozClient != null) {
      ozClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }


  @Test
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


    // Versioning turned on
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
    Assert.assertTrue(ozoneFileStatusList.size() == 1);
    if (!getLatestVersion && versioning) {
      Assert.assertEquals(2, ozoneFileStatusList.get(0).getKeyInfo()
          .getKeyLocationVersions().size());
    } else {
      Assert.assertEquals(1, ozoneFileStatusList.get(0).getKeyInfo()
          .getKeyLocationVersions().size());
    }
  }
}
