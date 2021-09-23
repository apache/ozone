package org.apache.hadoop.ozone.om.multitenant;

import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMMultiTenantManagerImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

public class TestMultiTenantVolume {
  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private Collection<OzoneClient> openClients;

  @Before
  public void setup() throws Exception {
    OMMultiTenantManagerImpl.setAuthorizerSupplier(() ->
        Mockito.mock(MultiTenantAccessAuthorizer.class)
    );

    openClients = new ArrayList<>();

    // TODO: Use cluster provider for tests or reuse cluster instance.
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .withoutDatanodes()
        .build();
    cluster.waitForClusterToBeReady();
  }

  @After
  public void teardown() throws Exception {
    for (OzoneClient client: openClients) {
      client.close();
    }
    cluster.shutdown();
  }

  @Test
  public void testS3BucketDefault() throws Exception {
    final String bucketName = "bucket";
    final String accessID = UUID.randomUUID().toString();
    final String s3VolumeName = HddsClientUtils.getDefaultS3VolumeName(conf);

    // Get Volume.
    ObjectStore store = getStoreForAccessID(accessID);
    OzoneVolume s3Volume = store.getS3Volume();
    Assert.assertEquals(s3VolumeName, s3Volume.getName());

    // Create bucket.
    store.createS3Bucket(bucketName);
    OzoneBucket bucket = store.getS3Bucket(bucketName);
    Assert.assertEquals(s3VolumeName, bucket.getVolumeName());
    // Should get the same bucket accessing from the volume directly.
    Assert.assertEquals(bucketName, s3Volume.getBucket(bucketName).getName());

    // Delete bucket.
    store.deleteS3Bucket(bucketName);
    Assert.assertNull(s3Volume.getBucket(bucketName));
  }

  @Test
  public void testS3BucketTenant() throws Exception {
    final String tenant = "tenant";
    final String username = "user";
    final String bucketName = "bucket";
    final String accessID = UUID.randomUUID().toString();
    ObjectStore store = getStoreForAccessID(accessID);

    store.createTenant(tenant);
    store.assignUserToTenant(username, tenant, accessID);

    // Get Volume.
    OzoneVolume tenantVolume = store.getS3Volume();
    Assert.assertEquals(tenant, tenantVolume.getName());

    // Create bucket.
    store.createS3Bucket(bucketName);
    // The tenant's bucket should have been created in a tenant volume.
    OzoneBucket bucket = store.getS3Bucket(bucketName);
    Assert.assertEquals(tenant, bucket.getVolumeName());
    // Should get the same bucket accessing from the volume directly.
    Assert.assertEquals(bucketName,
        tenantVolume.getBucket(bucketName).getName());

    // A different user should not see bucket, since they will be directed to
    // the s3 volume.
    ObjectStore store2 = getStoreForAccessID(UUID.randomUUID().toString());
    Assert.assertNull(store2.getS3Bucket(bucketName));

    // Delete bucket.
    store.deleteS3Bucket(bucketName);
    Assert.assertNull(tenantVolume.getBucket(bucketName));
  }

  private ObjectStore getStoreForAccessID(String accessID) throws Exception {
    UserGroupInformation remoteUser =
        UserGroupInformation.createRemoteUser(accessID);
    OzoneClient client =
        remoteUser.doAs((PrivilegedExceptionAction<OzoneClient>)
        () -> OzoneClientFactory.getRpcClient(conf));
    openClients.add(client);
    return client.getObjectStore();
  }
}
