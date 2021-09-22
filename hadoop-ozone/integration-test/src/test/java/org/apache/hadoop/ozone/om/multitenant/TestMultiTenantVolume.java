package org.apache.hadoop.ozone.om.multitenant;

import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMMultiTenantManagerImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.UUID;

public class TestMultiTenantVolume {
  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private ObjectStore store;
  private OzoneConfiguration conf;

  @Before
  public void setup() throws Exception {
    OMMultiTenantManagerImpl.setAuthorizerSupplier(() ->
        Mockito.mock(MultiTenantAccessAuthorizer.class)
    );
    // TODO: Use new cluster provider and non-dn cluster.
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setTotalPipelineNumLimit(10)
        .setScmId("scmId")
        .setClusterId("clusterId")
        .build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(conf);
    store = client.getObjectStore();
  }

  @After
  public void teardown() throws Exception {
    client.close();
    cluster.shutdown();
  }

  @Test
  public void testS3BucketDefault() throws Exception {
    final String bucketName = "bucket";
    final String accessID = UUID.randomUUID().toString();
    final String s3VolumeName = HddsClientUtils.getDefaultS3VolumeName(conf);

    // Get Volume.
    setRpcUser(accessID);
    OzoneVolume s3Volume = store.getS3Volume();
    Assert.assertEquals(s3VolumeName, s3Volume.getName());

    // Create bucket.
    setRpcUser(accessID);
    store.createS3Bucket(bucketName);
    OzoneBucket bucket = store.getS3Bucket(bucketName);
    Assert.assertEquals(s3VolumeName, bucket.getVolumeName());
    // Should get the same bucket accessing from the volume directly.
    Assert.assertEquals(bucketName, s3Volume.getBucket(bucketName).getName());

    // Delete bucket.
    setRpcUser(accessID);
    store.deleteS3Bucket(bucketName);
    Assert.assertNull(s3Volume.getBucket(bucketName));
  }

  @Test
  public void testS3BucketTenant() throws Exception {
    final String tenant = "tenant";
    final String username = "user";
    final String bucketName = "bucket";
    final String accessID = UUID.randomUUID().toString();

    store.createTenant(tenant);
    store.assignUserToTenant(username, tenant, accessID);

    // Get Volume.
    setRpcUser(accessID);
    OzoneVolume tenantVolume = store.getS3Volume();
    Assert.assertEquals(tenant, tenantVolume.getName());

    // Create bucket.
    setRpcUser(accessID);
    store.createS3Bucket(bucketName);
    // The tenant's bucket should have been created in a tenant volume.
    setRpcUser(accessID);
    OzoneBucket bucket = store.getS3Bucket(bucketName);
    Assert.assertEquals(tenant, bucket.getVolumeName());
    // Should get the same bucket accessing from the volume directly.
    Assert.assertEquals(bucketName,
        tenantVolume.getBucket(bucketName).getName());

    // A different user should not see bucket, since they will be directed to
    // the s3 volume.
    setRpcUser(UUID.randomUUID().toString());
    Assert.assertNull(store.getS3Bucket(bucketName));

    // Delete bucket.
    setRpcUser(accessID);
    store.deleteS3Bucket(bucketName);
    Assert.assertNull(tenantVolume.getBucket(bucketName));
  }

  private static void setRpcUser(String accessID) {
    // TODO: Figure out how to make this request as the user.
    Server.Call call = Mockito.spy(new Server.Call(1, 1, null, null,
        RPC.RpcKind.RPC_BUILTIN, new byte[] {1, 2, 3}));
    UserGroupInformation ugiAlice =
        UserGroupInformation.createRemoteUser(accessID);
    Mockito.when(call.getRemoteUser()).thenReturn(ugiAlice);
    Server.getCurCall().set(call);
  }

}
