package org.apache.hadoop.ozone.om.multitenant;

import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.OzoneDelegationTokenSecretManager;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto.Type.S3AUTHINFO;

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
    ObjectStore store = OzoneClientFactory.getRpcClient(conf).getObjectStore();
    OzoneVolume s3Volume = store.getS3Volume(accessID);
    Assert.assertEquals(s3VolumeName, s3Volume.getName());

    // Create bucket.
    s3Volume.createBucket(bucketName);
    OzoneBucket bucket = s3Volume.getBucket(bucketName);
    Assert.assertEquals(s3VolumeName, bucket.getVolumeName());

    // Delete bucket.
    s3Volume.deleteBucket(bucketName);
    assertBucketNotFound(s3Volume, bucketName);
  }

  @Test
  public void testS3BucketTenant() throws Exception {
    final String tenant = "tenant";
    final String username = "user";
    final String bucketName = "bucket";
    final String accessID = UUID.randomUUID().toString();
    ObjectStore store = OzoneClientFactory.getRpcClient(conf).getObjectStore();

    store.createTenant(tenant);
    store.assignUserToTenant(username, tenant, accessID);

    // Get Volume.
    OzoneVolume tenantVolume = store.getS3Volume(accessID);
    Assert.assertEquals(tenant, tenantVolume.getName());

    // Create bucket.
    tenantVolume.createBucket(bucketName);
    // The tenant's bucket should have been created in a tenant volume.
    OzoneBucket bucket = tenantVolume.getBucket(bucketName);
    Assert.assertEquals(tenant, bucket.getVolumeName());

    // A different user should not see bucket, since they will be directed to
    // the s3 volume.
    String accessID2 = UUID.randomUUID().toString();
    assertBucketNotFound(store.getS3Volume(accessID2), bucketName);

    // Delete bucket.
    tenantVolume.deleteBucket(bucketName);
    assertBucketNotFound(tenantVolume, bucketName);
  }

  private void assertBucketNotFound(OzoneVolume volume, String bucketName)
      throws Exception {
    try {
      volume.getBucket(bucketName);
    } catch(OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.BUCKET_NOT_FOUND) {
        throw ex;
      }
    }
  }

//  private void assertS3BucketNotFound(OzoneVolume volume, String bucketName)
//      throws Exception {
//    try {
//      volume.getBucket(bucketName);
//    } catch(OMException ex) {
//      if (ex.getResult() != OMException.ResultCodes.BUCKET_NOT_FOUND) {
//        throw ex;
//      }
//    }
//  }

//  private ObjectStore getStoreForAccessID(String accessID) throws Exception {
////    OzoneTokenIdentifier identifier = OzoneTokenIdentifier.newInstance();
////    identifier.setOmCertSerialId("foo");
////    identifier.setGetUserForAccessId(accID -> accessID);
//
//    UserGroupInformation remoteUser =
//        UserGroupInformation.createRemoteUser(accessID);
//
////    Token<OzoneTokenIdentifier> token = new Token(identifier.getBytes(),
////        identifier.getSignature().getBytes(StandardCharsets.UTF_8),
////        identifier.getKind(), null);
////    remoteUser.addToken(token);
//
//    OzoneClient client =
//        remoteUser.doAs((PrivilegedExceptionAction<OzoneClient>)
//        () -> OzoneClientFactory.getRpcClient(conf));
//    openClients.add(client);
//    client.
//    return client.getObjectStore();
//  }
}
