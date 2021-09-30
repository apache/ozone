/*
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
package org.apache.hadoop.ozone.om.multitenant;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneClusterProvider;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMMultiTenantManagerImpl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.mockito.Mockito;

import java.util.UUID;

public class TestMultiTenantVolume {
  private static MiniOzoneClusterProvider clusterProvider;
  private MiniOzoneCluster cluster;
  private static String s3VolumeName;

  @BeforeClass
  public static void initClusterProvider() {
    OzoneConfiguration conf = new OzoneConfiguration();
    MiniOzoneCluster.Builder builder = MiniOzoneCluster.newBuilder(conf)
        .withoutDatanodes();
    clusterProvider = new MiniOzoneClusterProvider(conf, builder, 2);
    s3VolumeName = HddsClientUtils.getDefaultS3VolumeName(conf);
  }

  @Before
  public void setup() throws Exception {
    OMMultiTenantManagerImpl.setAuthorizerSupplier(() ->
        Mockito.mock(MultiTenantAccessAuthorizer.class)
    );
    cluster = clusterProvider.provide();
  }

  @After
  public void teardown() throws Exception {
    clusterProvider.destroy(cluster);
  }

  @AfterAll
  public static void shutdownClusterProvider() throws Exception {
    clusterProvider.shutdown();
  }

  @Test
  public void testS3BucketDefault() throws Exception {
    final String bucketName = "bucket";
    final String accessID = UUID.randomUUID().toString();

    // Get Volume.
    ObjectStore store = cluster.getClient().getObjectStore();
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
    ObjectStore store = cluster.getClient().getObjectStore();

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
