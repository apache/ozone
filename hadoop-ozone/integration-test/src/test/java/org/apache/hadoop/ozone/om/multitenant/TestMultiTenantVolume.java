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
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.OMMultiTenantManagerImpl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;

import java.util.UUID;

/**
 * Tests that S3 requests for a tenant are directed to that tenant's volume,
 * and that users not belonging to a tenant are directed to the default S3
 * volume.
 */
public class TestMultiTenantVolume {
  private static MiniOzoneClusterProvider clusterProvider;
  private MiniOzoneCluster cluster;
  private static String s3VolumeName;

  @BeforeClass
  public static void initClusterProvider() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(
        OMMultiTenantManagerImpl.OZONE_OM_TENANT_DEV_SKIP_RANGER, true);
    MiniOzoneCluster.Builder builder = MiniOzoneCluster.newBuilder(conf)
        .withoutDatanodes();
    clusterProvider = new MiniOzoneClusterProvider(conf, builder, 2);
    s3VolumeName = HddsClientUtils.getDefaultS3VolumeName(conf);
  }

  @Before
  public void setup() throws Exception {
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
  public void testDefaultS3Volume() throws Exception {
    final String bucketName = "bucket";

    // Default client not belonging to a tenant should end up in the S3 volume.
    ObjectStore store = cluster.getClient().getObjectStore();
    Assert.assertEquals(s3VolumeName, store.getS3Volume().getName());

    // Create bucket.
    store.createS3Bucket(bucketName);
    Assert.assertEquals(s3VolumeName,
        store.getS3Bucket(bucketName).getVolumeName());

    // Delete bucket.
    store.deleteS3Bucket(bucketName);
    assertS3BucketNotFound(store, bucketName);
  }

  @Test
  public void testS3TenantVolume() throws Exception {
    final String tenant = "tenant";
    final String principal = "username";
    final String bucketName = "bucket";
    final String accessID = UUID.randomUUID().toString();

    ObjectStore store = getStoreForAccessID(accessID);
    store.createTenant(tenant);
    store.tenantAssignUserAccessId(principal, tenant, accessID);

    // S3 volume pointed to by the store should be for the tenant.
    Assert.assertEquals(tenant, store.getS3Volume().getName());

    // Create bucket in the tenant volume.
    store.createS3Bucket(bucketName);
    OzoneBucket bucket = store.getS3Bucket(bucketName);
    Assert.assertEquals(tenant, bucket.getVolumeName());

    // A different user should not see bucket, since they will be directed to
    // the s3 volume.
    ObjectStore store2 = getStoreForAccessID(UUID.randomUUID().toString());
    assertS3BucketNotFound(store2, bucketName);

    // Delete bucket.
    store.deleteS3Bucket(bucketName);
    assertS3BucketNotFound(store, bucketName);
  }

  /**
   * Checks that the bucket is not found using
   * {@link ObjectStore#getS3Bucket} and the designated S3 volume pointed to
   * by the ObjectStore.
   */
  private void assertS3BucketNotFound(ObjectStore store, String bucketName)
      throws Exception {
    try {
      store.getS3Bucket(bucketName);
    } catch(OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.BUCKET_NOT_FOUND) {
        throw ex;
      }
    }

    try {
      OzoneVolume volume = store.getS3Volume();
      volume.getBucket(bucketName);
    } catch(OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.BUCKET_NOT_FOUND) {
        throw ex;
      }
    }
  }

  private ObjectStore getStoreForAccessID(String accessID) throws Exception {
    // Cluster provider will modify our provided configuration. We must use
    // this version to build the client.
    OzoneConfiguration conf = cluster.getOzoneManager().getConfiguration();
    // Manually construct an object store instead of using the cluster
    // provided one so we can specify the access ID.
    RpcClient client = new RpcClient(conf, null);
    client.setTheadLocalS3Auth(new S3Auth("unused1", "unused2", accessID));
    return new ObjectStore(conf, client);
  }
}
