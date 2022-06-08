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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.UUID;

/**
 * Tests to verify bucket ops with older version client.
 */
public class TestBucketLayoutWithOlderClient {

  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static String clusterId;
  private static String scmId;
  private static String omId;

  @Rule
  public Timeout timeout = new Timeout(1200000);

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omId = UUID.randomUUID().toString();
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.OBJECT_STORE.name());
    cluster = MiniOzoneCluster.newBuilder(conf).setClusterId(clusterId)
        .setScmId(scmId).setOmId(omId).build();
    cluster.waitForClusterToBeReady();
  }

  @Test
  public void testCreateBucketWithOlderClient() throws Exception {
    // create a volume and a bucket without bucket layout argument
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster, null);
    String volumeName = bucket.getVolumeName();
    // OM defaulted bucket layout
    Assert.assertEquals(BucketLayout.OBJECT_STORE, bucket.getBucketLayout());

    // Sets bucket layout explicitly.
    OzoneBucket fsobucket = TestDataUtil
        .createVolumeAndBucket(cluster, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    Assert.assertEquals(BucketLayout.FILE_SYSTEM_OPTIMIZED,
        fsobucket.getBucketLayout());

    // Create bucket request by an older client.
    // Here sets ClientVersion.DEFAULT_VERSION
    String buckName = "buck-1-legacy-" + UUID.randomUUID();
    OzoneManager.setTestSecureOmFlag(true);
    OzoneManagerProtocolProtos.OMRequest createBucketReq =
        OzoneManagerProtocolProtos.OMRequest
            .newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateBucket)
            .setVersion(ClientVersion.DEFAULT_VERSION.toProtoValue())
            .setClientId(UUID.randomUUID().toString())
            .setCreateBucketRequest(
                OzoneManagerProtocolProtos.CreateBucketRequest.newBuilder()
                    .setBucketInfo(
                        OzoneManagerProtocolProtos.BucketInfo.newBuilder()
                            .setVolumeName(volumeName).setBucketName(buckName)
                            .setIsVersionEnabled(false).setStorageType(
                            OzoneManagerProtocolProtos.StorageTypeProto.DISK)
                            .build())
                    .build()).build();

    OzoneManagerProtocolProtos.OMResponse
        omResponse = cluster.getOzoneManager().getOmServerProtocol()
        .submitRequest(null, createBucketReq);

    Assert.assertEquals(omResponse.getStatus(),
        OzoneManagerProtocolProtos.Status.OK);

    OmBucketInfo bucketInfo =
        cluster.getOzoneManager().getBucketInfo(volumeName, buckName);
    Assert.assertNotNull(bucketInfo);
    Assert.assertEquals(BucketLayout.LEGACY, bucketInfo.getBucketLayout());
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
