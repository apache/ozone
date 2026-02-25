/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.UUID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Tests to verify bucket ops with older version client.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestBucketLayoutWithOlderClient implements NonHATests.TestCase {

  private MiniOzoneCluster cluster;
  private OzoneClient client;

  @BeforeAll
  void init() throws Exception {
    cluster = cluster();
    client = cluster.newClient();
  }

  @Test
  public void testCreateBucketWithOlderClient() throws Exception {
    // create a volume and a bucket without bucket layout argument
    BucketLayout defaultLayout = cluster().getConf().getEnum(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client, null);
    String volumeName = bucket.getVolumeName();
    // OM defaulted bucket layout
    assertEquals(defaultLayout, bucket.getBucketLayout());

    // Sets bucket layout explicitly.
    OzoneBucket fsobucket = TestDataUtil
        .createVolumeAndBucket(client, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    assertEquals(BucketLayout.FILE_SYSTEM_OPTIMIZED,
        fsobucket.getBucketLayout());
    OzoneBucket obsBucket = TestDataUtil.createVolumeAndBucket(client, BucketLayout.OBJECT_STORE);
    assertEquals(BucketLayout.OBJECT_STORE, obsBucket.getBucketLayout());

    // Create bucket request by an older client.
    // Here sets ClientVersion.DEFAULT_VERSION
    String buckName = "buck-1-legacy-" + UUID.randomUUID();
    OzoneManager.setTestSecureOmFlag(true);
    OzoneManagerProtocolProtos.OMRequest createBucketReq =
        OzoneManagerProtocolProtos.OMRequest
            .newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateBucket)
            .setVersion(ClientVersion.DEFAULT_VERSION.serialize())
            .setClientId(UUID.randomUUID().toString())
            .setCreateBucketRequest(
                OzoneManagerProtocolProtos.CreateBucketRequest.newBuilder()
                    .setBucketInfo(
                        OzoneManagerProtocolProtos.BucketInfo.newBuilder()
                            .setVolumeName(volumeName).setBucketName(buckName)
                            .setIsVersionEnabled(false).setStorageType(
                            StorageTypeProto.DISK)
                            .build())
                    .build()).build();
    createBucketReq = createBucketReq.toBuilder()
        .setUserInfo(OzoneManagerProtocolProtos.UserInfo.newBuilder()
            .setUserName(UserGroupInformation.getCurrentUser().getShortUserName()).build()).build();

    OzoneManagerProtocolProtos.OMResponse
        omResponse = cluster.getOzoneManager().getOmServerProtocol()
        .submitRequest(null, createBucketReq);

    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());

    OmBucketInfo bucketInfo =
        cluster.getOzoneManager().getBucketInfo(volumeName, buckName);
    assertNotNull(bucketInfo);
    assertEquals(BucketLayout.LEGACY, bucketInfo.getBucketLayout());
  }

  @AfterAll
  void cleanup() {
    IOUtils.closeQuietly(client);
  }
}
