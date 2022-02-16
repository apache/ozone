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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.request.bucket;

import java.util.UUID;

import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;

import static org.mockito.Mockito.when;

/**
 * Tests OMBucketDeleteRequest class which handles DeleteBucket request.
 */
public class TestOMBucketDeleteRequestWithFSO
    extends TestOMBucketDeleteRequest {

  @Test
  public void testValidateAndUpdateCacheWithFSO() throws Exception {
    /*when(ozoneManager.getOMDefaultBucketLayout()).thenReturn(
        BucketLayout.FILE_SYSTEM_OPTIMIZED.name());*/

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    Assert.assertEquals(0, omMetrics.getNumFSOBucketDeletes());

    OzoneManagerProtocolProtos.OMRequest omRequest =
        createDeleteBucketRequest(volumeName, bucketName);

    OMBucketDeleteRequest omBucketDeleteRequest =
        new OMBucketDeleteRequest(omRequest);

    // Create Volume and bucket entries in DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    omBucketDeleteRequest.validateAndUpdateCache(ozoneManager, 1,
        ozoneManagerDoubleBufferHelper);

    Assert.assertNull(omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName)));

    Assert.assertEquals(1, omMetrics.getNumFSOBucketDeletes());
  }

  private OMRequest createDeleteBucketRequest(
      String volumeName,
      String bucketName) {
    return OMRequest.newBuilder()
        .setDeleteBucketRequest(
            DeleteBucketRequest.newBuilder()
                .setBucketName(bucketName).setVolumeName(volumeName))
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteBucket)
        .setClientId(UUID.randomUUID().toString()).build();
  }
}
