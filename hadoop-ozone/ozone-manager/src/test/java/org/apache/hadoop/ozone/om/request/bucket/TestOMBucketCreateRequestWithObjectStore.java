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

package org.apache.hadoop.ozone.om.request.bucket;

import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.newBucketInfoBuilder;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.newCreateBucketRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests bucket creation behavior for ObjectStore / Legacy bucket layouts.
 */
public class TestOMBucketCreateRequestWithObjectStore extends TestOMBucketCreateRequest {

  @BeforeEach
  public void setupWithObjectStore() {
    ozoneManager.getConfiguration().set(
        OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        OMConfigKeys.OZONE_BUCKET_LAYOUT_OBJECT_STORE);
  }

  @Test
  public void testNonS3BucketNameRejectedForObjectStoreWhenStrictDisabled()
      throws Exception {

    // strict mode disabled
    ozoneManager.getConfiguration().setBoolean(
        OMConfigKeys.OZONE_OM_NAMESPACE_STRICT_S3, false);

    String volumeName = UUID.randomUUID().toString();
    String bucketName = "bucket_with_underscore"; // non-S3-compliant
    addCreateVolumeToTable(volumeName, omMetadataManager);

    // Explicitly set bucket layout to OBJECT_STORE so the test doesn't depend on
    // defaults or mocked OM behavior.
    OzoneManagerProtocolProtos.BucketInfo.Builder bucketInfo =
        newBucketInfoBuilder(bucketName, volumeName)
            .setBucketLayout(
                OzoneManagerProtocolProtos.BucketLayoutProto.OBJECT_STORE);

    OMRequest originalRequest = newCreateBucketRequest(bucketInfo).build();
    OMBucketCreateRequest req = new OMBucketCreateRequest(originalRequest);

    OMException ex = assertThrows(OMException.class,
        () -> req.preExecute(ozoneManager));

    assertEquals(OMException.ResultCodes.INVALID_BUCKET_NAME, ex.getResult());
  }
}
