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

package org.apache.hadoop.ozone.om.request.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.jupiter.api.Test;

/**
 * Test class for delete Lifecycle configuration request.
 */
public class TestOMLifecycleConfigurationDeleteRequest extends
    TestOMLifecycleConfigurationRequest {
  @Test
  public void testPreExecute() throws Exception {
    OMRequest omRequest = createDeleteLifecycleConfigurationRequest(
        UUID.randomUUID().toString(), UUID.randomUUID().toString());

    OMLifecycleConfigurationDeleteRequest request =
        new OMLifecycleConfigurationDeleteRequest(omRequest);

    // As user info gets added.
    assertNotEquals(omRequest, request.preExecute(ozoneManager));
  }

  @Test
  public void testPreExecuteWithLinkedBucket() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String resolvedBucketName = bucketName + "-resolved";
    String resolvedVolumeName = volumeName + "-resolved";
    // Mock the bucket link resolution
    when(ozoneManager.resolveBucketLink(any(Pair.class), any(OMClientRequest.class)))
        .thenAnswer(i -> new ResolvedBucket(i.getArgument(0), 
            Pair.of(resolvedVolumeName, resolvedBucketName),
            "owner", BucketLayout.FILE_SYSTEM_OPTIMIZED));

    OMRequest omRequest = createDeleteLifecycleConfigurationRequest(volumeName, bucketName);
    OMLifecycleConfigurationDeleteRequest request =
        new OMLifecycleConfigurationDeleteRequest(omRequest);
    OMRequest modifiedRequest = request.preExecute(ozoneManager);

    // Verify that the resolved volume and bucket names are used
    assertEquals(resolvedVolumeName,
        modifiedRequest.getDeleteLifecycleConfigurationRequest().getVolumeName());
    assertEquals(resolvedBucketName,
        modifiedRequest.getDeleteLifecycleConfigurationRequest().getBucketName());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    // Create Volume and bucket entries in DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    addLifecycleConfigurationToDB(volumeName, bucketName, "ownername");
    assertNotNull(omMetadataManager.getLifecycleConfigurationTable()
        .get(omMetadataManager.getBucketKey(volumeName, bucketName)));

    OMRequest omRequest =
        createDeleteLifecycleConfigurationRequest(volumeName, bucketName);

    OMLifecycleConfigurationDeleteRequest deleteRequest =
        new OMLifecycleConfigurationDeleteRequest(omRequest);

    deleteRequest.validateAndUpdateCache(ozoneManager, 1L);

    assertNull(omMetadataManager.getLifecycleConfigurationTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName)));
  }

  @Test
  public void testValidateAndUpdateCacheFailure() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    // Create Volume and bucket entries in DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMRequest omRequest =
        createDeleteLifecycleConfigurationRequest(volumeName, bucketName);

    OMLifecycleConfigurationDeleteRequest deleteRequest =
        new OMLifecycleConfigurationDeleteRequest(omRequest);

    OMClientResponse omClientResponse = deleteRequest.validateAndUpdateCache(
        ozoneManager, 1L);

    OMResponse omResponse = omClientResponse.getOMResponse();
    assertEquals(
        OzoneManagerProtocolProtos.Status.LIFECYCLE_CONFIGURATION_NOT_FOUND,
        omResponse.getStatus());
  }

  private void addLifecycleConfigurationToDB(String volumeName,
      String bucketName, String ownerName) throws IOException {
    OMRequest originalRequest = setLifecycleConfigurationRequest(volumeName,
        bucketName, ownerName);

    OMLifecycleConfigurationSetRequest request =
        new OMLifecycleConfigurationSetRequest(originalRequest);

    OMRequest modifiedRequest = request.preExecute(ozoneManager);

    String lifecycleKey = omMetadataManager.getBucketKey(volumeName,
        bucketName);

    assertNull(omMetadataManager.getLifecycleConfigurationTable().get(
        lifecycleKey));

    request = new OMLifecycleConfigurationSetRequest(modifiedRequest);
    long txLogIndex = 1;

    request.validateAndUpdateCache(ozoneManager, txLogIndex);
  }
}
