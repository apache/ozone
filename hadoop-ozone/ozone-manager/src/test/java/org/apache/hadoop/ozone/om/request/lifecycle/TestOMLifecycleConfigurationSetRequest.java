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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleConfiguration;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;

/**
 * Test class for create Lifecycle configuration request.
 */
public class TestOMLifecycleConfigurationSetRequest extends
    TestOMLifecycleConfigurationRequest {
  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    doPreExecute(volumeName, bucketName);

    assertThrows(OMException.class, () -> doPreExecute("v1", "bucket1"));
    assertThrows(OMException.class, () -> doPreExecute("volume1", "b1"));
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String ownerName = "ownerName";

    addVolumeAndBucketToTable(volumeName, bucketName, ownerName,
        omMetadataManager);

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
    long txLogIndex = 2;

    OMClientResponse omClientResponse = request.validateAndUpdateCache(ozoneManager, txLogIndex);
    OMResponse omResponse = omClientResponse.getOMResponse();

    assertNotNull(omResponse.getSetLifecycleConfigurationResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());
    assertEquals(Type.SetLifecycleConfiguration,
        omResponse.getCmdType());

    LifecycleConfiguration lifecycleConfigurationRequestProto =
        request.getOmRequest()
            .getSetLifecycleConfigurationRequest()
            .getLifecycleConfiguration();

    OmLifecycleConfiguration lifecycleConfigurationRequest =
        OmLifecycleConfiguration.getFromProtobuf(
            lifecycleConfigurationRequestProto);

    OmLifecycleConfiguration lifecycleConfigurationTable = omMetadataManager
        .getLifecycleConfigurationTable().get(lifecycleKey);

    assertNotNull(lifecycleConfigurationTable);
    assertEquals(lifecycleConfigurationRequest.getVolume(),
        lifecycleConfigurationTable.getVolume());
    assertEquals(lifecycleConfigurationRequest.getBucket(),
        lifecycleConfigurationTable.getBucket());
    assertEquals(lifecycleConfigurationRequest.getBucket(),
        lifecycleConfigurationTable.getBucket());
    assertEquals(lifecycleConfigurationRequest.getCreationTime(),
        lifecycleConfigurationTable.getCreationTime());
  }

  @Test
  public void testValidateAndUpdateNoBucket() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String ownerName = "ownerName";

    OMRequestTestUtils.addVolumeToDB(volumeName, ownerName, omMetadataManager);

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
    long txLogIndex = 2;

    OMClientResponse omClientResponse = request.validateAndUpdateCache(ozoneManager, txLogIndex);
    OMResponse omResponse = omClientResponse.getOMResponse();

    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omResponse.getStatus());
  }

  @Test
  public void testValidateAndUpdateInvalidLCC() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String ownerName = "ownerName";

    OMRequestTestUtils.addVolumeToDB(volumeName, ownerName, omMetadataManager);

    OMRequest originalRequest = setLifecycleConfigurationRequest(volumeName,
        bucketName, ownerName, false);

    OMLifecycleConfigurationSetRequest request =
        new OMLifecycleConfigurationSetRequest(originalRequest);

    OMRequest modifiedRequest = request.preExecute(ozoneManager);

    String lifecycleKey = omMetadataManager.getBucketKey(volumeName,
        bucketName);

    assertNull(omMetadataManager.getLifecycleConfigurationTable().get(
        lifecycleKey));

    request = new OMLifecycleConfigurationSetRequest(modifiedRequest);
    long txLogIndex = 2;

    OMClientResponse omClientResponse = request.validateAndUpdateCache(ozoneManager, txLogIndex);
    OMResponse omResponse = omClientResponse.getOMResponse();

    assertEquals(OzoneManagerProtocolProtos.Status.INVALID_REQUEST,
        omResponse.getStatus());
  }

  private void doPreExecute(String volumeName, String bucketName)
      throws Exception {

    OMRequest originalRequest = setLifecycleConfigurationRequest(volumeName,
        bucketName, "ownername");

    OMLifecycleConfigurationSetRequest request =
        new OMLifecycleConfigurationSetRequest(originalRequest);

    OMRequest modifiedRequest = request.preExecute(ozoneManager);
    verifyRequest(modifiedRequest, originalRequest);
  }

  /**
   * Verify modifiedOmRequest and originalRequest.
   * @param modifiedRequest
   * @param originalRequest
   */
  private void verifyRequest(OMRequest modifiedRequest,
      OMRequest originalRequest) {

    LifecycleConfiguration original =
        originalRequest.getSetLifecycleConfigurationRequest()
            .getLifecycleConfiguration();

    LifecycleConfiguration updated =
        modifiedRequest.getSetLifecycleConfigurationRequest()
            .getLifecycleConfiguration();

    assertEquals(original.getVolume(), updated.getVolume());
    assertEquals(original.getBucket(), updated.getBucket());
    assertNotEquals(original.getCreationTime(), updated.getCreationTime());
    assertEquals(original.getRulesList(), updated.getRulesList());
  }
}
