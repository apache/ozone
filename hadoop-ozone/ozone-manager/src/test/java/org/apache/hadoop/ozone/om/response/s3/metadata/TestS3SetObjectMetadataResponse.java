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

package org.apache.hadoop.ozone.om.response.s3.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.key.OMKeyResponseTests;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Test;

/**
 * Test set object metadata response.
 */
public class TestS3SetObjectMetadataResponse extends OMKeyResponseTests {

  @Test
  public void testAddToDBBatch() throws Exception {
    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setSetObjectMetadataResponse(
                OzoneManagerProtocolProtos.SetObjectMetadataResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.SetObjectMetadata)
            .build();

    String ozoneKey = addKeyToTable();
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertNotNull(omKeyInfo);
    assertThat(omKeyInfo.getMetadata()).doesNotContainKey("meta-key1");

    Map<String, String> metadata = new HashMap<>();
    metadata.put("meta-key1", "meta-value1");
    metadata.put("meta-key2", "meta-value2");

    omKeyInfo = omKeyInfo.toBuilder()
        .setMetadata(metadata)
        .build();

    S3SetObjectMetadataResponse setObjectMetadataResponse = getSetObjectMetadataResponse(omKeyInfo, omResponse);

    setObjectMetadataResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    OmKeyInfo updatedOmKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertNotSame(omKeyInfo, updatedOmKeyInfo);
    assertNotNull(updatedOmKeyInfo);
    assertThat(updatedOmKeyInfo.getMetadata()).containsAllEntriesOf(metadata);
  }

  protected String addKeyToTable() throws Exception {
    OMRequestTestUtils.addKeyToTable(false, false, volumeName, bucketName,
        keyName, clientID, RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        omMetadataManager);

    return omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);
  }

  protected S3SetObjectMetadataResponse getSetObjectMetadataResponse(OmKeyInfo omKeyInfo,
                                                                     OzoneManagerProtocolProtos.OMResponse omResponse)
      throws IOException {
    return new S3SetObjectMetadataResponse(omResponse, omKeyInfo);
  }
}
