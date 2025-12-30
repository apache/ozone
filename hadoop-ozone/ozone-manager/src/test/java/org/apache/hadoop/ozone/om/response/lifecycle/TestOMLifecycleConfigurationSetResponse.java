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

package org.apache.hadoop.ozone.om.response.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmLCExpiration;
import org.apache.hadoop.ozone.om.helpers.OmLCRule;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetLifecycleConfigurationResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * This class tests OMLifecycleConfigurationCreateResponse.
 */
public class TestOMLifecycleConfigurationSetResponse {

  private OMMetadataManager omMetadataManager;
  private BatchOperation batchOperation;

  @TempDir
  private File tempDir;

  @BeforeEach
  public void setup() throws Exception {
    OzoneManager ozoneManager = mock(OzoneManager.class);
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        tempDir.getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, ozoneManager);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
  }

  @AfterEach
  public void tearDown() {
    if (batchOperation != null) {
      batchOperation.close();
    }
  }

  @Test
  public void testAddToDBBatch() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OmLifecycleConfiguration omLifecycleConfiguration =
        new OmLifecycleConfiguration.Builder()
            .setVolume(volumeName)
            .setBucket(bucketName)
            .setBucketLayout(BucketLayout.OBJECT_STORE)
            .addRule(new OmLCRule.Builder().setPrefix("")
                .addAction(new OmLCExpiration.Builder().setDays(30).build())
                .build())
            .setCreationTime(Time.now())
            .build();

    assertEquals(0, omMetadataManager.countRowsInTable(
        omMetadataManager.getLifecycleConfigurationTable()));

    SetLifecycleConfigurationResponse setLifecycleConfigurationResponse =
        SetLifecycleConfigurationResponse.newBuilder()
            .build();

    OMLifecycleConfigurationSetResponse response =
        new OMLifecycleConfigurationSetResponse(OMResponse.newBuilder()
            .setCmdType(Type.SetLifecycleConfiguration)
            .setStatus(Status.OK)
            .setSetLifecycleConfigurationResponse(
                setLifecycleConfigurationResponse).build(),
            omLifecycleConfiguration);

    response.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertEquals(1, omMetadataManager.countRowsInTable(
        omMetadataManager.getLifecycleConfigurationTable()));

    Table.KeyValue<String, OmLifecycleConfiguration> keyValue =
        omMetadataManager.getLifecycleConfigurationTable()
            .iterator()
            .next();

    // Lifecycle configuration keys follow bucket key format.
    assertEquals(omMetadataManager.getBucketKey(volumeName, bucketName),
        keyValue.getKey());

    assertEquals(omLifecycleConfiguration.getProtobuf(), keyValue.getValue().getProtobuf());
  }
}
