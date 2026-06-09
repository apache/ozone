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

package org.apache.hadoop.ozone.om.response.upgrade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for the OMFinalizeUpgradeResponse class.
 */
public class TestOMFinalizeUpgradeResponse {

  @TempDir
  private Path folder;
  private OMMetadataManager omMetadataManager;
  private BatchOperation batchOperation;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, null);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
  }

  @Test
  public void testFinalizationInProgressKeyRemoved() throws IOException {
    // Add the in progress key which would normally have been added by the start finalization command
    omMetadataManager.getMetaTable().put(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY, "ignored");

    String value = omMetadataManager.getMetaTable().get(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY);
    assertEquals("ignored", value);

    OMFinalizeUpgradeResponse finalizeUpgradeResponse = new OMFinalizeUpgradeResponse(createRequest(), 1);
    finalizeUpgradeResponse.addToDBBatch(omMetadataManager, batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // Ensure the key is removed as expected
    value = omMetadataManager.getMetaTable().get(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY);
    assertNull(value);
  }

  private OzoneManagerProtocolProtos.OMResponse createRequest() {
    return OzoneManagerProtocolProtos.OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.FinalizeUpgrade)
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .build();
  }

}
