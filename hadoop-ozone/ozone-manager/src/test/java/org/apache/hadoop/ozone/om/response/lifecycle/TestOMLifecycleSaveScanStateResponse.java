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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleScanState;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.jupiter.api.Test;

/**
 * Tests OMLifecycleSaveScanStateResponse.
 */
public class TestOMLifecycleSaveScanStateResponse {

  @Test
  public void testAddToDBBatch() throws Exception {
    OMMetadataManager omMetadataManager = mock(OMMetadataManager.class);
    BatchOperation batchOperation = mock(BatchOperation.class);
    Table<String, OmLifecycleScanState> table = mock(Table.class);
    when(omMetadataManager.getLifecycleScanStateTable()).thenReturn(table);

    OmLifecycleScanState state = new OmLifecycleScanState.Builder()
        .setBucketKey("/vol1/bucket1")
        .setScanStartTime(123456789L)
        .setLastScannedKey("key1")
        .build();

    OMResponse omResponse = OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.SaveLifecycleScanState)
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .build();

    OMLifecycleSaveScanStateResponse response = new OMLifecycleSaveScanStateResponse(omResponse, state);
    response.addToDBBatch(omMetadataManager, batchOperation);

    verify(table, times(1)).putWithBatch(batchOperation, "/vol1/bucket1", state);
  }
}
