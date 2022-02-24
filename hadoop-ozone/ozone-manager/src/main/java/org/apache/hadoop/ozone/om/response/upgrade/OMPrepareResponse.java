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

package org.apache.hadoop.ozone.om.response.upgrade;

import static org.apache.hadoop.ozone.OzoneConsts.PREPARE_MARKER_KEY;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.TRANSACTION_INFO_TABLE;

import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.io.IOException;

/**
 * Response for prepare request.
 */
@CleanupTableInfo(cleanupTables = {TRANSACTION_INFO_TABLE})
public class OMPrepareResponse extends OMClientResponse {

  private long prepareIndex = -1;

  public OMPrepareResponse(OzoneManagerProtocolProtos.OMResponse omResponse,
                           long prepareIndex) {
    super(omResponse);
    this.prepareIndex = prepareIndex;
  }

  public OMPrepareResponse(OzoneManagerProtocolProtos.OMResponse omResponse) {
    super(omResponse);
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    if (prepareIndex != -1) {
      omMetadataManager.getTransactionInfoTable().putWithBatch(batchOperation,
          PREPARE_MARKER_KEY,
          new TransactionInfo.Builder()
              .setTransactionIndex(prepareIndex).build());
    }
  }
}
