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

import static org.apache.hadoop.ozone.OzoneConsts.LAYOUT_VERSION_KEY;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.META_TABLE;

import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Response for finalizeUpgrade request.
 */
@CleanupTableInfo(cleanupTables = {META_TABLE})
public class OMFinalizeUpgradeResponse extends OMClientResponse {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMFinalizeUpgradeResponse.class);
  private int layoutVersionToWrite = -1;

  public OMFinalizeUpgradeResponse(
      OzoneManagerProtocolProtos.OMResponse omResponse,
      int layoutVersionToWrite) {
    super(omResponse);
    this.layoutVersionToWrite = layoutVersionToWrite;
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    if (layoutVersionToWrite != -1) {
      LOG.info("Layout version to persist to DB : {}", layoutVersionToWrite);
      omMetadataManager.getMetaTable().putWithBatch(batchOperation,
          LAYOUT_VERSION_KEY,
          String.valueOf(layoutVersionToWrite));
    }
  }
}
