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

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.META_TABLE;

import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.OzoneConsts;
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
public class OMStartFinalizeUpgradeResponse extends OMClientResponse {
  private static final Logger LOG = LoggerFactory.getLogger(OMStartFinalizeUpgradeResponse.class);

  private final boolean finalizationNeeded;

  public OMStartFinalizeUpgradeResponse(OzoneManagerProtocolProtos.OMResponse omResponse) {
    this(omResponse, true);
  }

  /**
   * @param finalizationNeeded whether OM still needs to finalize. When {@code false} the finalization-in-progress
   *     marker is not persisted, so that initiating finalize on an already-finalized cluster does not orphan the key
   *     in the DB (the async {@code OMUpgradeFinalizeService} would never clear it since finalization is not needed).
   */
  public OMStartFinalizeUpgradeResponse(OzoneManagerProtocolProtos.OMResponse omResponse, boolean finalizationNeeded) {
    super(omResponse);
    this.finalizationNeeded = finalizationNeeded;
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    if (!finalizationNeeded) {
      LOG.info("OM does not need finalization; skipping persistence of the finalization-in-progress key.");
      return;
    }
    LOG.info("Persisting Finalization In Progress Key to the Meta DB table");
    omMetadataManager.getMetaTable().putWithBatch(batchOperation, OzoneConsts.FINALIZATION_IN_PROGRESS_KEY, "ignored");
  }
}
