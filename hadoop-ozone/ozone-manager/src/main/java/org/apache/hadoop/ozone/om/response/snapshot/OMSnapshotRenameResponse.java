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

package org.apache.hadoop.ozone.om.response.snapshot;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SNAPSHOT_INFO_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Response for OMSnapshotRenameRequest.
 */
@CleanupTableInfo(cleanupTables = {SNAPSHOT_INFO_TABLE})
public class OMSnapshotRenameResponse extends OMClientResponse {

  private String snapshotOldName;
  private String snapshotNewName;
  private SnapshotInfo renamedInfo;

  public OMSnapshotRenameResponse(OzoneManagerProtocolProtos.OMResponse omResponse,
                                  String snapshotOldName, String snapshotNewName,
                                  @Nonnull SnapshotInfo renamedInfo) {
    super(omResponse);
    this.snapshotOldName = snapshotOldName;
    this.snapshotNewName = snapshotNewName;
    this.renamedInfo = renamedInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMSnapshotRenameResponse(@Nonnull OzoneManagerProtocolProtos.OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager, BatchOperation batchOperation)
      throws IOException {
    omMetadataManager.getSnapshotInfoTable()
        .putWithBatch(batchOperation, snapshotNewName, renamedInfo);
    omMetadataManager.getSnapshotInfoTable()
        .deleteWithBatch(batchOperation, snapshotOldName);
  }
}
