/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om.response.snapshot;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.SNAPSHOT_INFO_TABLE;

/**
 * Response for OMSnapshotMoveDeletedKeysRequest.
 */
@CleanupTableInfo(cleanupTables = {SNAPSHOT_INFO_TABLE})
public class OMSnapshotMoveDeletedKeysResponse extends OMClientResponse {

  private OmSnapshot nextSnapshot;
  private List<SnapshotMoveKeyInfos> activeDBKeysList;
  private List<SnapshotMoveKeyInfos> nextDBKeysList;

  public OMSnapshotMoveDeletedKeysResponse(OMResponse omResponse,
       OmSnapshot omNextSnapshot, List<SnapshotMoveKeyInfos> activeDBKeysList,
       List<SnapshotMoveKeyInfos> nextDBKeysList) {
    super(omResponse);
    this.nextSnapshot = omNextSnapshot;
    this.activeDBKeysList = activeDBKeysList;
    this.nextDBKeysList = nextDBKeysList;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMSnapshotMoveDeletedKeysResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    for (SnapshotMoveKeyInfos activeDBKey : activeDBKeysList) {
      RepeatedOmKeyInfo activeDBOmKeyInfo =
          createRepeatedOmKeyInfo(activeDBKey.getKeyInfosList());

      if (activeDBOmKeyInfo == null) {
        continue;
      }

      omMetadataManager.getDeletedTable().putWithBatch(
          batchOperation, activeDBKey.getKey(), activeDBOmKeyInfo);
    }

    for (SnapshotMoveKeyInfos nextDBKey : nextDBKeysList) {
      RepeatedOmKeyInfo nextDBOmKeyInfo =
          createRepeatedOmKeyInfo(nextDBKey.getKeyInfosList());

      if (nextDBOmKeyInfo == null) {
        continue;
      }

      if (nextSnapshot != null) {
        nextSnapshot.getMetadataManager()
            .getDeletedTable().putWithBatch(batchOperation,
                nextDBKey.getKey(), nextDBOmKeyInfo);
      } else {
        omMetadataManager.getDeletedTable().putWithBatch(
            batchOperation, nextDBKey.getKey(), nextDBOmKeyInfo);
      }
    }
  }

  private RepeatedOmKeyInfo createRepeatedOmKeyInfo(List<KeyInfo> keyInfoList)
      throws IOException {
    RepeatedOmKeyInfo result = null;

    for (KeyInfo keyInfo: keyInfoList) {
      if (result == null) {
        result = new RepeatedOmKeyInfo(OmKeyInfo.getFromProtobuf(keyInfo));
      } else {
        result.addOmKeyInfo(OmKeyInfo.getFromProtobuf(keyInfo));
      }
    }

    return result;
  }
}

