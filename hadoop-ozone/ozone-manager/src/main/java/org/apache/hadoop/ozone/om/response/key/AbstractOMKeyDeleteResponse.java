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

package org.apache.hadoop.ozone.om.response.key;

import static org.apache.hadoop.ozone.om.helpers.OmKeyInfo.isKeyEmpty;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Base class for responses that need to move keys from an arbitrary table to
 * the deleted table.
 */
public abstract class AbstractOMKeyDeleteResponse extends OmKeyResponse {

  public AbstractOMKeyDeleteResponse(
      @Nonnull OMResponse omResponse) {
    super(omResponse);
  }

  public AbstractOMKeyDeleteResponse(@Nonnull OMResponse omResponse,
                                     @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
  }

  /**
   * Adds the operation of deleting the {@code keyName omKeyInfo} pair from
   * {@code fromTable} to the batch operation {@code batchOperation}. The
   * batch operation is not committed, so no changes are persisted to disk.
   * The log transaction index used will be retrieved by calling
   * {@link OmKeyInfo#getUpdateID} on {@code omKeyInfo}.
   */
  protected void addDeletionToBatch(
      OMMetadataManager omMetadataManager,
      BatchOperation batchOperation,
      Table<String, ?> fromTable,
      String keyName,
      OmKeyInfo omKeyInfo,
      long bucketId,
      boolean isCommittedKey) throws IOException {

    // For OmResponse with failure, this should do nothing. This method is
    // not called in failure scenario in OM code.
    fromTable.deleteWithBatch(batchOperation, keyName);

    // If Key is not empty add this to delete table.
    if (!isKeyEmpty(omKeyInfo)) {
      // If a deleted key is put in the table where a key with the same
      // name already exists, then the old deleted key information would be
      // lost. To avoid this, first check if a key with same name exists.
      // deletedTable in OM Metadata stores <KeyName, RepeatedOMKeyInfo>.
      // The RepeatedOmKeyInfo is the structure that allows us to store a
      // list of OmKeyInfo that can be tied to same key name. For a keyName
      // if RepeatedOMKeyInfo structure is null, we create a new instance,
      // if it is not null, then we simply add to the list and store this
      // instance in deletedTable.
      omKeyInfo = omKeyInfo.withCommittedKeyDeletedFlag(isCommittedKey);
      RepeatedOmKeyInfo repeatedOmKeyInfo = OmUtils.prepareKeyForDelete(
          bucketId, omKeyInfo, omKeyInfo.getUpdateID()
      );
      String delKeyName = omMetadataManager.getOzoneDeletePathKey(
          omKeyInfo.getObjectID(), keyName);
      omMetadataManager.getDeletedTable().putWithBatch(
          batchOperation, delKeyName, repeatedOmKeyInfo);
    }
  }

  /**
   *  This method is used for FSO file deletes.
   *  Since a common deletedTable is used ,it requires to add  key in the
   *  full format (vol/buck/key). This method deletes the key from
   *  file table (which is in prefix format) and adds the fullKey
   *  into the deletedTable
   * @param keyName     (format: objectId/key)
   * @param deleteKeyName (format: vol/buck/key/objectId)
   * @param omKeyInfo
   * @throws IOException
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  protected void addDeletionToBatch(
      OMMetadataManager omMetadataManager,
      BatchOperation batchOperation,
      Table<String, ?> fromTable,
      String keyName, String deleteKeyName,
      OmKeyInfo omKeyInfo,
      long bucketId,
      boolean isCommittedKey) throws IOException {

    // For OmResponse with failure, this should do nothing. This method is
    // not called in failure scenario in OM code.
    fromTable.deleteWithBatch(batchOperation, keyName);

    // If Key is not empty add this to delete table.
    if (!isKeyEmpty(omKeyInfo)) {
      // If a deleted key is put in the table where a key with the same
      // name already exists, then the old deleted key information would be
      // lost. To avoid this, first check if a key with same name exists.
      // deletedTable in OM Metadata stores <KeyName, RepeatedOMKeyInfo>.
      // The RepeatedOmKeyInfo is the structure that allows us to store a
      // list of OmKeyInfo that can be tied to same key name. For a keyName
      // if RepeatedOMKeyInfo structure is null, we create a new instance,
      // if it is not null, then we simply add to the list and store this
      // instance in deletedTable.
      omKeyInfo = omKeyInfo.withCommittedKeyDeletedFlag(isCommittedKey);
      RepeatedOmKeyInfo repeatedOmKeyInfo = OmUtils.prepareKeyForDelete(
          bucketId, omKeyInfo, omKeyInfo.getUpdateID()
      );
      omMetadataManager.getDeletedTable().putWithBatch(
          batchOperation, deleteKeyName, repeatedOmKeyInfo);
    }
  }

  @Override
  public abstract void addToDBBatch(OMMetadataManager omMetadataManager,
        BatchOperation batchOperation) throws IOException;
}
