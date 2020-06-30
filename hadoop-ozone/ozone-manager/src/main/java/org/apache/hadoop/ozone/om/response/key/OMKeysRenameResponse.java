/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.response.key;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Response for RenameKey request.
 */
public class OMKeysRenameResponse extends OMClientResponse {

  private String fromKeyName;
  private String toKeyName;
  private OmKeyInfo newKeyInfo;

  public OMKeysRenameResponse(@Nonnull OMResponse omResponse,
                              String fromKeyName, String toKeyName,
                              @Nonnull OmKeyInfo renameKeyInfo) {
    super(omResponse);
    this.fromKeyName = fromKeyName;
    this.toKeyName = toKeyName;
    this.newKeyInfo = renameKeyInfo;
  }

  /**
   * When Rename request is replayed and toKey already exists, but fromKey
   * has not been deleted.
   * For example, lets say we have the following sequence of transactions
   *  Trxn 1 : Create Key1
   *  Trnx 2 : Rename Key1 to Key2 -> Deletes Key1 and Creates Key2
   *  Now if these transactions are replayed:
   *  Replay Trxn 1 : Creates Key1 again as Key1 does not exist in DB
   *  Replay Trxn 2 : Key2 is not created as it exists in DB and the request
   *  would be deemed a replay. But Key1 is still in the DB and needs to be
   *  deleted.
   */
  public OMKeysRenameResponse(@Nonnull OMResponse omResponse,
                              String fromKeyName, OmKeyInfo fromKeyInfo) {
    super(omResponse);
    this.fromKeyName = fromKeyName;
    this.newKeyInfo = fromKeyInfo;
    this.toKeyName = null;
  }

  /**
   * For when the request is not successful or it is a replay transaction.
   * For a successful request, the other constructor should be used.
   */
  public OMKeysRenameResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    String volumeName = newKeyInfo.getVolumeName();
    String bucketName = newKeyInfo.getBucketName();
    // If toKeyName is null, then we need to only delete the fromKeyName from
    // KeyTable. This is the case of replay where toKey exists but fromKey
    // has not been deleted.
    if (deleteFromKeyOnly()) {
      omMetadataManager.getKeyTable().deleteWithBatch(batchOperation,
          omMetadataManager.getOzoneKey(volumeName, bucketName, fromKeyName));
    } else if (createToKeyAndDeleteFromKey()) {
      // If both from and toKeyName are equal do nothing
      omMetadataManager.getKeyTable().deleteWithBatch(batchOperation,
          omMetadataManager.getOzoneKey(volumeName, bucketName, fromKeyName));
      omMetadataManager.getKeyTable().putWithBatch(batchOperation,
          omMetadataManager.getOzoneKey(volumeName, bucketName, toKeyName),
          newKeyInfo);
    }
  }

  @VisibleForTesting
  public boolean deleteFromKeyOnly() {
    return toKeyName == null && fromKeyName != null;
  }

  @VisibleForTesting
  public boolean createToKeyAndDeleteFromKey() {
    return toKeyName != null && !toKeyName.equals(fromKeyName);
  }
}
