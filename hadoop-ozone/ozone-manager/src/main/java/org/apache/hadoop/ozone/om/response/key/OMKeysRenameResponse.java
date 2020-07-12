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
import org.apache.hadoop.ozone.om.OmRenameKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

/**
 * Response for RenameKeys request.
 */
@CleanupTableInfo(cleanupTables = {KEY_TABLE})
public class OMKeysRenameResponse extends OMClientResponse {

  private List<OmRenameKeyInfo> renameKeyInfoList;
  private String fromKeyName = null;
  private String toKeyName = null;

  public OMKeysRenameResponse(@Nonnull OMResponse omResponse,
                              List<OmRenameKeyInfo> renameKeyInfoList) {
    super(omResponse);
    this.renameKeyInfoList = renameKeyInfoList;
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
    for (OmRenameKeyInfo omRenameKeyInfo : renameKeyInfoList) {
      String volumeName = omRenameKeyInfo.getNewKeyInfo().getVolumeName();
      String bucketName = omRenameKeyInfo.getNewKeyInfo().getBucketName();
      fromKeyName = omRenameKeyInfo.getFromKeyName();
      OmKeyInfo newKeyInfo = omRenameKeyInfo.getNewKeyInfo();
      toKeyName = newKeyInfo.getKeyName();
      // If toKeyName is null, then we need to only delete the fromKeyName
      // from KeyTable. This is the case of replay where toKey exists but
      // fromKey has not been deleted.
      if (deleteFromKeyOnly()) {
        omMetadataManager.getKeyTable().deleteWithBatch(batchOperation,
            omMetadataManager
                .getOzoneKey(volumeName, bucketName, fromKeyName));
      } else if (createToKeyAndDeleteFromKey()) {
        omMetadataManager.getKeyTable().deleteWithBatch(batchOperation,
            omMetadataManager
                .getOzoneKey(volumeName, bucketName, fromKeyName));
        omMetadataManager.getKeyTable().putWithBatch(batchOperation,
            omMetadataManager.getOzoneKey(volumeName, bucketName, toKeyName),
            newKeyInfo);
      }

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
