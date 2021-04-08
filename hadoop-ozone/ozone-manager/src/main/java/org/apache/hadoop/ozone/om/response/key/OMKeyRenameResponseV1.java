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

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.*;

/**
 * Response for RenameKey request layout version V1.
 */
@CleanupTableInfo(cleanupTables = {FILE_TABLE, DIRECTORY_TABLE})
public class OMKeyRenameResponseV1 extends OMKeyRenameResponse {

  private boolean isRenameDirectory;

  public OMKeyRenameResponseV1(@Nonnull OMResponse omResponse,
      String fromKeyName, String toKeyName, @Nonnull OmKeyInfo renameKeyInfo,
      boolean isRenameDirectory) {
    super(omResponse, fromKeyName, toKeyName, renameKeyInfo);
    this.isRenameDirectory = isRenameDirectory;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
                           BatchOperation batchOperation) throws IOException {

    if (isRenameDirectory) {
      omMetadataManager.getDirectoryTable().deleteWithBatch(batchOperation,
              getFromKeyName());

      OmDirectoryInfo renameDirInfo =
              OMFileRequest.getDirectoryInfo(getRenameKeyInfo());
      omMetadataManager.getDirectoryTable().putWithBatch(batchOperation,
              getToKeyName(), renameDirInfo);

    } else {
      omMetadataManager.getKeyTable().deleteWithBatch(batchOperation,
              getFromKeyName());
      omMetadataManager.getKeyTable().putWithBatch(batchOperation,
              getToKeyName(), getRenameKeyInfo());
    }
  }
}
