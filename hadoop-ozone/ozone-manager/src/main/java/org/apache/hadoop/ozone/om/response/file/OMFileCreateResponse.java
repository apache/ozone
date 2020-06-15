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

package org.apache.hadoop.ozone.om.response.file;

import javax.annotation.Nonnull;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Response for crate file request.
 */
public class OMFileCreateResponse extends OMClientResponse {

  public static final Logger LOG =
          LoggerFactory.getLogger(OMKeyCreateResponse.class);
  private OmKeyInfo omKeyInfo;
  private long openKeySessionID;
  private List<OmDirectoryInfo> parentDirInfos;

  public OMFileCreateResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmKeyInfo omKeyInfo,
      List<OmDirectoryInfo> parentDirInfos, long openKeySessionID) {
    super(omResponse);
    this.omKeyInfo = omKeyInfo;
    this.openKeySessionID = openKeySessionID;
    this.parentDirInfos = parentDirInfos;
  }

  /**
   * For when the request is not successful or it is a replay transaction.
   * For a successful request, the other constructor should be used.
   */
  public OMFileCreateResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
                              BatchOperation batchOperation) throws IOException {

    /**
     * Create parent directory entries during Key Create - do not wait
     * for Key Commit request.
     * XXX handle stale directory entries.
     */
    if (parentDirInfos != null) {
      for (OmDirectoryInfo parentKeyInfo : parentDirInfos) {
        String parentKey = omMetadataManager
                .getOzonePrefixKey(parentKeyInfo.getParentObjectID(), parentKeyInfo.getName());
        if (LOG.isDebugEnabled()) {
          LOG.debug("putWithBatch adding parent : key {} info : {}", parentKey,
                  parentKeyInfo);
        }
        omMetadataManager.getDirectoryTable()
                .putWithBatch(batchOperation, parentKey, parentKeyInfo);
      }
    }

    String openKey = omMetadataManager.getOpenLeafNodeKey(omKeyInfo.getParentObjectID(),
            omKeyInfo.getLeafNodeName(), openKeySessionID);
    omMetadataManager.getOpenKeyTable().putWithBatch(batchOperation,
            openKey, omKeyInfo);
  }
}
