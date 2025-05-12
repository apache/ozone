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

package org.apache.hadoop.ozone.om.response.key.acl.prefix;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.PREFIX_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for Prefix Acl request.
 */
@CleanupTableInfo(cleanupTables = {PREFIX_TABLE})
public class OMPrefixAclResponse extends OMClientResponse {

  private OmPrefixInfo prefixInfo;

  public OMPrefixAclResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmPrefixInfo omPrefixInfo) {
    super(omResponse);
    this.prefixInfo = omPrefixInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMPrefixAclResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    if (getOMResponse().hasRemoveAclResponse() &&
        prefixInfo.getAcls().isEmpty()) {
      // if acl list size is zero, delete the entry.
      omMetadataManager.getPrefixTable().deleteWithBatch(batchOperation,
          prefixInfo.getName());
    } else {
      omMetadataManager.getPrefixTable().putWithBatch(batchOperation,
          prefixInfo.getName(), prefixInfo);
    }
  }
}


