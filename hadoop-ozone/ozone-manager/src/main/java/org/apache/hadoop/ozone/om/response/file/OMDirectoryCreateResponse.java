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

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Response for create directory request.
 */
public class OMDirectoryCreateResponse extends OMClientResponse {

  public static final Logger LOG =
      LoggerFactory.getLogger(OMDirectoryCreateResponse.class);
  private OmKeyInfo dirKeyInfo;

  public OMDirectoryCreateResponse(@Nonnull OMResponse omResponse,
      @Nullable OmKeyInfo dirKeyInfo) {
    super(omResponse);
    this.dirKeyInfo = dirKeyInfo;
  }

  /**
   * For when the request is not successful or it is a replay transaction or
   * the directory already exists.
   */
  public OMDirectoryCreateResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    if (dirKeyInfo != null) {
      String dirKey = omMetadataManager.getOzoneKey(dirKeyInfo.getVolumeName(),
          dirKeyInfo.getBucketName(), dirKeyInfo.getKeyName());
      omMetadataManager.getKeyTable().putWithBatch(batchOperation, dirKey,
          dirKeyInfo);
    }
  }
}
