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

import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.file.OMFileCreateResponseV1;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_FILE_TABLE;

/**
 * Response for CreateKey request layout version V1.
 */
@CleanupTableInfo(cleanupTables = {DIRECTORY_TABLE, OPEN_FILE_TABLE})
public class OMKeyCreateResponseV1 extends OMFileCreateResponseV1 {

  public OMKeyCreateResponseV1(@Nonnull OMResponse omResponse,
                               @Nonnull OmKeyInfo omKeyInfo,
                               @Nonnull List<OmDirectoryInfo> parentDirInfos,
                               long openKeySessionID,
                               @Nonnull OmBucketInfo omBucketInfo) {
    super(omResponse, omKeyInfo, parentDirInfos, openKeySessionID,
            omBucketInfo);
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMKeyCreateResponseV1(@Nonnull OMResponse omResponse) {
    super(omResponse);
  }
}
