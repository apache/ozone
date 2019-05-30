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

package org.apache.hadoop.ozone.om.response.bucket;

import java.io.IOException;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.utils.db.BatchOperation;

/**
 * Response for DeleteBucket request.
 */
public final class OMBucketDeleteResponse extends OMClientResponse {

  private String volumeName;
  private String bucketName;

  public OMBucketDeleteResponse(
      String volumeName, String bucketName,
      OzoneManagerProtocolProtos.OMResponse omResponse) {
    super(omResponse);
    this.volumeName = volumeName;
    this.bucketName = bucketName;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    String dbBucketKey =
        omMetadataManager.getBucketKey(volumeName, bucketName);
    omMetadataManager.getBucketTable().deleteWithBatch(batchOperation,
        dbBucketKey);
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

}

