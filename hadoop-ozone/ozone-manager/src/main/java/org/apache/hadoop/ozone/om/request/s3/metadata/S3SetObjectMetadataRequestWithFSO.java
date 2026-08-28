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

package org.apache.hadoop.ozone.om.request.s3.metadata;

import java.io.IOException;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles set object metadata request for FSO bucket.
 */
public class S3SetObjectMetadataRequestWithFSO extends S3SetObjectMetadataRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3SetObjectMetadataRequestWithFSO.class);

  public S3SetObjectMetadataRequestWithFSO(OMRequest omRequest,
                                           BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  protected KeyUpdateTarget resolveTarget(OzoneManager ozoneManager, OMMetadataManager omMetadataManager,
      String volumeName, String bucketName, String keyName) throws IOException {
    return resolveFsoTarget(ozoneManager, omMetadataManager, volumeName, bucketName, keyName);
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
