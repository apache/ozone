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

package org.apache.hadoop.ozone.om.ratis.execution.request;

import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;

/**
 * Interface for key write requests.
 */
public abstract class OMKeyRequestBase extends OMRequestBase {
  private OmBucketInfo bucketInfo;
  public OMKeyRequestBase(OMRequest omRequest, OmBucketInfo bucketInfo) {
    super(omRequest);
    this.bucketInfo = bucketInfo;
  }

  public OmBucketInfo resolveBucket(OzoneManager ozoneManager, String volume, String bucket) throws IOException {
    String bucketKey = ozoneManager.getMetadataManager().getBucketKey(volume, bucket);

    CacheValue<OmBucketInfo> value = ozoneManager.getMetadataManager().getBucketTable()
        .getCacheValue(new CacheKey<>(bucketKey));
    if (value == null || value.getCacheValue() == null) {
      throw new OMException("Bucket not found: " + volume + "/" + bucket, OMException.ResultCodes.BUCKET_NOT_FOUND);
    }
    bucketInfo = value.getCacheValue();
    return bucketInfo;
  }

  protected BucketLayout getBucketLayout() {
    return bucketInfo == null ? BucketLayout.DEFAULT : bucketInfo.getBucketLayout();
  }
  public OmBucketInfo getBucketInfo() {
    return bucketInfo;
  }
}
