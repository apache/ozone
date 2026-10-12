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

package org.apache.hadoop.ozone.om.helpers;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Wrapper class to return BucketInfo and optionally the S3 user principal.
 */
public final class BucketInfoWithVolumeContext {
  private final OmBucketInfo bucketInfo;
  private final String userPrincipal;

  private BucketInfoWithVolumeContext(Builder builder) {
    this.bucketInfo = builder.bucketInfo;
    this.userPrincipal = builder.userPrincipal;
  }

  public OmBucketInfo getBucketInfo() {
    return bucketInfo;
  }

  public Optional<String> getUserPrincipal() {
    return Optional.ofNullable(userPrincipal);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static BucketInfoWithVolumeContext fromProtobuf(OMResponse proto) throws IOException {
    return newBuilder()
        .setUserPrincipal(proto.hasUserPrincipal() ? proto.getUserPrincipal() : null)
        .setBucketInfo(OmBucketInfo.getFromProtobuf(proto.getInfoBucketResponse().getBucketInfo()))
        .build();
  }

  /**
   * Builder for BucketInfoWithVolumeContext.
   */
  public static class Builder {
    private OmBucketInfo bucketInfo;
    private String userPrincipal;

    public Builder setBucketInfo(OmBucketInfo bucket) {
      this.bucketInfo = bucket;
      return this;
    }

    public Builder setUserPrincipal(String principal) {
      this.userPrincipal = principal;
      return this;
    }

    public BucketInfoWithVolumeContext build() {
      return new BucketInfoWithVolumeContext(this);
    }
  }
}
