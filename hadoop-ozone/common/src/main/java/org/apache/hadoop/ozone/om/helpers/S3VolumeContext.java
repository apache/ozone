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

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3VolumeContextResponse;

/**
 * A class that encapsulates GetS3VolumeContextResponse protobuf message.
 */
public class S3VolumeContext {

  /**
   * Various volume arguments.
   */
  private final OmVolumeArgs omVolumeArgs;

  /**
   * Piggybacked username (principal) response.
   * To be used for client-side operations involving KMS like getDEK().
   */
  private final String userPrincipal;

  public S3VolumeContext(OmVolumeArgs omVolumeArgs, String userPrincipal) {
    this.omVolumeArgs = omVolumeArgs;
    this.userPrincipal = userPrincipal;
  }

  public OmVolumeArgs getOmVolumeArgs() {
    return omVolumeArgs;
  }

  public String getUserPrincipal() {
    return userPrincipal;
  }

  public static S3VolumeContext fromProtobuf(GetS3VolumeContextResponse resp) {
    return new S3VolumeContext(
        OmVolumeArgs.getFromProtobuf(resp.getVolumeInfo()),
        resp.getUserPrincipal());
  }

  public GetS3VolumeContextResponse getProtobuf() {
    return GetS3VolumeContextResponse.newBuilder()
        .setVolumeInfo(omVolumeArgs.getProtobuf())
        .setUserPrincipal(userPrincipal)
        .build();
  }

  public static S3VolumeContext.Builder newBuilder() {
    return new S3VolumeContext.Builder();
  }

  /**
   * Builder for S3VolumeContext.
   */
  public static final class Builder {
    private OmVolumeArgs omVolumeArgs;
    private String userPrincipal;

    private Builder() {
    }

    public Builder setOmVolumeArgs(OmVolumeArgs omVolumeArgs) {
      this.omVolumeArgs = omVolumeArgs;
      return this;
    }

    public Builder setUserPrincipal(String userPrincipal) {
      this.userPrincipal = userPrincipal;
      return this;
    }

    public S3VolumeContext build() {
      return new S3VolumeContext(omVolumeArgs, userPrincipal);
    }
  }
}
