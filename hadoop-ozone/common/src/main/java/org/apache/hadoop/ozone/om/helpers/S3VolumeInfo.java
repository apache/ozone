/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3VolumeInfoResponse;

/**
 * A class that encapsulates GetS3VolumeInfoResponse protobuf message.
 */
public class S3VolumeInfo {

  /**
   * Various volume arguments.
   */
  private final OmVolumeArgs omVolumeArgs;

  /**
   * Volume name to be created for this tenant.
   * Default volume name would be the same as tenant name if unspecified.
   */
  private final String userPrincipal;

  public S3VolumeInfo(OmVolumeArgs omVolumeArgs, String userPrincipal) {
    this.omVolumeArgs = omVolumeArgs;
    this.userPrincipal = userPrincipal;
  }

  public OmVolumeArgs getOmVolumeArgs() {
    return omVolumeArgs;
  }

  public String getUserPrincipal() {
    return userPrincipal;
  }

  public static S3VolumeInfo fromProtobuf(GetS3VolumeInfoResponse resp) {
    return new S3VolumeInfo(
        OmVolumeArgs.getFromProtobuf(resp.getVolumeInfo()),
        resp.getUserPrincipal());
  }

  public GetS3VolumeInfoResponse getProtobuf() {
    return GetS3VolumeInfoResponse.newBuilder()
        .setVolumeInfo(omVolumeArgs.getProtobuf())
        .setUserPrincipal(userPrincipal)
        .build();
  }

  public static S3VolumeInfo.Builder newBuilder() {
    return new S3VolumeInfo.Builder();
  }

  /**
   * Builder for S3VolumeInfo.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class Builder {
    private OmVolumeArgs omVolumeArgs;
    private String userPrincipal;

    private Builder() {
    }

    public static Builder aS3VolumeInfo() {
      return new Builder();
    }

    public Builder setOmVolumeArgs(OmVolumeArgs omVolumeArgs) {
      this.omVolumeArgs = omVolumeArgs;
      return this;
    }

    public Builder setUserPrincipal(String userPrincipal) {
      this.userPrincipal = userPrincipal;
      return this;
    }

    public S3VolumeInfo build() {
      return new S3VolumeInfo(omVolumeArgs, userPrincipal);
    }
  }
}
