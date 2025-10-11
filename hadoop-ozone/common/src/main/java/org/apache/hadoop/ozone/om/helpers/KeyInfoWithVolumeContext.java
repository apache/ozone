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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetKeyInfoResponse;

/**
 * Encloses a {@link OmKeyInfo} and optionally a volume context.
 */
public class KeyInfoWithVolumeContext {
  /**
   * Volume arguments.
   */
  private final Optional<OmVolumeArgs> volumeArgs;

  /**
   * To be used for client-side operations involving KMS like getDEK().
   */
  private final Optional<String> userPrincipal;

  private final OmKeyInfo keyInfo;

  public KeyInfoWithVolumeContext(OmVolumeArgs volumeArgs,
                                  String userPrincipal,
                                  OmKeyInfo keyInfo) {
    this.volumeArgs = Optional.ofNullable(volumeArgs);
    this.userPrincipal = Optional.ofNullable(userPrincipal);
    this.keyInfo = keyInfo;
  }

  public static KeyInfoWithVolumeContext fromProtobuf(
      GetKeyInfoResponse proto) throws IOException {
    return newBuilder()
        .setVolumeArgs(proto.hasVolumeInfo() ?
            OmVolumeArgs.getFromProtobuf(proto.getVolumeInfo()) : null)
        .setUserPrincipal(proto.getUserPrincipal())
        .setKeyInfo(OmKeyInfo.getFromProtobuf(proto.getKeyInfo()))
        .build();
  }

  public GetKeyInfoResponse toProtobuf(int clientVersion) {
    GetKeyInfoResponse.Builder builder = GetKeyInfoResponse.newBuilder();
    volumeArgs.ifPresent(v -> builder.setVolumeInfo(v.getProtobuf()));
    userPrincipal.ifPresent(builder::setUserPrincipal);
    builder.setKeyInfo(keyInfo.getProtobuf(clientVersion));
    return builder.build();
  }

  public OmKeyInfo getKeyInfo() {
    return keyInfo;
  }

  public Optional<OmVolumeArgs> getVolumeArgs() {
    return volumeArgs;
  }

  public Optional<String> getUserPrincipal() {
    return userPrincipal;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for KeyInfoWithVolumeContext.
   */
  public static class Builder {
    private OmVolumeArgs volumeArgs;
    private String userPrincipal;
    private OmKeyInfo keyInfo;

    public Builder setVolumeArgs(OmVolumeArgs volumeArgs) {
      this.volumeArgs = volumeArgs;
      return this;
    }

    public Builder setUserPrincipal(String userPrincipal) {
      this.userPrincipal = userPrincipal;
      return this;
    }

    public Builder setKeyInfo(OmKeyInfo keyInfo) {
      this.keyInfo = keyInfo;
      return this;
    }

    public KeyInfoWithVolumeContext build() {
      return new KeyInfoWithVolumeContext(volumeArgs, userPrincipal, keyInfo);
    }
  }
}
