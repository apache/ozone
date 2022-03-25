/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKey;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * A class that encapsulates information of an open key.
 * Including basic info of the bucket and volume it belongs to.
 */
public final class OmOpenKeyInfo {

  private final String volumeName;
  private final String bucketName;
  private final String keyName;
  private final long clientID;
  private final long parentID;
  private final BucketLayout bucketLayout;

  private OmOpenKeyInfo(Builder builder) {
    this.volumeName = builder.volumeName;
    this.bucketName = builder.bucketName;
    this.keyName = builder.keyName;
    this.clientID = builder.clientID;
    this.parentID = builder.parentID;
    this.bucketLayout = builder.bucketLayout;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getKeyName() {
    return keyName;
  }

  public long getClientID() {
    return clientID;
  }

  public long getParentID() {
    return parentID;
  }

  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }

  public String toString() {
    if (bucketLayout.isFileSystemOptimized()) {
      return parentID + OM_KEY_PREFIX + keyName + OM_KEY_PREFIX + clientID;
    } else {
      return OM_KEY_PREFIX + volumeName + OM_KEY_PREFIX + bucketName
          + OM_KEY_PREFIX + keyName + OM_KEY_PREFIX + clientID;
    }
  }

  public OpenKey toProto() {
    OpenKey.Builder builder = OpenKey.newBuilder()
        .setName(keyName)
        .setClientID(clientID);
    /* TODO: change the proto to support FSO keys
    if (bucketLayout.isFileSystemOptimized()) {
      builder.setParentID(parentID);
    }
    */
    return builder.build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for OmOpenKeyInfo.
   */
  public static class Builder {
    private String volumeName;
    private String bucketName;
    private String keyName;
    private long clientID;
    private long parentID;
    private BucketLayout bucketLayout;

    public Builder() {
      bucketLayout = BucketLayout.DEFAULT;
    }

    public Builder setVolumeName(String volumeName) {
      this.volumeName = volumeName;
      return this;
    }

    public Builder setBucketName(String bucketName) {
      this.bucketName = bucketName;
      return this;
    }

    public Builder setKeyName(String keyName) {
      this.keyName = keyName;
      return this;
    }

    public Builder setClientID(long clientID) {
      this.clientID = clientID;
      return this;
    }

    public Builder setParentID(long parentID) {
      this.parentID = parentID;
      return this;
    }

    public Builder setBucketLayout(BucketLayout bucketLayout) {
      this.bucketLayout = bucketLayout;
      return this;
    }

    public OmOpenKeyInfo build() {
      return new OmOpenKeyInfo(this);
    }
  }
}
