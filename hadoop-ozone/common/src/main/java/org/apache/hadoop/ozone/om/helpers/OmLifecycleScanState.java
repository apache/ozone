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

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleScanState;

/**
 * POJO for LifecycleScanState.
 */
public class OmLifecycleScanState {
  private String bucketKey;
  private long bucketObjID;
  private long lifecycleConfigurationUpdateID;
  private long scanStartTime;
  private Long scanEndTime;
  private String lastScannedKey;
  private String lastScannedDir;
  private String lastScannedDirKey;

  private static final Codec<OmLifecycleScanState> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(LifecycleScanState.getDefaultInstance()),
      OmLifecycleScanState::getFromProtobuf,
      OmLifecycleScanState::getProtobuf,
      OmLifecycleScanState.class);

  public static Codec<OmLifecycleScanState> getCodec() {
    return CODEC;
  }

  public OmLifecycleScanState(String bucketKey, long scanStartTime) {
    this.bucketKey = bucketKey;
    this.scanStartTime = scanStartTime;
  }

  private OmLifecycleScanState(Builder builder) {
    this.bucketKey = builder.bucketKey;
    this.bucketObjID = builder.bucketObjID;
    this.lifecycleConfigurationUpdateID = builder.lifecycleConfigurationUpdateID;
    this.scanStartTime = builder.scanStartTime;
    this.scanEndTime = builder.scanEndTime;
    this.lastScannedKey = builder.lastScannedKey;
    this.lastScannedDir = builder.lastScannedDir;
    this.lastScannedDirKey = builder.lastScannedDirKey;
  }

  public String getBucketKey() {
    return bucketKey;
  }

  public long getBucketObjID() {
    return bucketObjID;
  }

  public long getLifecycleConfigurationUpdateID() {
    return lifecycleConfigurationUpdateID;
  }

  public long getScanStartTime() {
    return scanStartTime;
  }

  public Long getScanEndTime() {
    return scanEndTime;
  }

  public void setScanEndTime(Long scanEndTime) {
    this.scanEndTime = scanEndTime;
  }

  public String getLastScannedKey() {
    return lastScannedKey;
  }

  public void setLastScannedKey(String lastScannedKey) {
    this.lastScannedKey = lastScannedKey;
  }

  public String getLastScannedDir() {
    return lastScannedDir;
  }

  public void setLastScannedDir(String dir) {
    this.lastScannedDir = dir;
  }

  public String getLastScannedDirKey() {
    return lastScannedDirKey;
  }

  public LifecycleScanState getProtobuf() {
    LifecycleScanState.Builder builder = LifecycleScanState.newBuilder()
        .setBucketKey(bucketKey)
        .setBucketObjID(bucketObjID)
        .setLifecycleConfigurationUpdateID(lifecycleConfigurationUpdateID)
        .setScanStartTime(scanStartTime);

    if (scanEndTime != null) {
      builder.setScanEndTime(scanEndTime);
    }
    if (lastScannedKey != null) {
      builder.setLastScannedKey(lastScannedKey);
    }
    if (lastScannedDir != null) {
      builder.setLastScannedDir(lastScannedDir);
    }
    if (lastScannedDirKey != null) {
      builder.setLastScannedDirKey(lastScannedDirKey);
    }
    return builder.build();
  }

  public static OmLifecycleScanState getFromProtobuf(LifecycleScanState proto) {
    Builder builder = new Builder()
        .setBucketKey(proto.getBucketKey())
        .setBucketObjID(proto.getBucketObjID())
        .setLifecycleConfigurationUpdateID(proto.getLifecycleConfigurationUpdateID())
        .setScanStartTime(proto.getScanStartTime());

    if (proto.hasScanEndTime()) {
      builder.setScanEndTime(proto.getScanEndTime());
    }
    if (proto.hasLastScannedKey()) {
      builder.setLastScannedKey(proto.getLastScannedKey());
    }
    if (proto.hasLastScannedDir()) {
      builder.setLastScannedDir(proto.getLastScannedDir());
    }
    if (proto.hasLastScannedDirKey()) {
      builder.setLastScannedDirKey(proto.getLastScannedDirKey());
    }
    return builder.build();
  }

  @Override
  public String toString() {
    return "OmLifecycleScanState{" +
        "bucketKey='" + bucketKey + '\'' +
        ", bucketObjID=" + bucketObjID +
        ", lifecycleConfigurationUpdateID=" + lifecycleConfigurationUpdateID +
        ", scanStartTime=" + scanStartTime +
        ", scanEndTime=" + scanEndTime +
        ", lastScannedKey='" + lastScannedKey + '\'' +
        ", lastScannedDir='" + lastScannedDir + '\'' +
        ", lastScannedDirKey='" + lastScannedDirKey + '\'' +
        '}';
  }

  public Builder toBuilder() {
    return new Builder()
        .setBucketKey(bucketKey)
        .setBucketObjID(bucketObjID)
        .setLifecycleConfigurationUpdateID(lifecycleConfigurationUpdateID)
        .setScanStartTime(scanStartTime)
        .setScanEndTime(scanEndTime)
        .setLastScannedKey(lastScannedKey)
        .setLastScannedDir(lastScannedDir)
        .setLastScannedDirKey(lastScannedDirKey);
  }

  /**
   * Builder for OmLifecycleScanState.
   */
  public static class Builder {
    private String bucketKey;
    private long bucketObjID;
    private long lifecycleConfigurationUpdateID;
    private long scanStartTime;
    private Long scanEndTime;
    private String lastScannedKey;
    private String lastScannedDir;
    private String lastScannedDirKey;

    public Builder setBucketKey(String bucketKey) {
      this.bucketKey = bucketKey;
      return this;
    }

    public long getBucketObjID() {
      return bucketObjID;
    }

    public Builder setBucketObjID(long bucketObjID) {
      this.bucketObjID = bucketObjID;
      return this;
    }

    public long getLifecycleConfigurationUpdateID() {
      return lifecycleConfigurationUpdateID;
    }

    public Builder setLifecycleConfigurationUpdateID(long lifecycleConfigurationUpdateID) {
      this.lifecycleConfigurationUpdateID = lifecycleConfigurationUpdateID;
      return this;
    }

    public Builder setScanStartTime(long scanStartTime) {
      this.scanStartTime = scanStartTime;
      return this;
    }

    public Builder setScanEndTime(Long scanEndTime) {
      this.scanEndTime = scanEndTime;
      return this;
    }

    public Builder setLastScannedKey(String keyTableKey) {
      this.lastScannedKey = keyTableKey;
      return this;
    }

    public String getLastScannedKey() {
      return lastScannedKey;
    }

    public String getLastScannedDir() {
      return lastScannedDir;
    }

    public Builder setLastScannedDir(String dirTableKey) {
      this.lastScannedDir = dirTableKey;
      return this;
    }

    public String getLastScannedDirKey() {
      return lastScannedDirKey;
    }

    public Builder setLastScannedDirKey(String lastScannedDirKey) {
      this.lastScannedDirKey = lastScannedDirKey;
      return this;
    }

    public OmLifecycleScanState build() {
      return new OmLifecycleScanState(this);
    }
  }
}
