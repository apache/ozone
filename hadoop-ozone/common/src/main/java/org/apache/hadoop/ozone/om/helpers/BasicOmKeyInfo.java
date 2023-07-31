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
package org.apache.hadoop.ozone.om.helpers;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BasicKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListKeysRequest;

/**
 * Lightweight OmKeyInfo class.
 */
public class BasicOmKeyInfo {

  private String volumeName;
  private String bucketName;
  private String keyName;
  private long dataSize;
  private long creationTime;
  private long modificationTime;
  private ReplicationConfig replicationConfig;
  private boolean isFile;

  @SuppressWarnings("parameternumber")
  public BasicOmKeyInfo(String volumeName, String bucketName, String keyName,
                        long dataSize, long creationTime, long modificationTime,
                        ReplicationConfig replicationConfig, boolean isFile) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.dataSize = dataSize;
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
    this.replicationConfig = replicationConfig;
    this.isFile = isFile;
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

  public long getDataSize() {
    return dataSize;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  public boolean isFile() {
    return isFile;
  }

  /**
   * Builder of BasicOmKeyInfo.
   */
  public static class Builder {
    private String volumeName;
    private String bucketName;
    private String keyName;
    private long dataSize;
    private long creationTime;
    private long modificationTime;
    private ReplicationConfig replicationConfig;
    private boolean isFile;

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

    public Builder setDataSize(long dataSize) {
      this.dataSize = dataSize;
      return this;
    }

    public Builder setCreationTime(long creationTime) {
      this.creationTime = creationTime;
      return this;
    }

    public Builder setModificationTime(long modificationTime) {
      this.modificationTime = modificationTime;
      return this;
    }

    public Builder setReplicationConfig(ReplicationConfig replicationConfig) {
      this.replicationConfig = replicationConfig;
      return this;
    }

    public Builder setIsFile(boolean isFile) {
      this.isFile = isFile;
      return this;
    }

    public BasicOmKeyInfo build() {
      return new BasicOmKeyInfo(volumeName, bucketName, keyName, dataSize,
          creationTime, modificationTime, replicationConfig, isFile);
    }
  }

  public BasicKeyInfo getProtobuf() {
    BasicKeyInfo.Builder builder = BasicKeyInfo.newBuilder()
        .setKeyName(keyName)
        .setDataSize(dataSize)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setType(replicationConfig.getReplicationType());
    if (replicationConfig instanceof ECReplicationConfig) {
      builder.setEcReplicationConfig(
          ((ECReplicationConfig) replicationConfig).toProto());
    } else {
      builder.setFactor(ReplicationConfig.getLegacyFactor(replicationConfig));
    }

    return builder.build();
  }

  public static BasicOmKeyInfo getFromProtobuf(BasicKeyInfo basicKeyInfo,
                                               ListKeysRequest request)
      throws IOException {
    if (basicKeyInfo == null || request == null) {
      return null;
    }

    String keyName = basicKeyInfo.getKeyName();

    Builder builder = new Builder()
        .setVolumeName(request.getVolumeName())
        .setBucketName(request.getBucketName())
        .setKeyName(keyName)
        .setDataSize(basicKeyInfo.getDataSize())
        .setCreationTime(basicKeyInfo.getCreationTime())
        .setModificationTime(basicKeyInfo.getModificationTime())
        .setReplicationConfig(ReplicationConfig.fromProto(
            basicKeyInfo.getType(),
            basicKeyInfo.getFactor(),
            basicKeyInfo.getEcReplicationConfig()))
        .setIsFile(!keyName.endsWith("/"));

    return builder.build();
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BasicOmKeyInfo basicOmKeyInfo = (BasicOmKeyInfo) o;
    return volumeName.equals(basicOmKeyInfo.volumeName) &&
        bucketName.equals(basicOmKeyInfo.bucketName) &&
        keyName.equals(basicOmKeyInfo.keyName) &&
        dataSize == basicOmKeyInfo.dataSize &&
        creationTime == basicOmKeyInfo.creationTime &&
        modificationTime == basicOmKeyInfo.modificationTime &&
        replicationConfig.equals(basicOmKeyInfo.replicationConfig) &&
        isFile == basicOmKeyInfo.isFile;
  }

  public int hashCode() {
    return Objects.hash(volumeName, bucketName, keyName);
  }

  public static BasicOmKeyInfo fromOmKeyInfo(OmKeyInfo omKeyInfo) {
    return new BasicOmKeyInfo(
        omKeyInfo.getVolumeName(),
        omKeyInfo.getBucketName(),
        omKeyInfo.getKeyName(),
        omKeyInfo.getDataSize(),
        omKeyInfo.getCreationTime(),
        omKeyInfo.getModificationTime(),
        omKeyInfo.getReplicationConfig(),
        omKeyInfo.isFile());
  }
}
