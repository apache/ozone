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

import static org.apache.hadoop.ozone.OzoneConsts.ETAG;

import java.io.IOException;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BasicKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListKeysRequest;

/**
 * Lightweight OmKeyInfo class.
 */
public final class BasicOmKeyInfo {

  private final String volumeName;
  private final String bucketName;
  private final String keyName;
  private final long dataSize;
  private final long creationTime;
  private final long modificationTime;
  private final ReplicationConfig replicationConfig;
  private final boolean isFile;
  private final String eTag;
  private String ownerName;
  private final boolean isEncrypted;

  private BasicOmKeyInfo(Builder b) {
    this.volumeName = b.volumeName;
    this.bucketName = b.bucketName;
    this.keyName = b.keyName;
    this.dataSize = b.dataSize;
    this.creationTime = b.creationTime;
    this.modificationTime = b.modificationTime;
    this.replicationConfig = b.replicationConfig;
    this.isFile = b.isFile;
    this.eTag = StringUtils.isNotEmpty(b.eTag) ? b.eTag : null;
    this.ownerName = b.ownerName;
    this.isEncrypted = b.isEncrypted;
  }

  private BasicOmKeyInfo(OmKeyInfo b) {
    this.volumeName = b.getVolumeName();
    this.bucketName = b.getBucketName();
    this.keyName = b.getKeyName();
    this.dataSize = b.getDataSize();
    this.creationTime = b.getCreationTime();
    this.modificationTime = b.getModificationTime();
    this.replicationConfig = b.getReplicationConfig();
    this.isFile = b.isFile();
    this.eTag = b.getMetadata().get(ETAG);
    this.ownerName = b.getOwnerName();
    this.isEncrypted = b.getFileEncryptionInfo() != null;
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

  public String getETag() {
    return eTag;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public boolean isEncrypted() {
    return isEncrypted;
  }

  public long getReplicatedSize() {
    return QuotaUtil.getReplicatedSize(getDataSize(), replicationConfig);
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
    private String eTag;
    private String ownerName;
    private boolean isEncrypted;

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

    public Builder setETag(String etag) {
      this.eTag = etag;
      return this;
    }

    public Builder setOwnerName(String ownerName) {
      this.ownerName = ownerName;
      return this;
    }

    public Builder setIsEncrypted(boolean isEncrypted) {
      this.isEncrypted = isEncrypted;
      return this;
    }

    public BasicOmKeyInfo build() {
      return new BasicOmKeyInfo(this);
    }
  }

  public BasicKeyInfo getProtobuf() {
    BasicKeyInfo.Builder builder = BasicKeyInfo.newBuilder()
        .setKeyName(keyName)
        .setDataSize(dataSize)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setIsFile(isFile)
        .setType(replicationConfig.getReplicationType())
        .setIsEncrypted(isEncrypted);
    if (ownerName != null) {
      builder.setOwnerName(ownerName);
    }
    if (replicationConfig instanceof ECReplicationConfig) {
      builder.setEcReplicationConfig(
          ((ECReplicationConfig) replicationConfig).toProto());
    } else {
      builder.setFactor(ReplicationConfig.getLegacyFactor(replicationConfig));
    }
    if (StringUtils.isNotEmpty(eTag)) {
      builder.setETag(eTag);
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
        .setETag(basicKeyInfo.getETag())
        .setOwnerName(basicKeyInfo.getOwnerName())
        .setIsEncrypted(basicKeyInfo.getIsEncrypted());

    if (basicKeyInfo.hasIsFile()) {
      builder.setIsFile(basicKeyInfo.getIsFile());
    } else {
      builder.setIsFile(!keyName.endsWith("/"));
    }

    return builder.build();
  }

  public static BasicOmKeyInfo getFromProtobuf(String volumeName,
      String bucketName, BasicKeyInfo basicKeyInfo) throws IOException {
    if (basicKeyInfo == null) {
      return null;
    }

    String keyName = basicKeyInfo.getKeyName();

    Builder builder = new Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(basicKeyInfo.getDataSize())
        .setCreationTime(basicKeyInfo.getCreationTime())
        .setModificationTime(basicKeyInfo.getModificationTime())
        .setReplicationConfig(ReplicationConfig.fromProto(
            basicKeyInfo.getType(),
            basicKeyInfo.getFactor(),
            basicKeyInfo.getEcReplicationConfig()))
        .setETag(basicKeyInfo.getETag())
        .setOwnerName(basicKeyInfo.getOwnerName())
        .setIsEncrypted(basicKeyInfo.getIsEncrypted());

    if (basicKeyInfo.hasIsFile()) {
      builder.setIsFile(basicKeyInfo.getIsFile());
    } else {
      builder.setIsFile(!keyName.endsWith("/"));
    }

    return builder.build();
  }

  @Override
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
        Objects.equals(eTag, basicOmKeyInfo.eTag) &&
        isFile == basicOmKeyInfo.isFile &&
        ownerName.equals(basicOmKeyInfo.ownerName) &&
        isEncrypted == basicOmKeyInfo.isEncrypted;
  }

  @Override
  public int hashCode() {
    return Objects.hash(volumeName, bucketName, keyName);
  }

  public static BasicOmKeyInfo fromOmKeyInfo(OmKeyInfo omKeyInfo) {
    return new BasicOmKeyInfo(omKeyInfo);
  }
}
