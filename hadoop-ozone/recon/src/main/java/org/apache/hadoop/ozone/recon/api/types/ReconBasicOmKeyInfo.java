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

package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Lightweight OmKeyInfo class.
 */
public final class ReconBasicOmKeyInfo {

  private final String volumeName;
  private final String bucketName;
  private final String keyName;
  private final long dataSize;
  private final long creationTime;
  private final long modificationTime;

  /** This is key table key of rocksDB and will help UI to implement pagination
   * where UI will use the last record key to send in API as preKeyPrefix. */
  @JsonProperty("key")
  private String key;

  /** Path of a key/file. */
  @JsonProperty("path")
  private String path;

  @JsonProperty("replicatedSize")
  private final long replicatedSize;

  @JsonProperty("replicationInfo")
  private final ReplicationConfig replicationConfig;

  private final boolean isFile;
  private final long parentId;

  public static Codec<ReconBasicOmKeyInfo> getCodec() {
    return DelegatedCodec.decodeOnly(
        Proto2Codec.get(OzoneManagerProtocolProtos.KeyInfoProtoLight.getDefaultInstance()),
        ReconBasicOmKeyInfo::getFromProtobuf,
        ReconBasicOmKeyInfo.class);
  }

  private ReconBasicOmKeyInfo(Builder b) {
    this.volumeName = b.volumeName;
    this.bucketName = b.bucketName;
    this.keyName = b.keyName;
    this.dataSize = b.dataSize;
    this.creationTime = b.creationTime;
    this.modificationTime = b.modificationTime;
    this.replicationConfig = b.replicationConfig;
    this.replicatedSize = QuotaUtil.getReplicatedSize(getDataSize(), replicationConfig);
    this.isFile = b.isFile;
    this.parentId = b.parentId;
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

  @JsonProperty("creationTime")
  public long getCreationTime() {
    return creationTime;
  }

  @JsonProperty("modificationTime")
  public long getModificationTime() {
    return modificationTime;
  }

  @JsonProperty("replicationInfo")
  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  public boolean isFile() {
    return isFile;
  }

  @JsonProperty("replicatedSize")
  public long getReplicatedSize() {
    return replicatedSize;
  }

  @JsonProperty("key")
  public String getKey() {
    if (key == null) {
      throw new IllegalStateException("Key must be set to correctly serialize this object.");
    }
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  @JsonProperty("path")
  public String getPath() {
    if (path == null) {
      throw new IllegalStateException("Path must be set to correctly serialize this object.");
    }
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  @JsonProperty("size")
  public long getSize() {
    return dataSize;
  }

  @JsonProperty("isKey")
  public boolean getIsKey() {
    return isFile();
  }

  public long getParentId() {
    return parentId;
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
    private long parentId;

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

    public Builder setParentId(long parentId) {
      this.parentId = parentId;
      return this;
    }

    public ReconBasicOmKeyInfo build() {
      return new ReconBasicOmKeyInfo(this);
    }
  }

  public static ReconBasicOmKeyInfo getFromProtobuf(OzoneManagerProtocolProtos.KeyInfoProtoLight keyInfoProtoLight) {
    if (keyInfoProtoLight == null) {
      return null;
    }

    String keyName = keyInfoProtoLight.getKeyName();

    Builder builder = new Builder()
        .setVolumeName(keyInfoProtoLight.getVolumeName())
        .setBucketName(keyInfoProtoLight.getBucketName())
        .setKeyName(keyName)
        .setDataSize(keyInfoProtoLight.getDataSize())
        .setCreationTime(keyInfoProtoLight.getCreationTime())
        .setModificationTime(keyInfoProtoLight.getModificationTime())
        .setReplicationConfig(ReplicationConfig.fromProto(
            keyInfoProtoLight.getType(),
            keyInfoProtoLight.getFactor(),
            keyInfoProtoLight.getEcReplicationConfig()))
        .setIsFile(!keyName.endsWith("/"))
        .setParentId(keyInfoProtoLight.getParentID());

    return builder.build();
  }

  /**
   * Converts a KeyInfo protobuf object into a ReconBasicOmKeyInfo instance.
   * This method extracts only the essential fields required for Recon event handling, avoiding the overhead of
   * deserializing unused metadata such as KeyLocationList or ACLs.
   *
   * @param keyInfoProto required for deserialization.
   * @return the deserialized lightweight ReconBasicOmKeyInfo object.
   */
  public static ReconBasicOmKeyInfo getFromProtobuf(OzoneManagerProtocolProtos.KeyInfo keyInfoProto) {
    if (keyInfoProto == null) {
      return null;
    }

    String keyName = keyInfoProto.getKeyName();

    Builder builder = new Builder()
        .setVolumeName(keyInfoProto.getVolumeName())
        .setBucketName(keyInfoProto.getBucketName())
        .setKeyName(keyName)
        .setDataSize(keyInfoProto.getDataSize())
        .setCreationTime(keyInfoProto.getCreationTime())
        .setModificationTime(keyInfoProto.getModificationTime())
        .setReplicationConfig(ReplicationConfig.fromProto(
            keyInfoProto.getType(),
            keyInfoProto.getFactor(),
            keyInfoProto.getEcReplicationConfig()))
        .setIsFile(!keyName.endsWith("/"))
        .setParentId(keyInfoProto.getParentID());

    return builder.build();
  }

  public OzoneManagerProtocolProtos.KeyInfoProtoLight toProtobuf() {
    throw new UnsupportedOperationException("This method is not supported.");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReconBasicOmKeyInfo basicOmKeyInfo = (ReconBasicOmKeyInfo) o;
    return volumeName.equals(basicOmKeyInfo.volumeName) &&
        bucketName.equals(basicOmKeyInfo.bucketName) &&
        keyName.equals(basicOmKeyInfo.keyName) &&
        dataSize == basicOmKeyInfo.dataSize &&
        creationTime == basicOmKeyInfo.creationTime &&
        modificationTime == basicOmKeyInfo.modificationTime &&
        replicationConfig.equals(basicOmKeyInfo.replicationConfig) &&
        isFile == basicOmKeyInfo.isFile;
  }

  @Override
  public int hashCode() {
    return Objects.hash(volumeName, bucketName, keyName);
  }

}
