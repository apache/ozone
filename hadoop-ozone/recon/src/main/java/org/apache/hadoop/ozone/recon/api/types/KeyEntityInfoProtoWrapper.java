/*
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

package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * POJO object wrapper for metadata of a given key/file. This class wraps a KeyInfo protobuf
 * object and delegates most accessors to it.
 */
public final class KeyEntityInfoProtoWrapper {

  public static Codec<KeyEntityInfoProtoWrapper> getCodec() {
    return new DelegatedCodec<>(
        Proto2Codec.get(OzoneManagerProtocolProtos.KeyInfo.getDefaultInstance()),
        KeyEntityInfoProtoWrapper::getFromProtobuf,
        KeyEntityInfoProtoWrapper::toProtobuf,
        KeyEntityInfoProtoWrapper.class);
  }

  private final OzoneManagerProtocolProtos.KeyInfo keyInfoProto;

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

  private KeyEntityInfoProtoWrapper(OzoneManagerProtocolProtos.KeyInfo proto) {
    keyInfoProto = proto;
    replicationConfig = ReplicationConfig.fromProto(proto.getType(), proto.getFactor(),
        proto.getEcReplicationConfig());
    this.replicatedSize = QuotaUtil.getReplicatedSize(getSize(), getReplicationConfig());
  }

  public static KeyEntityInfoProtoWrapper getFromProtobuf(OzoneManagerProtocolProtos.KeyInfo keyInfo) {
    return new KeyEntityInfoProtoWrapper(keyInfo);
  }

  public OzoneManagerProtocolProtos.KeyInfo toProtobuf() {
    throw new UnsupportedOperationException("This method is not supported.");
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
    return keyInfoProto.getDataSize();
  }

  @JsonProperty("replicatedSize")
  public long getReplicatedSize() {
    return replicatedSize;
  }

  @JsonProperty("replicationInfo")
  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  @JsonProperty("creationTime")
  public long getCreationTime() {
    return keyInfoProto.getCreationTime();
  }

  @JsonProperty("modificationTime")
  public long getModificationTime() {
    return keyInfoProto.getModificationTime();
  }

  @JsonProperty("isKey")
  public boolean getIsKey() {
    return keyInfoProto.getIsFile();
  }

  public long getParentId() {
    return keyInfoProto.getParentID();
  }

  public String getVolumeName() {
    return keyInfoProto.getVolumeName();
  }

  public String getBucketName() {
    return keyInfoProto.getBucketName();
  }

  /** Returns the key name of the key stored in the OM Key Info object. */
  public String getKeyName() {
    return keyInfoProto.getKeyName();
  }
}
