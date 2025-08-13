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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartKeyInfoLight;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfoLight;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * This class is the light-weight version of OmMultipartKeyInfo.
 * It is used by Recon to reduce payload and deserialization cost.
 */
public class OmMultipartKeyInfoLight {

  private final String uploadID;
  private final long creationTime;
  private final ReplicationConfig replicationConfig;
  private final Map<Integer, PartKeyInfoLight> partKeyInfoMap;
  private final long objectID;
  private final long updateID;
  private final long parentID;

  public OmMultipartKeyInfoLight(Builder b) {
    this.uploadID = b.uploadID;
    this.creationTime = b.creationTime;
    this.replicationConfig = b.replicationConfig;
    this.partKeyInfoMap = b.partKeyInfoMap;
    this.objectID = b.objectID;
    this.updateID = b.updateID;
    this.parentID = b.parentID;
  }

  public String getUploadID() {
    return uploadID;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  public Map<Integer, PartKeyInfoLight> getPartKeyInfoMap() {
    return partKeyInfoMap;
  }

  public long getObjectID() {
    return objectID;
  }

  public long getUpdateID() {
    return updateID;
  }

  public long getParentID() {
    return parentID;
  }

  public static OmMultipartKeyInfoLight getFromProtobuf(
      MultipartKeyInfoLight multipartKeyInfoLight) {
    return new Builder()
        .setUploadID(multipartKeyInfoLight.getUploadID())
        .setCreationTime(multipartKeyInfoLight.getCreationTime())
        .setReplicationConfig(
            ReplicationConfig.fromProto(
                multipartKeyInfoLight.getType(),
                multipartKeyInfoLight.getFactor(),
                multipartKeyInfoLight.getEcReplicationConfig()
            ))
        .setPartKeyInfoMap(
            multipartKeyInfoLight.getPartKeyInfoListList().stream()
                .collect(Collectors.toMap(
                    PartKeyInfoLight::getPartNumber,
                    partKeyInfo -> partKeyInfo,
                    (v1, v2) -> v1,
                    TreeMap::new
                )))
        .setObjectID(multipartKeyInfoLight.getObjectID())
        .setUpdateID(multipartKeyInfoLight.getUpdateID())
        .setParentID(multipartKeyInfoLight.getParentID())
        .build();
  }

  public MultipartKeyInfoLight getProtobuf() {
    MultipartKeyInfoLight.Builder builder = MultipartKeyInfoLight.newBuilder()
        .setUploadID(uploadID)
        .setCreationTime(creationTime)
        .setObjectID(objectID)
        .setUpdateID(updateID)
        .setParentID(parentID);

    ReplicationConfig repConfig = getReplicationConfig();
    builder.setType(repConfig.getReplicationType());

    if (repConfig.getReplicationType() == HddsProtos.ReplicationType.RATIS) {
      builder.setFactor(ReplicationConfig.getLegacyFactor(repConfig));
    } else if (repConfig.getReplicationType() ==
        HddsProtos.ReplicationType.EC) {
      builder.setEcReplicationConfig(
          ((ECReplicationConfig) repConfig).toProto());
    }

    builder.addAllPartKeyInfoList(partKeyInfoMap.values());

    return builder.build();
  }

  /**
   * Builder of OmMultipartKeyInfoLight.
   */
  public static class Builder {
    private String uploadID;
    private long creationTime;
    private ReplicationConfig replicationConfig;
    private Map<Integer, PartKeyInfoLight> partKeyInfoMap;
    private long objectID;
    private long updateID;
    private long parentID;

    public Builder setUploadID(String uploadID) {
      this.uploadID = uploadID;
      return this;
    }

    public Builder setCreationTime(long creationTime) {
      this.creationTime = creationTime;
      return this;
    }

    public Builder setReplicationConfig(ReplicationConfig replicationConfig) {
      this.replicationConfig = replicationConfig;
      return this;
    }

    public Builder setPartKeyInfoMap(Map<Integer, PartKeyInfoLight> partKeyInfoMap) {
      this.partKeyInfoMap = partKeyInfoMap;
      return this;
    }

    public Builder setObjectID(long objectID) {
      this.objectID = objectID;
      return this;
    }

    public Builder setUpdateID(long updateID) {
      this.updateID = updateID;
      return this;
    }

    public Builder setParentID(long parentID) {
      this.parentID = parentID;
      return this;
    }

    public OmMultipartKeyInfoLight build() {
      return new OmMultipartKeyInfoLight(this);
    }
  }
}

