/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class represents multipart upload information for a key, which holds
 * upload part information of the key.
 */
public class OmMultipartKeyInfo extends WithObjectID {
  private final String uploadID;
  private final long creationTime;
  private final ReplicationType replicationType;
  private final ReplicationFactor replicationFactor;
  private TreeMap<Integer, PartKeyInfo> partKeyInfoList;

  /**
   * Construct OmMultipartKeyInfo object which holds multipart upload
   * information for a key.
   */
  public OmMultipartKeyInfo(String id, long creationTime,
      ReplicationType replicationType, ReplicationFactor replicationFactor,
      Map<Integer, PartKeyInfo> list, long objectID, long updateID) {
    this.uploadID = id;
    this.creationTime = creationTime;
    this.replicationType = replicationType;
    this.replicationFactor = replicationFactor;
    this.partKeyInfoList = new TreeMap<>(list);
    this.objectID = objectID;
    this.updateID = updateID;
  }

  /**
   * Returns the uploadID for this multi part upload of a key.
   * @return uploadID
   */
  public String getUploadID() {
    return uploadID;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public TreeMap<Integer, PartKeyInfo> getPartKeyInfoMap() {
    return partKeyInfoList;
  }

  public void addPartKeyInfo(int partNumber, PartKeyInfo partKeyInfo) {
    this.partKeyInfoList.put(partNumber, partKeyInfo);
  }

  public PartKeyInfo getPartKeyInfo(int partNumber) {
    return partKeyInfoList.get(partNumber);
  }

  public ReplicationType getReplicationType() {
    return replicationType;
  }

  public ReplicationFactor getReplicationFactor() {
    return replicationFactor;
  }

  /**
   * Builder of OmMultipartKeyInfo.
   */
  public static class Builder {
    private String uploadID;
    private long creationTime;
    private ReplicationType replicationType;
    private ReplicationFactor replicationFactor;
    private TreeMap<Integer, PartKeyInfo> partKeyInfoList;
    private long objectID;
    private long updateID;

    public Builder() {
      this.partKeyInfoList = new TreeMap<>();
    }

    public Builder setUploadID(String uploadId) {
      this.uploadID = uploadId;
      return this;
    }

    public Builder setCreationTime(long crTime) {
      this.creationTime = crTime;
      return this;
    }

    public Builder setReplicationType(ReplicationType replType) {
      this.replicationType = replType;
      return this;
    }

    public Builder setReplicationFactor(ReplicationFactor replFactor) {
      this.replicationFactor = replFactor;
      return this;
    }

    public Builder setPartKeyInfoList(Map<Integer, PartKeyInfo> partKeyInfos) {
      if (partKeyInfos != null) {
        this.partKeyInfoList.putAll(partKeyInfos);
      }
      return this;
    }

    public Builder addPartKeyInfoList(int partNum, PartKeyInfo partKeyInfo) {
      if (partKeyInfo != null) {
        partKeyInfoList.put(partNum, partKeyInfo);
      }
      return this;
    }

    public Builder setObjectID(long obId) {
      this.objectID = obId;
      return this;
    }

    public Builder setUpdateID(long id) {
      this.updateID = id;
      return this;
    }

    public OmMultipartKeyInfo build() {
      return new OmMultipartKeyInfo(uploadID, creationTime, replicationType,
          replicationFactor, partKeyInfoList, objectID, updateID);
    }
  }

  /**
   * Construct OmMultipartInfo from MultipartKeyInfo proto object.
   * @param multipartKeyInfo
   * @return OmMultipartKeyInfo
   */
  public static OmMultipartKeyInfo getFromProto(
      MultipartKeyInfo multipartKeyInfo) {
    Map<Integer, PartKeyInfo> list = new HashMap<>();
    multipartKeyInfo.getPartKeyInfoListList().forEach(partKeyInfo ->
        list.put(partKeyInfo.getPartNumber(), partKeyInfo));
    return new OmMultipartKeyInfo(multipartKeyInfo.getUploadID(),
        multipartKeyInfo.getCreationTime(), multipartKeyInfo.getType(),
        multipartKeyInfo.getFactor(), list, multipartKeyInfo.getObjectID(),
        multipartKeyInfo.getUpdateID());
  }

  /**
   * Construct MultipartKeyInfo from this object.
   * @return MultipartKeyInfo
   */
  public MultipartKeyInfo getProto() {
    MultipartKeyInfo.Builder builder = MultipartKeyInfo.newBuilder()
        .setUploadID(uploadID)
        .setCreationTime(creationTime)
        .setType(replicationType)
        .setFactor(replicationFactor)
        .setObjectID(objectID)
        .setUpdateID(updateID);
    partKeyInfoList.forEach((key, value) -> builder.addPartKeyInfoList(value));
    return builder.build();
  }

  @Override
  public String getObjectInfo() {
    return getProto().toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    return other instanceof OmMultipartKeyInfo && uploadID.equals(
        ((OmMultipartKeyInfo)other).getUploadID());
  }

  @Override
  public int hashCode() {
    return uploadID.hashCode();
  }

  public OmMultipartKeyInfo copyObject() {
    // For partKeyInfoList we can do shallow copy here, as the PartKeyInfo is
    // immutable here.
    return new OmMultipartKeyInfo(uploadID, creationTime, replicationType,
        replicationFactor, new TreeMap<>(partKeyInfoList), objectID, updateID);
  }

}
