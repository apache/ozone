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
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Args for key. Client use this to specify key's attributes on  key creation
 * (putKey()).
 */
public final class OmKeyArgs implements Auditable {
  private final String volumeName;
  private final String bucketName;
  private final String keyName;
  private long dataSize;
  private final ReplicationConfig replicationConfig;
  private List<OmKeyLocationInfo> locationInfoList;
  private final boolean isMultipartKey;
  private final String multipartUploadID;
  private final int multipartUploadPartNumber;
  private Map<String, String> metadata;
  private boolean sortDatanodesInPipeline;
  private List<OzoneAcl> acls;
  private boolean latestVersionLocation;
  private boolean recursive;
  private boolean headOp;
  private boolean forceUpdateContainerCacheFromSCM;

  @SuppressWarnings("parameternumber")
  private OmKeyArgs(String volumeName, String bucketName, String keyName,
      long dataSize, ReplicationConfig replicationConfig,
      List<OmKeyLocationInfo> locationInfoList, boolean isMultipart,
      String uploadID, int partNumber,
      Map<String, String> metadataMap,
      List<OzoneAcl> acls, boolean sortDatanode,
      boolean latestVersionLocation, boolean recursive, boolean headOp,
      boolean forceUpdateContainerCacheFromSCM) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.dataSize = dataSize;
    this.replicationConfig = replicationConfig;
    this.locationInfoList = locationInfoList;
    this.isMultipartKey = isMultipart;
    this.multipartUploadID = uploadID;
    this.multipartUploadPartNumber = partNumber;
    this.metadata = metadataMap;
    this.acls = acls;
    this.sortDatanodesInPipeline = sortDatanode;
    this.latestVersionLocation = latestVersionLocation;
    this.recursive = recursive;
    this.headOp = headOp;
    this.forceUpdateContainerCacheFromSCM = forceUpdateContainerCacheFromSCM;
  }

  public boolean getIsMultipartKey() {
    return isMultipartKey;
  }

  public String getMultipartUploadID() {
    return multipartUploadID;
  }

  public int getMultipartUploadPartNumber() {
    return multipartUploadPartNumber;
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  public List<OzoneAcl> getAcls() {
    return acls;
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

  public void setDataSize(long size) {
    dataSize = size;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public void setLocationInfoList(List<OmKeyLocationInfo> locationInfoList) {
    this.locationInfoList = locationInfoList;
  }

  public List<OmKeyLocationInfo> getLocationInfoList() {
    return locationInfoList;
  }

  public boolean getSortDatanodes() {
    return sortDatanodesInPipeline;
  }

  public boolean getLatestVersionLocation() {
    return latestVersionLocation;
  }

  public boolean isRecursive() {
    return recursive;
  }

  public boolean isHeadOp() {
    return headOp;
  }

  public boolean isForceUpdateContainerCacheFromSCM() {
    return forceUpdateContainerCacheFromSCM;
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, this.volumeName);
    auditMap.put(OzoneConsts.BUCKET, this.bucketName);
    auditMap.put(OzoneConsts.KEY, this.keyName);
    auditMap.put(OzoneConsts.DATA_SIZE, String.valueOf(this.dataSize));
    auditMap.put(OzoneConsts.REPLICATION_CONFIG,
        (this.replicationConfig != null) ?
            this.replicationConfig.toString() : null);
    return auditMap;
  }

  @VisibleForTesting
  public void addLocationInfo(OmKeyLocationInfo locationInfo) {
    if (this.locationInfoList == null) {
      locationInfoList = new ArrayList<>();
    }
    locationInfoList.add(locationInfo);
  }

  public OmKeyArgs.Builder toBuilder() {
    return new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(dataSize)
        .setReplicationConfig(replicationConfig)
        .setLocationInfoList(locationInfoList)
        .setIsMultipartKey(isMultipartKey)
        .setMultipartUploadID(multipartUploadID)
        .setMultipartUploadPartNumber(multipartUploadPartNumber)
        .addAllMetadata(metadata)
        .setSortDatanodesInPipeline(sortDatanodesInPipeline)
        .setHeadOp(headOp)
        .setLatestVersionLocation(latestVersionLocation)
        .setAcls(acls)
        .setForceUpdateContainerCacheFromSCM(forceUpdateContainerCacheFromSCM);
  }

  @NotNull
  public KeyArgs toProtobuf() {
    return KeyArgs.newBuilder()
        .setVolumeName(getVolumeName())
        .setBucketName(getBucketName())
        .setKeyName(getKeyName())
        .setDataSize(getDataSize())
        .setSortDatanodes(getSortDatanodes())
        .setLatestVersionLocation(getLatestVersionLocation())
        .setHeadOp(isHeadOp())
        .setForceUpdateContainerCacheFromSCM(
            isForceUpdateContainerCacheFromSCM())
        .build();
  }

  /**
   * Builder class of OmKeyArgs.
   */
  public static class Builder {
    private String volumeName;
    private String bucketName;
    private String keyName;
    private long dataSize;
    private ReplicationConfig replicationConfig;
    private List<OmKeyLocationInfo> locationInfoList;
    private boolean isMultipartKey;
    private String multipartUploadID;
    private int multipartUploadPartNumber;
    private Map<String, String> metadata = new HashMap<>();
    private boolean sortDatanodesInPipeline;
    private boolean latestVersionLocation;
    private List<OzoneAcl> acls;
    private boolean recursive;
    private boolean headOp;
    private boolean forceUpdateContainerCacheFromSCM;

    public Builder setVolumeName(String volume) {
      this.volumeName = volume;
      return this;
    }

    public Builder setBucketName(String bucket) {
      this.bucketName = bucket;
      return this;
    }

    public Builder setKeyName(String key) {
      this.keyName = key;
      return this;
    }

    public Builder setDataSize(long size) {
      this.dataSize = size;
      return this;
    }

    public Builder setReplicationConfig(ReplicationConfig replConfig) {
      this.replicationConfig = replConfig;
      return this;
    }

    public Builder setLocationInfoList(List<OmKeyLocationInfo> locationInfos) {
      this.locationInfoList = locationInfos;
      return this;
    }

    public Builder setAcls(List<OzoneAcl> listOfAcls) {
      this.acls = listOfAcls;
      return this;
    }

    public Builder setIsMultipartKey(boolean isMultipart) {
      this.isMultipartKey = isMultipart;
      return this;
    }

    public Builder setMultipartUploadID(String uploadID) {
      this.multipartUploadID = uploadID;
      return this;
    }

    public Builder setMultipartUploadPartNumber(int partNumber) {
      this.multipartUploadPartNumber = partNumber;
      return this;
    }

    public Builder addMetadata(String key, String value) {
      this.metadata.put(key, value);
      return this;
    }

    public Builder addAllMetadata(Map<String, String> metadatamap) {
      this.metadata.putAll(metadatamap);
      return this;
    }

    public Builder setSortDatanodesInPipeline(boolean sort) {
      this.sortDatanodesInPipeline = sort;
      return this;
    }

    public Builder setLatestVersionLocation(boolean latest) {
      this.latestVersionLocation = latest;
      return this;
    }

    public Builder setRecursive(boolean isRecursive) {
      this.recursive = isRecursive;
      return this;
    }

    public Builder setHeadOp(boolean isHeadOp) {
      this.headOp = isHeadOp;
      return this;
    }

    public Builder setForceUpdateContainerCacheFromSCM(boolean value) {
      this.forceUpdateContainerCacheFromSCM = value;
      return this;
    }

    public OmKeyArgs build() {
      return new OmKeyArgs(volumeName, bucketName, keyName, dataSize,
          replicationConfig, locationInfoList, isMultipartKey,
          multipartUploadID,
          multipartUploadPartNumber, metadata, acls,
          sortDatanodesInPipeline, latestVersionLocation, recursive, headOp,
          forceUpdateContainerCacheFromSCM);
    }

  }
}
