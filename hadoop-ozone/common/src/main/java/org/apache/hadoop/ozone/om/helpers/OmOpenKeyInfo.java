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

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper for OmKeyInfo with additional fields that are only meaningful
 * for open keys stored in openKeyTable/openFileTable. These fields are used
 * during key creation and commit, but should not be persisted in keyTable.
 *
 * <p>This class uses composition to wrap {@link OmKeyInfo} and adds:
 * <ul>
 *   <li>expectedDataGeneration - for conditional writes based on key generation</li>
 *   <li>expectedETag - for conditional writes based on S3 ETag (If-Match header)</li>
 * </ul>
 */
public final class OmOpenKeyInfo implements CopyObject<OmOpenKeyInfo> {
  private static final Logger LOG = LoggerFactory.getLogger(OmOpenKeyInfo.class);

  private static final Codec<OmOpenKeyInfo> CODEC_TRUE = newCodec(true);
  private static final Codec<OmOpenKeyInfo> CODEC_FALSE = newCodec(false);

  private final OmKeyInfo keyInfo;
  private final Long expectedDataGeneration;
  private final String expectedETag;

  private OmOpenKeyInfo(Builder builder) {
    this.keyInfo = Objects.requireNonNull(builder.keyInfo, "keyInfo cannot be null");
    this.expectedDataGeneration = builder.expectedDataGeneration;
    this.expectedETag = builder.expectedETag;
  }

  /**
   * Creates a backward-compatible codec that can read both:
   * - Old format: raw KeyInfo proto bytes (for existing data migration)
   * - New format: OpenKeyInfo proto bytes
   */
  private static Codec<OmOpenKeyInfo> newCodec(boolean ignorePipeline) {
    return new BackwardCompatibleCodec(ignorePipeline);
  }

  public static Codec<OmOpenKeyInfo> getCodec(boolean ignorePipeline) {
    LOG.debug("OmOpenKeyInfo.getCodec ignorePipeline = {}", ignorePipeline);
    return ignorePipeline ? CODEC_TRUE : CODEC_FALSE;
  }

  /**
   * Returns the wrapped OmKeyInfo.
   * Use this when you need to extract the key info for commit operations
   * or when interacting with keyTable.
   */
  public OmKeyInfo getKeyInfo() {
    return keyInfo;
  }

  public Long getExpectedDataGeneration() {
    return expectedDataGeneration;
  }

  public String getExpectedETag() {
    return expectedETag;
  }

  public String getVolumeName() {
    return keyInfo.getVolumeName();
  }

  public String getBucketName() {
    return keyInfo.getBucketName();
  }

  public String getKeyName() {
    return keyInfo.getKeyName();
  }

  public long getDataSize() {
    return keyInfo.getDataSize();
  }

  public void setDataSize(long size) {
    keyInfo.setDataSize(size);
  }

  public long getCreationTime() {
    return keyInfo.getCreationTime();
  }

  public long getModificationTime() {
    return keyInfo.getModificationTime();
  }

  public void setModificationTime(long modificationTime) {
    keyInfo.setModificationTime(modificationTime);
  }

  public ReplicationConfig getReplicationConfig() {
    return keyInfo.getReplicationConfig();
  }

  public void setReplicationConfig(ReplicationConfig repConfig) {
    keyInfo.setReplicationConfig(repConfig);
  }

  public List<OmKeyLocationInfoGroup> getKeyLocationVersions() {
    return keyInfo.getKeyLocationVersions();
  }

  public void setKeyLocationVersions(List<OmKeyLocationInfoGroup> keyLocationVersions) {
    keyInfo.setKeyLocationVersions(keyLocationVersions);
  }

  public OmKeyLocationInfoGroup getLatestVersionLocations() {
    return keyInfo.getLatestVersionLocations();
  }

  public FileEncryptionInfo getFileEncryptionInfo() {
    return keyInfo.getFileEncryptionInfo();
  }

  public void setFileEncryptionInfo(FileEncryptionInfo encInfo) {
    keyInfo.setFileEncryptionInfo(encInfo);
  }

  public FileChecksum getFileChecksum() {
    return keyInfo.getFileChecksum();
  }

  public List<OzoneAcl> getAcls() {
    return keyInfo.getAcls();
  }

  public Map<String, String> getMetadata() {
    return keyInfo.getMetadata();
  }

  public Map<String, String> getTags() {
    return keyInfo.getTags();
  }

  public long getObjectID() {
    return keyInfo.getObjectID();
  }

  public long getUpdateID() {
    return keyInfo.getUpdateID();
  }

  public long getParentObjectID() {
    return keyInfo.getParentObjectID();
  }

  public String getFileName() {
    return keyInfo.getFileName();
  }

  public boolean isFile() {
    return keyInfo.isFile();
  }

  public boolean isHsync() {
    return keyInfo.isHsync();
  }

  public String getOwnerName() {
    return keyInfo.getOwnerName();
  }

  public long getGeneration() {
    return keyInfo.getGeneration();
  }

  public long getReplicatedSize() {
    return keyInfo.getReplicatedSize();
  }

  public String getPath() {
    return keyInfo.getPath();
  }

  public boolean hasBlocks() {
    return keyInfo.hasBlocks();
  }

  /**
   * Append new blocks to the latest version.
   */
  public void appendNewBlocks(List<OmKeyLocationInfo> newLocationList, boolean updateTime) {
    keyInfo.appendNewBlocks(newLocationList, updateTime);
  }

  /**
   * Add a new version with the given blocks.
   */
  public long addNewVersion(List<OmKeyLocationInfo> newLocationList,
      boolean updateTime, boolean keepOldVersions) {
    return keyInfo.addNewVersion(newLocationList, updateTime, keepOldVersions);
  }

  /**
   * Updates the location info list with committed blocks.
   */
  public List<OmKeyLocationInfo> updateLocationInfoList(
      List<OmKeyLocationInfo> locationInfoList, boolean isMpu) {
    return keyInfo.updateLocationInfoList(locationInfoList, isMpu);
  }

  public List<OmKeyLocationInfo> updateLocationInfoList(
      List<OmKeyLocationInfo> locationInfoList, boolean isMpu, boolean skipBlockIDCheck) {
    return keyInfo.updateLocationInfoList(locationInfoList, isMpu, skipBlockIDCheck);
  }

  // ========== Proto conversion ==========

  public OpenKeyInfo getProtobuf(boolean ignorePipeline, int clientVersion) {
    OpenKeyInfo.Builder builder = OpenKeyInfo.newBuilder()
        .setKeyInfo(keyInfo.getProtobuf(ignorePipeline, clientVersion));

    if (expectedDataGeneration != null) {
      builder.setExpectedDataGeneration(expectedDataGeneration);
    }
    if (expectedETag != null) {
      builder.setExpectedETag(expectedETag);
    }
    return builder.build();
  }

  public static OmOpenKeyInfo getFromProtobuf(OpenKeyInfo proto) {
    if (proto == null) {
      return null;
    }
    return new Builder()
        .setKeyInfo(OmKeyInfo.getFromProtobuf(proto.getKeyInfo()))
        .setExpectedDataGeneration(
            proto.hasExpectedDataGeneration() ? proto.getExpectedDataGeneration() : null)
        .setExpectedETag(
            proto.hasExpectedETag() ? proto.getExpectedETag() : null)
        .build();
  }

  /**
   * Creates an OmOpenKeyInfo from a legacy KeyInfo proto.
   * This is used for backward compatibility when reading old data.
   */
  public static OmOpenKeyInfo fromLegacyKeyInfo(KeyInfo keyInfo) {
    if (keyInfo == null) {
      return null;
    }
    OmKeyInfo omKeyInfo = OmKeyInfo.getFromProtobuf(keyInfo);
    return new Builder()
        .setKeyInfo(omKeyInfo)
        // Legacy KeyInfo may have these fields - extract them if present
        .setExpectedDataGeneration(
            keyInfo.hasExpectedDataGeneration() ? keyInfo.getExpectedDataGeneration() : null)
        .setExpectedETag(
            keyInfo.hasExpectedETag() ? keyInfo.getExpectedETag() : null)
        .build();
  }

  @Override
  public OmOpenKeyInfo copyObject() {
    return new Builder()
        .setKeyInfo(keyInfo.copyObject())
        .setExpectedDataGeneration(expectedDataGeneration)
        .setExpectedETag(expectedETag)
        .build();
  }

  /**
   * Creates a new Builder initialized with values from this object.
   */
  public Builder toBuilder() {
    return new Builder()
        .setKeyInfo(keyInfo.copyObject())
        .setExpectedDataGeneration(expectedDataGeneration)
        .setExpectedETag(expectedETag);
  }

  @Override
  public String toString() {
    return "OmOpenKeyInfo{" +
        "keyInfo=" + keyInfo +
        ", expectedDataGeneration=" + expectedDataGeneration +
        ", expectedETag='" + expectedETag + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OmOpenKeyInfo that = (OmOpenKeyInfo) o;
    return Objects.equals(keyInfo, that.keyInfo) &&
        Objects.equals(expectedDataGeneration, that.expectedDataGeneration) &&
        Objects.equals(expectedETag, that.expectedETag);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyInfo, expectedDataGeneration, expectedETag);
  }

  /**
   * Builder for OmOpenKeyInfo.
   */
  public static class Builder {
    private OmKeyInfo keyInfo;
    private Long expectedDataGeneration;
    private String expectedETag;

    public Builder() {
    }

    public Builder setKeyInfo(OmKeyInfo keyInfo) {
      this.keyInfo = keyInfo;
      return this;
    }

    public Builder setExpectedDataGeneration(Long expectedDataGeneration) {
      this.expectedDataGeneration = expectedDataGeneration;
      return this;
    }

    public Builder setExpectedETag(String expectedETag) {
      this.expectedETag = expectedETag;
      return this;
    }

    public OmOpenKeyInfo build() {
      return new OmOpenKeyInfo(this);
    }
  }

  /**
   * Backward-compatible codec that can read both old KeyInfo format
   * and new OpenKeyInfo format.
   */
  private static class BackwardCompatibleCodec implements Codec<OmOpenKeyInfo> {
    private final boolean ignorePipeline;

    BackwardCompatibleCodec(boolean ignorePipeline) {
      this.ignorePipeline = ignorePipeline;
    }

    @Override
    public byte[] toPersistedFormatImpl(OmOpenKeyInfo object) {
      return object.getProtobuf(ignorePipeline, ClientVersion.CURRENT_VERSION).toByteArray();
    }

    @Override
    public OmOpenKeyInfo fromPersistedFormatImpl(byte[] rawData) throws InvalidProtocolBufferException {
      // Try to parse as OpenKeyInfo first
      try {
        OpenKeyInfo openKeyInfo = OpenKeyInfo.parseFrom(rawData);
        // Check if this is actually an OpenKeyInfo (has nested KeyInfo with valid fields)
        if (openKeyInfo.hasKeyInfo() && openKeyInfo.getKeyInfo().hasVolumeName()) {
          return getFromProtobuf(openKeyInfo);
        }
      } catch (InvalidProtocolBufferException e) {
        // Fall through to try KeyInfo parsing
        LOG.debug("Failed to parse as OpenKeyInfo, trying KeyInfo format", e);
      }

      // Try to parse as legacy KeyInfo format
      KeyInfo keyInfo = KeyInfo.parseFrom(rawData);
      if (keyInfo.hasVolumeName()) {
        return fromLegacyKeyInfo(keyInfo);
      }

      throw new InvalidProtocolBufferException(
          "Invalid data format: could not parse as OpenKeyInfo or KeyInfo");
    }

    @Override
    public OmOpenKeyInfo copyObject(OmOpenKeyInfo object) {
      return object.copyObject();
    }

    @Override
    public Class<OmOpenKeyInfo> getTypeClass() {
      return OmOpenKeyInfo.class;
    }
  }
}
