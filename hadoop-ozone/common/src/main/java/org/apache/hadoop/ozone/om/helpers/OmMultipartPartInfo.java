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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocationList;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartPartInfo;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a part of a multipart upload key.
 */
public final class OmMultipartPartInfo {
  private static final Logger LOG =
      LoggerFactory.getLogger(OmMultipartPartInfo.class);
  private static final Codec<OmMultipartPartInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(MultipartPartInfo.getDefaultInstance()),
      OmMultipartPartInfo::getFromProto,
      OmMultipartPartInfo::getProto,
      OmMultipartPartInfo.class);

  private final String partName;
  private final int partNumber;
  private final long dataSize;
  private final long modificationTime;
  private final long objectID;
  private final long updateID;
  private final List<OmKeyLocationInfoGroup> keyLocationInfos;
  private final String eTag;
  private final FileEncryptionInfo encInfo;
  private final FileChecksum fileChecksum;

  public static Codec<OmMultipartPartInfo> getCodec() {
    return CODEC;
  }

  private OmMultipartPartInfo(Builder b) {
    if (StringUtils.isBlank(b.partName)) {
      throw new IllegalArgumentException("partName is required");
    }
    if (b.partNumber <= 0) {
      throw new IllegalArgumentException("partNumber is required and > 0");
    }
    if (StringUtils.isBlank(b.eTag)) {
      throw new IllegalArgumentException("eTag is required");
    }
    if (b.keyLocationInfos == null || b.keyLocationInfos.isEmpty()) {
      throw new IllegalArgumentException("keyLocationList is required");
    }
    this.partName = b.partName;
    this.partNumber = b.partNumber;
    this.dataSize = b.dataSize;
    this.modificationTime = b.modificationTime;
    this.objectID = b.objectID;
    this.updateID = b.updateID;
    this.keyLocationInfos = Collections.unmodifiableList(b.keyLocationInfos);
    this.eTag = b.eTag;
    this.encInfo = b.encInfo;
    this.fileChecksum = b.fileChecksum;
  }

  /**
   * Builder of OmMultipartPartInfo.
   */
  public static class Builder {
    private String partName;
    private int partNumber;
    private long dataSize;
    private long modificationTime;
    private long objectID;
    private long updateID;
    private List<OmKeyLocationInfoGroup> keyLocationInfos;
    private String eTag;
    private FileEncryptionInfo encInfo;
    private FileChecksum fileChecksum;

    protected Builder() {
      this.keyLocationInfos = new ArrayList<>();
    }

    public Builder(OmMultipartPartInfo obj) {
      this.partName = obj.partName;
      this.partNumber = obj.partNumber;
      this.dataSize = obj.dataSize;
      this.modificationTime = obj.modificationTime;
      this.objectID = obj.objectID;
      this.updateID = obj.updateID;
      this.keyLocationInfos = new ArrayList<>(obj.keyLocationInfos);
      this.eTag = obj.eTag;
      this.encInfo = obj.encInfo;
      this.fileChecksum = obj.fileChecksum;
    }

    public Builder setPartName(String partName) {
      this.partName = partName;
      return this;
    }

    public Builder setPartNumber(int partNumber) {
      this.partNumber = partNumber;
      return this;
    }

    public Builder setDataSize(long dataSize) {
      this.dataSize = dataSize;
      return this;
    }

    public Builder setModificationTime(long modificationTime) {
      this.modificationTime = modificationTime;
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

    public Builder setETag(String eTagValue) {
      if (StringUtils.isBlank(eTagValue)) {
        throw new IllegalArgumentException("eTag is required");
      }
      this.eTag = eTagValue;
      return this;
    }

    public Builder setKeyLocationInfos(
        List<OmKeyLocationInfoGroup> keyLocationInfos) {
      this.keyLocationInfos = new ArrayList<>(keyLocationInfos);
      return this;
    }

    public Builder setEncInfo(FileEncryptionInfo encInfo) {
      this.encInfo = encInfo;
      return this;
    }

    public Builder setFileChecksum(FileChecksum fileChecksum) {
      this.fileChecksum = fileChecksum;
      return this;
    }

    public OmMultipartPartInfo build() {
      return new OmMultipartPartInfo(this);
    }
  }

  public static OmMultipartPartInfo getFromProto(
      MultipartPartInfo multipartPartInfo) {
    validateRequiredProtoFields(multipartPartInfo);
    Builder builder = new Builder()
        .setPartName(multipartPartInfo.getPartName())
        .setPartNumber(multipartPartInfo.getPartNumber())
        .setDataSize(multipartPartInfo.getDataSize())
        .setModificationTime(multipartPartInfo.getModificationTime())
        .setETag(multipartPartInfo.getETag())
        .setKeyLocationInfos(getKeyLocationInfosFromProto(multipartPartInfo))
        .setEncInfo(null);

    if (!multipartPartInfo.hasObjectID()) {
      LOG.warn("MultipartPartInfo missing objectID for part {}",
          multipartPartInfo.getPartNumber());
    }
    builder.setObjectID(multipartPartInfo.getObjectID());

    if (!multipartPartInfo.hasUpdateID()) {
      LOG.warn("MultipartPartInfo missing updateID for part {}",
          multipartPartInfo.getPartNumber());
    }
    builder.setUpdateID(multipartPartInfo.getUpdateID());

    if (multipartPartInfo.hasFileEncryptionInfo()) {
      builder.setEncInfo(
          OMPBHelper.convert(multipartPartInfo.getFileEncryptionInfo()));
    }

    if (multipartPartInfo.hasFileChecksum()) {
      builder.setFileChecksum(
          OMPBHelper.convert(multipartPartInfo.getFileChecksum()));
    }

    return builder.build();
  }

  public MultipartPartInfo getProto() {
    if (StringUtils.isBlank(partName)) {
      throw new IllegalArgumentException("partName is required");
    }
    if (partNumber <= 0) {
      throw new IllegalArgumentException("partNumber is required and > 0");
    }
    if (dataSize < 0) {
      throw new IllegalArgumentException("dataSize is required");
    }
    if (modificationTime <= 0) {
      throw new IllegalArgumentException("modificationTime is required");
    }
    if (keyLocationInfos == null || keyLocationInfos.isEmpty()) {
      throw new IllegalArgumentException("keyLocationList is required");
    }
    MultipartPartInfo.Builder builder = MultipartPartInfo.newBuilder()
        .setPartName(partName)
        .setPartNumber(partNumber)
        .setKeyLocationList(getKeyLocationInfosAsProto())
        .setDataSize(dataSize)
        .setModificationTime(modificationTime)
        .setObjectID(objectID)
        .setUpdateID(updateID)
        .setETag(Objects.requireNonNull(eTag, "eTag is required"));

    if (encInfo != null) {
      builder.setFileEncryptionInfo(OMPBHelper.convert(encInfo));
    }
    if (fileChecksum != null) {
      builder.setFileChecksum(OMPBHelper.convert(fileChecksum));
    }
    return builder.build();
  }

  public String getPartName() {
    return partName;
  }

  public int getPartNumber() {
    return partNumber;
  }

  public long getDataSize() {
    return dataSize;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public long getObjectID() {
    return objectID;
  }

  public long getUpdateID() {
    return updateID;
  }

  public List<OmKeyLocationInfoGroup> getKeyLocationInfos() {
    return keyLocationInfos;
  }

  public String getETag() {
    return eTag;
  }

  public FileEncryptionInfo getEncInfo() {
    return encInfo;
  }

  public FileChecksum getFileChecksum() {
    return fileChecksum;
  }

  public static OmMultipartPartInfo from(
      String partName, int partNumber, OmKeyInfo omKeyInfo) {
    if (omKeyInfo.getObjectID() == 0L) {
      LOG.warn("Multipart part {} has unset objectID", partNumber);
    }
    if (omKeyInfo.getUpdateID() == 0L) {
      LOG.warn("Multipart part {} has unset updateID", partNumber);
    }
    Builder builder = new Builder()
        .setPartName(partName)
        .setPartNumber(partNumber)
        .setDataSize(omKeyInfo.getDataSize())
        .setModificationTime(omKeyInfo.getModificationTime())
        .setObjectID(omKeyInfo.getObjectID())
        .setUpdateID(omKeyInfo.getUpdateID())
        .setKeyLocationInfos(omKeyInfo.getKeyLocationVersions())
        .setEncInfo(omKeyInfo.getFileEncryptionInfo())
        .setFileChecksum(omKeyInfo.getFileChecksum())
        .setETag(omKeyInfo.getMetadata().get(OzoneConsts.ETAG));
    return builder.build();
  }

  private KeyLocationList getKeyLocationInfosAsProto() {
    if (keyLocationInfos == null || keyLocationInfos.isEmpty()) {
      throw new IllegalArgumentException("keyLocationList is required");
    }
    return keyLocationInfos.get(0).getProtobuf(true, ClientVersion.CURRENT_VERSION);
  }

  private static List<OmKeyLocationInfoGroup> getKeyLocationInfosFromProto(
      MultipartPartInfo multipartPartInfo) {
    return Collections.singletonList(
        OmKeyLocationInfoGroup.getFromProtobuf(
            multipartPartInfo.getKeyLocationList()));
  }

  private static void validateRequiredProtoFields(MultipartPartInfo partInfo) {
    if (!partInfo.hasPartName() || StringUtils.isBlank(partInfo.getPartName())) {
      throw new IllegalArgumentException("MultipartPartInfo missing partName");
    }
    if (!partInfo.hasPartNumber()) {
      throw new IllegalArgumentException("MultipartPartInfo missing partNumber");
    }
    if (!partInfo.hasETag() || StringUtils.isBlank(partInfo.getETag())) {
      throw new IllegalArgumentException("MultipartPartInfo missing eTag");
    }
    if (!partInfo.hasKeyLocationList()) {
      throw new IllegalArgumentException("MultipartPartInfo missing keyLocationList");
    }
    if (!partInfo.hasDataSize()) {
      throw new IllegalArgumentException("MultipartPartInfo missing dataSize");
    }
    if (!partInfo.hasModificationTime()) {
      throw new IllegalArgumentException("MultipartPartInfo missing modificationTime");
    }
  }
}
