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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.FileChecksumProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocationList;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Args for key block. The block instance for the key requested in putKey.
 * This is returned from OM to client, and client use class to talk to
 * datanode. Also, this is the metadata written to om.db on server side.
 */
public final class OmKeyInfo extends WithParentObjectId
    implements CopyObject<OmKeyInfo>, WithTags {
  private static final Logger LOG = LoggerFactory.getLogger(OmKeyInfo.class);

  private static final Codec<OmKeyInfo> CODEC_TRUE = newCodec(true);
  private static final Codec<OmKeyInfo> CODEC_FALSE = newCodec(false);
  /**
   * Metadata key flag to indicate whether a deleted key was a committed key.
   * The flag is set when a committed key is deleted from AOS but still held in
   * a snapshot to help with accurate bucket quota usage calculation.
   */
  private static final String COMMITTED_KEY_DELETED_FLAG = "CKDEL";

  private final String volumeName;
  private final String bucketName;
  // name of key client specified
  private String keyName;
  private long dataSize;
  private List<OmKeyLocationInfoGroup> keyLocationVersions;
  private final long creationTime;
  private long modificationTime;
  private ReplicationConfig replicationConfig;
  private FileEncryptionInfo encInfo;
  private final FileChecksum fileChecksum;

  /**
   * Support OFS use-case to identify if the key is a file or a directory.
   */
  private boolean isFile;

  /**
   * Represents leaf node name. This also will be used when the keyName is
   * created on a FileSystemOptimized(FSO) bucket. For example, the user given
   * keyName is "a/b/key1" then the fileName stores "key1".
   */
  private String fileName;
  private final String ownerName;

  /**
   * ACL Information.
   */
  private final ImmutableList<OzoneAcl> acls;

  /**
   * Used for S3 tags.
   */
  private final ImmutableMap<String, String> tags;

  // expectedDataGeneration, when used in key creation indicates that a
  // key with the same keyName should exist with the given generation.
  // For a key commit to succeed, the original key should still be present with the
  // generation unchanged.
  // This allows a key to be created an committed atomically if the original has not
  // been modified.
  private Long expectedDataGeneration = null;
  private String expectedETag;

  private OmKeyInfo(Builder b) {
    super(b);
    this.volumeName = b.volumeName;
    this.bucketName = b.bucketName;
    this.keyName = b.keyName;
    this.dataSize = b.dataSize;
    this.keyLocationVersions = b.omKeyLocationInfoGroups;
    this.creationTime = b.creationTime;
    this.modificationTime = b.modificationTime;
    this.replicationConfig = b.replicationConfig;
    this.encInfo = b.encInfo;
    this.acls = b.acls.build();
    this.fileChecksum = b.fileChecksum;
    this.fileName = b.fileName;
    this.isFile = b.isFile;
    this.ownerName = b.ownerName;
    this.tags = b.tags.build();
    this.expectedDataGeneration = b.expectedDataGeneration;
    this.expectedETag = b.expectedETag;
  }

  private static Codec<OmKeyInfo> newCodec(boolean ignorePipeline) {
    return new DelegatedCodec<>(
        Proto2Codec.get(KeyInfo.getDefaultInstance()),
        OmKeyInfo::getFromProtobuf,
        k -> k.getProtobuf(ignorePipeline, ClientVersion.CURRENT_VERSION),
        OmKeyInfo.class);
  }

  public static Codec<OmKeyInfo> getCodec(boolean ignorePipeline) {
    LOG.debug("OmKeyInfo.getCodec ignorePipeline = {}", ignorePipeline);
    return ignorePipeline ? CODEC_TRUE : CODEC_FALSE;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  public String getKeyName() {
    return keyName;
  }

  public void setKeyName(String keyName) {
    this.keyName = Objects.requireNonNull(keyName, "keyName == null");
    this.fileName = OzoneFSUtils.getFileName(keyName);
  }

  public long getDataSize() {
    return dataSize;
  }

  public long getReplicatedSize() {
    return QuotaUtil.getReplicatedSize(getDataSize(), replicationConfig);
  }

  public void setDataSize(long size) {
    this.dataSize = size;
  }

  public String getFileName() {
    return fileName;
  }

  public void setExpectedDataGeneration(Long generation) {
    this.expectedDataGeneration = generation;
  }

  public Long getExpectedDataGeneration() {
    return expectedDataGeneration;
  }

  public void setExpectedETag(String eTag) {
    this.expectedETag = eTag;
  }

  public String getExpectedETag() {
    return expectedETag;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public OmKeyInfo withCommittedKeyDeletedFlag(boolean val) {
    if (val) {
      return withMetadataMutations(
          metadata -> metadata.put(COMMITTED_KEY_DELETED_FLAG, "true"));
    }
    return withMetadataMutations(
        metadata -> metadata.remove(COMMITTED_KEY_DELETED_FLAG));
  }

  /**
   * Returns a new {@link OmKeyInfo} instance with metadata updated by the
   * provided mutator.
   *
   * @param metadataUpdater a function that applies mutations to a copy of the metadata
   * @return a new {@link OmKeyInfo} instance with updated metadata
   */
  public OmKeyInfo withMetadataMutations(
      Consumer<Map<String, String>> metadataUpdater) {
    Objects.requireNonNull(metadataUpdater, "metadataUpdater == null");
    Map<String, String> metadataCopy = new HashMap<>(getMetadata());
    metadataUpdater.accept(metadataCopy);
    return toBuilder().setMetadata(metadataCopy).build();
  }

  public boolean isDeletedKeyCommitted() {
    return Boolean.parseBoolean(getMetadata().get(COMMITTED_KEY_DELETED_FLAG));
  }

  /**
   * Returns the generation of the object. Note this is currently the same as updateID for a key.
   * @return long
   */
  public long getGeneration() {
    return getUpdateID();
  }

  public synchronized OmKeyLocationInfoGroup getLatestVersionLocations() {
    return keyLocationVersions.isEmpty() ? null :
        keyLocationVersions.get(keyLocationVersions.size() - 1);
  }

  public List<OmKeyLocationInfoGroup> getKeyLocationVersions() {
    return keyLocationVersions;
  }

  public void setKeyLocationVersions(
      List<OmKeyLocationInfoGroup> keyLocationVersions) {
    this.keyLocationVersions = keyLocationVersions;
  }

  public void setFile(boolean file) {
    isFile = file;
  }

  public boolean isFile() {
    return isFile;
  }

  public boolean isHsync() {
    return getMetadata().containsKey(OzoneConsts.HSYNC_CLIENT_ID);
  }

  @Override
  public Map<String, String> getTags() {
    return tags;
  }

  /**
   * updates the length of the each block in the list given.
   * This will be called when the key is being committed to OzoneManager.
   * Return the uncommitted locationInfo to be deleted.
   *
   * @param locationInfoList list of locationInfo
   * @return allocated but uncommitted locationInfos
   */
  public List<OmKeyLocationInfo> updateLocationInfoList(
      List<OmKeyLocationInfo> locationInfoList, boolean isMpu) {
    return updateLocationInfoList(locationInfoList, isMpu, false);
  }

  /**
   * updates the length of the each block in the list given.
   * This will be called when the key is being committed to OzoneManager.
   * Return the uncommitted locationInfo to be deleted.
   *
   * @param locationInfoList list of locationInfo
   * @param isMpu a true represents multi part key, false otherwise
   * @param skipBlockIDCheck a true represents that the blockId verification
   *                         check should be skipped, false represents that
   *                         the blockId verification will be required
   * @return allocated but uncommitted locationInfos
   */
  public List<OmKeyLocationInfo> updateLocationInfoList(
      List<OmKeyLocationInfo> locationInfoList,
      boolean isMpu, boolean skipBlockIDCheck) {
    long latestVersion = getLatestVersionLocations().getVersion();
    OmKeyLocationInfoGroup keyLocationInfoGroup = getLatestVersionLocations();

    keyLocationInfoGroup.setMultipartKey(isMpu);

    // Compare user given block location against allocatedBlockLocations
    // present in OmKeyInfo.
    List<OmKeyLocationInfo> uncommittedBlocks;
    List<OmKeyLocationInfo> updatedBlockLocations;
    if (skipBlockIDCheck) {
      updatedBlockLocations = locationInfoList;
      uncommittedBlocks = new ArrayList<>();
    } else {
      Pair<List<OmKeyLocationInfo>, List<OmKeyLocationInfo>> verifiedResult =
          verifyAndGetKeyLocations(locationInfoList, keyLocationInfoGroup);
      updatedBlockLocations = verifiedResult.getLeft();
      uncommittedBlocks = verifiedResult.getRight();
    }

    keyLocationInfoGroup.removeBlocks(latestVersion);
    // set each of the locationInfo object to the latest version
    updatedBlockLocations.forEach(omKeyLocationInfo -> omKeyLocationInfo
        .setCreateVersion(latestVersion));
    keyLocationInfoGroup.addAll(latestVersion, updatedBlockLocations);

    return uncommittedBlocks;
  }

  /**
   *  1. Verify committed KeyLocationInfos
   *  2. Find out the allocated but uncommitted KeyLocationInfos.
   *
   * @param locationInfoList committed KeyLocationInfos
   * @param keyLocationInfoGroup allocated KeyLocationInfoGroup
   * @return Pair of updatedOmKeyLocationInfo and uncommittedOmKeyLocationInfo
   */
  private Pair<List<OmKeyLocationInfo>, List<OmKeyLocationInfo>>
      verifyAndGetKeyLocations(
          List<OmKeyLocationInfo> locationInfoList,
          OmKeyLocationInfoGroup keyLocationInfoGroup) {
    // Only check ContainerBlockID here to avoid the mismatch of the pipeline
    // field and BcsId in the OmKeyLocationInfo, as the OmKeyInfoCodec ignores
    // the pipeline field by default and bcsId would be updated in Ratis mode.
    Map<ContainerBlockID, OmKeyLocationInfo> allocatedBlockLocations =
        new HashMap<>();
    for (OmKeyLocationInfo existingLocationInfo : keyLocationInfoGroup.
        getLocationList()) {
      ContainerBlockID existingBlockID = existingLocationInfo.getBlockID().
          getContainerBlockID();
      // The case of overwriting value should never happen
      allocatedBlockLocations.put(existingBlockID, existingLocationInfo);
    }

    List<OmKeyLocationInfo> updatedBlockLocations = new ArrayList<>();
    for (OmKeyLocationInfo modifiedLocationInfo : locationInfoList) {
      ContainerBlockID modifiedContainerBlockId =
          modifiedLocationInfo.getBlockID().getContainerBlockID();
      if (allocatedBlockLocations.containsKey(modifiedContainerBlockId)) {
        updatedBlockLocations.add(modifiedLocationInfo);
        allocatedBlockLocations.remove(modifiedContainerBlockId);
      } else {
        LOG.warn("Unknown BlockLocation:{}, where the blockID of given "
            + "location doesn't match with the stored/allocated block of"
            + " keyName:{}", modifiedLocationInfo, keyName);
      }
    }
    List<OmKeyLocationInfo> uncommittedLocationInfos = new ArrayList<>(
        allocatedBlockLocations.values());
    return Pair.of(updatedBlockLocations, uncommittedLocationInfos);
  }

  /**
   * Append a set of blocks to the latest version. Note that these blocks are
   * part of the latest version, not a new version.
   *
   * @param newLocationList the list of new blocks to be added.
   * @param updateTime if true, will update modification time.
   */
  public synchronized void appendNewBlocks(
      List<OmKeyLocationInfo> newLocationList, boolean updateTime) {
    if (keyLocationVersions.isEmpty()) {
      throw new IllegalStateException("Appending new blocks but keyLocationVersions is empty");
    }
    OmKeyLocationInfoGroup currentLatestVersion =
        keyLocationVersions.get(keyLocationVersions.size() - 1);
    currentLatestVersion.appendNewBlocks(newLocationList);
    if (updateTime) {
      setModificationTime(Time.now());
    }
  }

  /**
   * Add a new set of blocks. The new blocks will be added as appending a new
   * version to the all version list.
   *
   * @param newLocationList the list of new blocks to be added.
   * @param updateTime if true, updates modification time.
   * @param keepOldVersions if false, old blocks won't be kept
   *                        and the new block versions will always be 0
   */
  public synchronized long addNewVersion(
      List<OmKeyLocationInfo> newLocationList, boolean updateTime,
      boolean keepOldVersions) {
    long latestVersionNum;

    if (!keepOldVersions) {
      // If old versions are cleared, new block version will always start at 0
      keyLocationVersions.clear();
    }

    if (keyLocationVersions.isEmpty()) {
      // no version exist, these blocks are the very first version.
      keyLocationVersions.add(new OmKeyLocationInfoGroup(0, newLocationList));
      latestVersionNum = 0;
    } else {
      // it is important that the new version are always at the tail of the list
      OmKeyLocationInfoGroup currentLatestVersion =
          keyLocationVersions.get(keyLocationVersions.size() - 1);

      // Create a new version here. When bucket versioning is enabled,
      // It includes previous block versions. Otherwise, only the blocks
      // of new version is included.
      OmKeyLocationInfoGroup newVersion =
          currentLatestVersion.generateNextVersion(newLocationList);
      keyLocationVersions.add(newVersion);
      latestVersionNum = newVersion.getVersion();
    }

    if (updateTime) {
      setModificationTime(Time.now());
    }
    return latestVersionNum;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }

  public FileEncryptionInfo getFileEncryptionInfo() {
    return encInfo;
  }

  public List<OzoneAcl> getAcls() {
    return acls;
  }

  public void setReplicationConfig(ReplicationConfig repConfig) {
    this.replicationConfig = repConfig;
  }

  public FileChecksum getFileChecksum() {
    return fileChecksum;
  }

  @Override
  public String toString() {
    return "OmKeyInfo{" +
        "volumeName='" + volumeName + '\'' +
        ", bucketName='" + bucketName + '\'' +
        ", keyName='" + keyName + '\'' +
        ", dataSize=" + dataSize +
        ", keyLocationVersions=" + keyLocationVersions +
        ", creationTime=" + creationTime +
        ", modificationTime=" + modificationTime +
        ", replicationConfig=" + replicationConfig +
        ", encInfo=" + (encInfo == null ? "null" : "<REDACTED>") +
        ", fileChecksum=" + fileChecksum +
        ", isFile=" + isFile +
        ", fileName='" + fileName + '\'' +
        ", acls=" + acls +
        '}';
  }

  /**
   * Builder of OmKeyInfo.
   */
  public static class Builder extends WithParentObjectId.Builder<OmKeyInfo> {
    private String volumeName;
    private String bucketName;
    private String keyName;
    private String ownerName;
    private long dataSize;
    private final List<OmKeyLocationInfoGroup> omKeyLocationInfoGroups =
        new ArrayList<>();
    private long creationTime;
    private long modificationTime;
    private ReplicationConfig replicationConfig;
    private FileEncryptionInfo encInfo;
    private final AclListBuilder acls;
    // not persisted to DB. FileName will be the last element in path keyName.
    private String fileName;
    private FileChecksum fileChecksum;

    private boolean isFile;
    private final MapBuilder<String, String> tags;
    private Long expectedDataGeneration = null;
    private String expectedETag;

    public Builder() {
      this.acls = AclListBuilder.empty();
      this.tags = MapBuilder.empty();
    }

    public Builder(OmKeyInfo obj) {
      super(obj);
      this.acls = AclListBuilder.of(obj.acls);
      this.volumeName = obj.volumeName;
      this.bucketName = obj.bucketName;
      this.keyName = obj.keyName;
      this.ownerName = obj.ownerName;
      this.dataSize = obj.dataSize;
      this.creationTime = obj.creationTime;
      this.modificationTime = obj.modificationTime;
      this.replicationConfig = obj.replicationConfig;
      this.encInfo = obj.encInfo;
      this.fileName = obj.fileName;
      this.fileChecksum = obj.fileChecksum;
      this.isFile = obj.isFile;
      this.expectedDataGeneration = obj.expectedDataGeneration;
      this.expectedETag = obj.expectedETag;
      this.tags = MapBuilder.of(obj.tags);
      obj.keyLocationVersions.forEach(keyLocationVersion ->
          this.omKeyLocationInfoGroups.add(
              new OmKeyLocationInfoGroup(keyLocationVersion.getVersion(),
                  keyLocationVersion.getLocationList(),
                  keyLocationVersion.isMultipartKey())));
    }

    private Builder(OmDirectoryInfo dirInfo) {
      super(dirInfo);
      this.acls = AclListBuilder.of(dirInfo.getAcls());
      this.keyName = dirInfo.getName();
      this.creationTime = dirInfo.getCreationTime();
      this.modificationTime = dirInfo.getModificationTime();
      this.ownerName = dirInfo.getOwner();
      this.tags = MapBuilder.empty();
      this.replicationConfig = RatisReplicationConfig
          .getInstance(ReplicationFactor.ONE);
      this.omKeyLocationInfoGroups.add(
          new OmKeyLocationInfoGroup(0, new ArrayList<>()));
    }

    public Builder setVolumeName(String volume) {
      this.volumeName = volume;
      return this;
    }

    public Builder setBucketName(String bucket) {
      this.bucketName = bucket;
      return this;
    }

    public Builder setKeyName(String newValue) {
      if (!Objects.equals(newValue, keyName)) {
        this.keyName = newValue;
        this.fileName = null;
      }
      return this;
    }

    public Builder setOwnerName(String owner) {
      this.ownerName = owner;
      return this;
    }

    public Builder setOmKeyLocationInfos(
        List<OmKeyLocationInfoGroup> omKeyLocationInfoList) {
      if (omKeyLocationInfoList != null) {
        this.omKeyLocationInfoGroups.clear();
        this.omKeyLocationInfoGroups.addAll(omKeyLocationInfoList);
      }
      return this;
    }

    public Builder addOmKeyLocationInfoGroup(OmKeyLocationInfoGroup
        omKeyLocationInfoGroup) {
      if (omKeyLocationInfoGroup != null) {
        this.omKeyLocationInfoGroups.add(omKeyLocationInfoGroup);
      }
      return this;
    }

    public Builder setDataSize(long size) {
      this.dataSize = size;
      return this;
    }

    public Builder setCreationTime(long crTime) {
      this.creationTime = crTime;
      return this;
    }

    public Builder setModificationTime(long mTime) {
      this.modificationTime = mTime;
      return this;
    }

    public Builder setReplicationConfig(ReplicationConfig replConfig) {
      this.replicationConfig = replConfig;
      return this;
    }

    @Override
    public Builder addMetadata(String key, String value) {
      super.addMetadata(key, value);
      return this;
    }

    @Override
    public Builder addAllMetadata(Map<String, String> newMetadata) {
      super.addAllMetadata(newMetadata);
      return this;
    }

    @Override
    public Builder setMetadata(Map<String, String> map) {
      super.setMetadata(map);
      return this;
    }

    public Builder setFileEncryptionInfo(FileEncryptionInfo feInfo) {
      this.encInfo = feInfo;
      return this;
    }

    public Builder setAcls(List<OzoneAcl> listOfAcls) {
      if (listOfAcls != null) {
        this.acls.set(listOfAcls);
      }
      return this;
    }

    public AclListBuilder acls() {
      return acls;
    }

    public Builder addAcl(OzoneAcl ozoneAcl) {
      if (ozoneAcl != null) {
        this.acls.add(ozoneAcl);
      }
      return this;
    }

    @Override
    public Builder setObjectID(long obId) {
      super.setObjectID(obId);
      return this;
    }

    @Override
    public Builder setUpdateID(long id) {
      super.setUpdateID(id);
      return this;
    }

    @Override
    public Builder setParentObjectID(long parentID) {
      super.setParentObjectID(parentID);
      return this;
    }

    public Builder setFileChecksum(FileChecksum checksum) {
      this.fileChecksum = checksum;
      return this;
    }

    public Builder setFile(boolean isAFile) {
      this.isFile = isAFile;
      return this;
    }

    public Builder setTags(Map<String, String> tags) {
      this.tags.set(tags);
      return this;
    }

    public Builder addTag(String key, String value) {
      this.tags.put(key, value);
      return this;
    }

    public Builder addAllTags(Map<String, String> keyTags) {
      this.tags.putAll(keyTags);
      return this;
    }

    public Builder setExpectedDataGeneration(Long existingGeneration) {
      this.expectedDataGeneration = existingGeneration;
      return this;
    }

    public Builder setExpectedETag(String eTag) {
      this.expectedETag = eTag;
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      Objects.requireNonNull(keyName, "keyName == null");
    }

    @Override
    protected OmKeyInfo buildObject() {
      // not persisted to DB
      if (fileName == null) {
        fileName = OzoneFSUtils.getFileName(keyName);
      }
      return new OmKeyInfo(this);
    }
  }

  /**
   * For network transmit.
   * @return KeyInfo
   */
  public KeyInfo getProtobuf(int clientVersion) {
    return getProtobuf(false, clientVersion);
  }

  /**
   * For network transmit to return KeyInfo.
   * @param clientVersion
   * @param latestVersion
   * @return key info.
   */
  public KeyInfo getNetworkProtobuf(int clientVersion, boolean latestVersion) {
    return getProtobuf(false, null, clientVersion, latestVersion);
  }

  /**
   * For network transmit to return KeyInfo.
   *
   * @param fullKeyName the user given full key name
   * @param clientVersion
   * @param latestVersion
   * @return key info with the user given full key name
   */
  public KeyInfo getNetworkProtobuf(String fullKeyName, int clientVersion,
      boolean latestVersion) {
    return getProtobuf(false, fullKeyName, clientVersion, latestVersion);
  }

  /**
   *
   * @param ignorePipeline true for persist to DB, false for network transmit.
   * @return KeyInfo
   */
  public KeyInfo getProtobuf(boolean ignorePipeline, int clientVersion) {
    return getProtobuf(ignorePipeline, null, clientVersion, false);
  }

  /**
   * Gets KeyInfo with the user given key name.
   *
   * @param ignorePipeline   ignore pipeline flag
   * @param fullKeyName user given key name
   * @return key info object
   */
  private KeyInfo getProtobuf(boolean ignorePipeline, String fullKeyName,
                              int clientVersion, boolean latestVersionBlocks) {
    long latestVersion = keyLocationVersions.isEmpty() ? -1 :
        keyLocationVersions.get(keyLocationVersions.size() - 1).getVersion();

    List<KeyLocationList> keyLocations = new ArrayList<>();
    if (!latestVersionBlocks) {
      for (OmKeyLocationInfoGroup locationInfoGroup : keyLocationVersions) {
        keyLocations.add(locationInfoGroup.getProtobuf(
            ignorePipeline, clientVersion));
      }
    } else {
      if (latestVersion != -1) {
        keyLocations.add(keyLocationVersions.get(keyLocationVersions.size() - 1)
            .getProtobuf(ignorePipeline, clientVersion));
      }
    }

    KeyInfo.Builder kb = KeyInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setDataSize(dataSize)
        .setType(replicationConfig.getReplicationType());
    if (replicationConfig instanceof ECReplicationConfig) {
      kb.setEcReplicationConfig(
          ((ECReplicationConfig) replicationConfig).toProto());
    } else {
      kb.setFactor(ReplicationConfig.getLegacyFactor(replicationConfig));
    }
    kb.setLatestVersion(latestVersion)
        .addAllKeyLocationList(keyLocations)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .addAllMetadata(KeyValueUtil.toProtobuf(getMetadata()))
        .addAllTags(KeyValueUtil.toProtobuf(getTags()))
        .addAllAcls(OzoneAclUtil.toProtobuf(acls))
        .setObjectID(getObjectID())
        .setUpdateID(getUpdateID())
        .setParentID(getParentObjectID());

    FileChecksumProto fileChecksumProto = OMPBHelper.convert(fileChecksum);
    if (fileChecksumProto != null) {
      kb.setFileChecksum(fileChecksumProto);
    }
    if (StringUtils.isNotBlank(fullKeyName)) {
      kb.setKeyName(fullKeyName);
    } else {
      kb.setKeyName(keyName);
    }
    if (encInfo != null) {
      kb.setFileEncryptionInfo(OMPBHelper.convert(encInfo));
    }
    kb.setIsFile(isFile);
    if (expectedDataGeneration != null) {
      kb.setExpectedDataGeneration(expectedDataGeneration);
    }
    if (expectedETag != null) {
      kb.setExpectedETag(expectedETag);
    }
    if (ownerName != null) {
      kb.setOwnerName(ownerName);
    }
    return kb.build();
  }

  public static Builder builderFromProtobuf(KeyInfo keyInfo) {
    if (keyInfo == null) {
      return null;
    }

    List<OmKeyLocationInfoGroup> omKeyLocationInfos = new ArrayList<>();
    for (KeyLocationList keyLocationList : keyInfo.getKeyLocationListList()) {
      omKeyLocationInfos.add(
          OmKeyLocationInfoGroup.getFromProtobuf(keyLocationList));
    }

    Builder builder = new Builder()
        .setVolumeName(keyInfo.getVolumeName())
        .setBucketName(keyInfo.getBucketName())
        .setKeyName(keyInfo.getKeyName())
        .setOmKeyLocationInfos(omKeyLocationInfos)
        .setDataSize(keyInfo.getDataSize())
        .setCreationTime(keyInfo.getCreationTime())
        .setModificationTime(keyInfo.getModificationTime())
        .setReplicationConfig(ReplicationConfig
            .fromProto(keyInfo.getType(), keyInfo.getFactor(),
                keyInfo.getEcReplicationConfig()))
        .addAllMetadata(KeyValueUtil.getFromProtobuf(keyInfo.getMetadataList()))
        .addAllTags(KeyValueUtil.getFromProtobuf(keyInfo.getTagsList()))
        .setFileEncryptionInfo(keyInfo.hasFileEncryptionInfo() ?
            OMPBHelper.convert(keyInfo.getFileEncryptionInfo()) : null)
        .setAcls(OzoneAclUtil.fromProtobuf(keyInfo.getAclsList()));
    if (keyInfo.hasObjectID()) {
      builder.setObjectID(keyInfo.getObjectID());
    }
    if (keyInfo.hasUpdateID()) {
      builder.setUpdateID(keyInfo.getUpdateID());
    }
    if (keyInfo.hasParentID()) {
      builder.setParentObjectID(keyInfo.getParentID());
    }
    if (keyInfo.hasFileChecksum()) {
      FileChecksum fileChecksum = OMPBHelper.convert(keyInfo.getFileChecksum());
      builder.setFileChecksum(fileChecksum);
    }

    if (keyInfo.hasIsFile()) {
      builder.setFile(keyInfo.getIsFile());
    }
    if (keyInfo.hasExpectedDataGeneration()) {
      builder.setExpectedDataGeneration(keyInfo.getExpectedDataGeneration());
    }
    if (keyInfo.hasExpectedETag()) {
      builder.setExpectedETag(keyInfo.getExpectedETag());
    }

    if (keyInfo.hasOwnerName()) {
      builder.setOwnerName(keyInfo.getOwnerName());
    }
    return builder;
  }

  public static OmKeyInfo getFromProtobuf(KeyInfo keyInfo) {
    Builder builder = builderFromProtobuf(keyInfo);
    return builder == null ? null : builder.build();
  }

  @Override
  public String getObjectInfo() {
    return "OMKeyInfo{" +
        "volume='" + volumeName + '\'' +
        ", bucket='" + bucketName + '\'' +
        ", key='" + keyName + '\'' +
        ", owner='" + ownerName + '\'' +
        ", dataSize='" + dataSize + '\'' +
        ", creationTime='" + creationTime + '\'' +
        ", objectID='" + getObjectID() + '\'' +
        ", parentID='" + getParentObjectID() + '\'' +
        ", replication='" + replicationConfig + '\'' +
        ", fileChecksum='" + fileChecksum +
        '}';
  }

  public boolean isKeyInfoSame(OmKeyInfo omKeyInfo, boolean checkPath,
                               boolean checkKeyLocationVersions,
                               boolean checkModificationTime,
                               boolean checkUpdateID,
                               boolean checkOwnerName) {
    boolean isEqual = dataSize == omKeyInfo.dataSize &&
        creationTime == omKeyInfo.creationTime &&
        volumeName.equals(omKeyInfo.volumeName) &&
        bucketName.equals(omKeyInfo.bucketName) &&
        replicationConfig.equals(omKeyInfo.replicationConfig) &&
        Objects.equals(getMetadata(), omKeyInfo.getMetadata()) &&
        Objects.equals(acls, omKeyInfo.acls) &&
        Objects.equals(getTags(), omKeyInfo.getTags()) &&
        getObjectID() == omKeyInfo.getObjectID();

    if (isEqual && checkUpdateID) {
      isEqual = getUpdateID() == omKeyInfo.getUpdateID();
    }

    if (isEqual && checkModificationTime) {
      isEqual = modificationTime == omKeyInfo.modificationTime;
    }

    if (isEqual && checkPath) {
      isEqual = getParentObjectID() == omKeyInfo.getParentObjectID() &&
          keyName.equals(omKeyInfo.keyName);
    }

    if (isEqual && checkKeyLocationVersions) {
      isEqual = Objects
          .equals(keyLocationVersions, omKeyInfo.keyLocationVersions);
    }

    if (isEqual && checkOwnerName) {
      isEqual = Objects
          .equals(ownerName, omKeyInfo.ownerName);
    }

    return isEqual;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return isKeyInfoSame((OmKeyInfo) o, true, true, true, true, true);
  }

  @Override
  public int hashCode() {
    return Objects.hash(volumeName, bucketName, keyName, getParentObjectID());
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  public static Builder builderFromOmDirectoryInfo(OmDirectoryInfo dirInfo) {
    return new Builder(dirInfo);
  }

  public OmDirectoryInfo.Builder toDirectoryInfoBuilder() {
    return OmDirectoryInfo.builderFromOmKeyInfo(this);
  }

  /**
   * Return a new copy of the object.
   */
  @Override
  public OmKeyInfo copyObject() {
    return new Builder(this).build();
  }

  /**
   * Method to clear the fileEncryptionInfo.
   * This method is called when a KeyDelete operation is performed.
   * This ensures that when TDE is enabled and GDPR is enforced on a bucket,
   * the encryption info is deleted from Key Metadata before the key is moved
   * to deletedTable in OM Metadata.
   */
  public void clearFileEncryptionInfo() {
    this.encInfo = null;
  }

  /**
   * Set the file encryption info.
   * @param fileEncryptionInfo
   */
  public void setFileEncryptionInfo(FileEncryptionInfo fileEncryptionInfo) {
    this.encInfo = fileEncryptionInfo;
  }

  public String getPath() {
    if (StringUtils.isBlank(getFileName())) {
      return getKeyName();
    }
    return getParentObjectID() + OzoneConsts.OM_KEY_PREFIX + getFileName();
  }

  public boolean hasBlocks() {
    for (OmKeyLocationInfoGroup keyLocationList : getKeyLocationVersions()) {
      if (keyLocationList.getLocationListCount() != 0) {
        return true;
      }
    }
    return false;
  }

  public static boolean isKeyEmpty(@Nullable OmKeyInfo keyInfo) {
    if (keyInfo == null) {
      return true;
    }
    return !keyInfo.hasBlocks();
  }
}
