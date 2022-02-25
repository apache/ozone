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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
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
public final class OmKeyInfo extends WithParentObjectId {
  private static final Logger LOG = LoggerFactory.getLogger(OmKeyInfo.class);
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

  /**
   * Represents leaf node name. This also will be used when the keyName is
   * created on a FileSystemOptimized(FSO) bucket. For example, the user given
   * keyName is "a/b/key1" then the fileName stores "key1".
   */
  private String fileName;

  /**
   * ACL Information.
   */
  private List<OzoneAcl> acls;

  @SuppressWarnings("parameternumber")
  OmKeyInfo(String volumeName, String bucketName, String keyName,
      List<OmKeyLocationInfoGroup> versions, long dataSize,
      long creationTime, long modificationTime,
      ReplicationConfig replicationConfig,
      Map<String, String> metadata,
      FileEncryptionInfo encInfo, List<OzoneAcl> acls,
      long objectID, long updateID) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.dataSize = dataSize;
    this.keyLocationVersions = versions;
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
    this.replicationConfig = replicationConfig;
    this.metadata = metadata;
    this.encInfo = encInfo;
    this.acls = acls;
    this.objectID = objectID;
    this.updateID = updateID;
  }

  @SuppressWarnings("parameternumber")
  OmKeyInfo(String volumeName, String bucketName, String keyName,
            String fileName, List<OmKeyLocationInfoGroup> versions,
            long dataSize, long creationTime, long modificationTime,
            ReplicationConfig replicationConfig,
            Map<String, String> metadata,
            FileEncryptionInfo encInfo, List<OzoneAcl> acls,
            long parentObjectID, long objectID, long updateID) {
    this(volumeName, bucketName, keyName, versions, dataSize,
            creationTime, modificationTime, replicationConfig, metadata,
            encInfo, acls, objectID, updateID);
    this.fileName = fileName;
    this.parentObjectID = parentObjectID;
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
    this.keyName = keyName;
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

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public String getFileName() {
    return fileName;
  }

  public long getParentObjectID() {
    return parentObjectID;
  }


  public synchronized OmKeyLocationInfoGroup getLatestVersionLocations() {
    return keyLocationVersions.size() == 0 ? null :
        keyLocationVersions.get(keyLocationVersions.size() - 1);
  }

  public List<OmKeyLocationInfoGroup> getKeyLocationVersions() {
    return keyLocationVersions;
  }

  public void setKeyLocationVersions(
      List<OmKeyLocationInfoGroup> keyLocationVersions) {
    this.keyLocationVersions = keyLocationVersions;
  }

  public void updateModifcationTime() {
    this.modificationTime = Time.monotonicNow();
  }

  /**
   * updates the length of the each block in the list given.
   * This will be called when the key is being committed to OzoneManager.
   *
   * @param locationInfoList list of locationInfo
   */
  public void updateLocationInfoList(List<OmKeyLocationInfo> locationInfoList,
      boolean isMpu) {
    updateLocationInfoList(locationInfoList, isMpu, false);
  }

  /**
   * updates the length of the each block in the list given.
   * This will be called when the key is being committed to OzoneManager.
   *
   * @param locationInfoList list of locationInfo
   * @param isMpu a true represents multi part key, false otherwise
   * @param skipBlockIDCheck a true represents that the blockId verification
   *                         check should be skipped, false represents that
   *                         the blockId verification will be required
   */
  public void updateLocationInfoList(List<OmKeyLocationInfo> locationInfoList,
      boolean isMpu, boolean skipBlockIDCheck) {
    long latestVersion = getLatestVersionLocations().getVersion();
    OmKeyLocationInfoGroup keyLocationInfoGroup = getLatestVersionLocations();

    keyLocationInfoGroup.setMultipartKey(isMpu);

    // Compare user given block location against allocatedBlockLocations
    // present in OmKeyInfo.
    List<OmKeyLocationInfo> updatedBlockLocations;
    if (skipBlockIDCheck) {
      updatedBlockLocations = locationInfoList;
    } else {
      updatedBlockLocations =
          verifyAndGetKeyLocations(locationInfoList, keyLocationInfoGroup);
    }
    // Updates the latest locationList in the latest version only with
    // given locationInfoList here.
    // TODO : The original allocated list and the updated list here may vary
    // as the containers on the Datanode on which the blocks were pre allocated
    // might get closed. The diff of blocks between these two lists here
    // need to be garbage collected in case the ozone client dies.
    keyLocationInfoGroup.removeBlocks(latestVersion);
    // set each of the locationInfo object to the latest version
    updatedBlockLocations.forEach(omKeyLocationInfo -> omKeyLocationInfo
        .setCreateVersion(latestVersion));
    keyLocationInfoGroup.addAll(latestVersion, updatedBlockLocations);
  }

  private List<OmKeyLocationInfo> verifyAndGetKeyLocations(
      List<OmKeyLocationInfo> locationInfoList,
      OmKeyLocationInfoGroup keyLocationInfoGroup) {

    List<OmKeyLocationInfo> allocatedBlockLocations =
        keyLocationInfoGroup.getBlocksLatestVersionOnly();
    List<OmKeyLocationInfo> updatedBlockLocations = new ArrayList<>();

    List<ContainerBlockID> existingBlockIDs = new ArrayList<>();
    for (OmKeyLocationInfo existingLocationInfo : allocatedBlockLocations) {
      BlockID existingBlockID = existingLocationInfo.getBlockID();
      existingBlockIDs.add(existingBlockID.getContainerBlockID());
    }

    for (OmKeyLocationInfo modifiedLocationInfo : locationInfoList) {
      BlockID modifiedBlockID = modifiedLocationInfo.getBlockID();
      if (existingBlockIDs.contains(modifiedBlockID.getContainerBlockID())) {
        updatedBlockLocations.add(modifiedLocationInfo);
      } else {
        LOG.warn("Unknown BlockLocation:{}, where the blockID of given "
            + "location doesn't match with the stored/allocated block of"
            + " keyName:{}", modifiedLocationInfo, keyName);
      }
    }
    return updatedBlockLocations;
  }

  /**
   * Append a set of blocks to the latest version. Note that these blocks are
   * part of the latest version, not a new version.
   *
   * @param newLocationList the list of new blocks to be added.
   * @param updateTime if true, will update modification time.
   * @throws IOException
   */
  public synchronized void appendNewBlocks(
      List<OmKeyLocationInfo> newLocationList, boolean updateTime)
      throws IOException {
    if (keyLocationVersions.size() == 0) {
      throw new IOException("Appending new block, but no version exist");
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
   * @param updateTime - if true, updates modification time.
   * @param keepOldVersions - if false, old blocks won't be kept.
   * @throws IOException
   */
  public synchronized long addNewVersion(
      List<OmKeyLocationInfo> newLocationList, boolean updateTime,
      boolean keepOldVersions) {
    long latestVersionNum;
    if (keyLocationVersions.size() == 0) {
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
      if (!keepOldVersions) {
        // Even though old versions are cleared here, they will be
        // moved to delete table at the time of key commit
        keyLocationVersions.clear();
      }
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

  public boolean addAcl(OzoneAcl acl) {
    return OzoneAclUtil.addAcl(acls, acl);
  }

  public boolean removeAcl(OzoneAcl acl) {
    return OzoneAclUtil.removeAcl(acls, acl);
  }

  public boolean setAcls(List<OzoneAcl> newAcls) {
    return OzoneAclUtil.setAcl(acls, newAcls);
  }

  public void setParentObjectID(long parentObjectID) {
    this.parentObjectID = parentObjectID;
  }

  /**
   * Builder of OmKeyInfo.
   */
  public static class Builder {
    private String volumeName;
    private String bucketName;
    private String keyName;
    private long dataSize;
    private List<OmKeyLocationInfoGroup> omKeyLocationInfoGroups =
        new ArrayList<>();
    private long creationTime;
    private long modificationTime;
    private ReplicationConfig replicationConfig;
    private Map<String, String> metadata;
    private FileEncryptionInfo encInfo;
    private List<OzoneAcl> acls;
    private long objectID;
    private long updateID;
    // not persisted to DB. FileName will be the last element in path keyName.
    private String fileName;
    private long parentObjectID;

    public Builder() {
      this.metadata = new HashMap<>();
      omKeyLocationInfoGroups = new ArrayList<>();
      acls = new ArrayList<>();
    }

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

    public Builder setOmKeyLocationInfos(
        List<OmKeyLocationInfoGroup> omKeyLocationInfoList) {
      if (omKeyLocationInfoList != null) {
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

    public Builder addMetadata(String key, String value) {
      metadata.put(key, value);
      return this;
    }

    public Builder addAllMetadata(Map<String, String> newMetadata) {
      metadata.putAll(newMetadata);
      return this;
    }

    public Builder setFileEncryptionInfo(FileEncryptionInfo feInfo) {
      this.encInfo = feInfo;
      return this;
    }

    public Builder setAcls(List<OzoneAcl> listOfAcls) {
      if (listOfAcls != null) {
        this.acls.addAll(listOfAcls);
      }
      return this;
    }

    public Builder addAcl(OzoneAcl ozoneAcl) {
      if (ozoneAcl != null) {
        this.acls.add(ozoneAcl);
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

    public Builder setFileName(String keyFileName) {
      this.fileName = keyFileName;
      return this;
    }

    public Builder setParentObjectID(long parentID) {
      this.parentObjectID = parentID;
      return this;
    }

    public OmKeyInfo build() {
      return new OmKeyInfo(
              volumeName, bucketName, keyName, fileName,
              omKeyLocationInfoGroups, dataSize, creationTime,
              modificationTime, replicationConfig, metadata, encInfo, acls,
              parentObjectID, objectID, updateID);
    }
  }

  /**
   * For network transmit.
   * @return
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
   * @return
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
    long latestVersion = keyLocationVersions.size() == 0 ? -1 :
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
        .addAllMetadata(KeyValueUtil.toProtobuf(metadata))
        .addAllAcls(OzoneAclUtil.toProtobuf(acls))
        .setObjectID(objectID)
        .setUpdateID(updateID)
        .setParentID(parentObjectID);
    if (StringUtils.isNotBlank(fullKeyName)) {
      kb.setKeyName(fullKeyName);
    } else {
      kb.setKeyName(keyName);
    }
    if (encInfo != null) {
      kb.setFileEncryptionInfo(OMPBHelper.convert(encInfo));
    }
    return kb.build();
  }

  public static OmKeyInfo getFromProtobuf(KeyInfo keyInfo) {
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
    // not persisted to DB. FileName will be filtered out from keyName
    builder.setFileName(OzoneFSUtils.getFileName(keyInfo.getKeyName()));
    return builder.build();
  }

  @Override
  public String getObjectInfo() {
    return "OMKeyInfo{" +
        "volume='" + volumeName + '\'' +
        ", bucket='" + bucketName + '\'' +
        ", key='" + keyName + '\'' +
        ", dataSize='" + dataSize + '\'' +
        ", creationTime='" + creationTime + '\'' +
        ", objectID='" + objectID + '\'' +
        ", parentID='" + parentObjectID + '\'' +
        ", replication='" + replicationConfig +
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
    OmKeyInfo omKeyInfo = (OmKeyInfo) o;
    return dataSize == omKeyInfo.dataSize &&
        creationTime == omKeyInfo.creationTime &&
        modificationTime == omKeyInfo.modificationTime &&
        volumeName.equals(omKeyInfo.volumeName) &&
        bucketName.equals(omKeyInfo.bucketName) &&
        keyName.equals(omKeyInfo.keyName) &&
        Objects
            .equals(keyLocationVersions, omKeyInfo.keyLocationVersions) &&
        replicationConfig.equals(omKeyInfo.replicationConfig) &&
        Objects.equals(metadata, omKeyInfo.metadata) &&
        Objects.equals(acls, omKeyInfo.acls) &&
        objectID == omKeyInfo.objectID &&
        updateID == omKeyInfo.updateID &&
        parentObjectID == omKeyInfo.parentObjectID;
  }

  @Override
  public int hashCode() {
    return Objects.hash(volumeName, bucketName, keyName, parentObjectID);
  }

  /**
   * Return a new copy of the object.
   */
  public OmKeyInfo copyObject() {
    OmKeyInfo.Builder builder = new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setDataSize(dataSize)
        .setReplicationConfig(replicationConfig)
        .setFileEncryptionInfo(encInfo)
        .setObjectID(objectID)
        .setUpdateID(updateID)
        .setParentObjectID(parentObjectID)
        .setFileName(fileName);

    keyLocationVersions.forEach(keyLocationVersion ->
        builder.addOmKeyLocationInfoGroup(
            new OmKeyLocationInfoGroup(keyLocationVersion.getVersion(),
                keyLocationVersion.getLocationList(),
                keyLocationVersion.isMultipartKey())));

    acls.forEach(acl -> builder.addAcl(new OzoneAcl(acl.getType(),
            acl.getName(), (BitSet) acl.getAclBitSet().clone(),
        acl.getAclScope())));

    if (metadata != null) {
      metadata.forEach((k, v) -> builder.addMetadata(k, v));
    }

    return builder.build();
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
}
