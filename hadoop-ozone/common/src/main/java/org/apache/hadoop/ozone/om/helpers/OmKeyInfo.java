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

import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocationList;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;

/**
 * Args for key block. The block instance for the key requested in putKey.
 * This is returned from OM to client, and client use class to talk to
 * datanode. Also, this is the metadata written to om.db on server side.
 */
public final class OmKeyInfo extends WithObjectID {
  private final String volumeName;
  private final String bucketName;
  // name of key client specified
  private String keyName;
  private long dataSize;
  private List<OmKeyLocationInfoGroup> keyLocationVersions;
  private final long creationTime;
  private long modificationTime;
  private HddsProtos.ReplicationType type;
  private HddsProtos.ReplicationFactor factor;
  private FileEncryptionInfo encInfo;

  /**
   * ACL Information.
   */
  private List<OzoneAcl> acls;

  @SuppressWarnings("parameternumber")
  OmKeyInfo(String volumeName, String bucketName, String keyName,
      List<OmKeyLocationInfoGroup> versions, long dataSize,
      long creationTime, long modificationTime,
      HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor,
      Map<String, String> metadata,
      FileEncryptionInfo encInfo, List<OzoneAcl> acls,
      long objectID, long updateID) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.dataSize = dataSize;
    // it is important that the versions are ordered from old to new.
    // Do this sanity check when versions got loaded on creating OmKeyInfo.
    // TODO : this is not necessary, here only because versioning is still a
    // work in-progress, remove this following check when versioning is
    // complete and prove correctly functioning
    long currentVersion = -1;
    for (OmKeyLocationInfoGroup version : versions) {
      Preconditions.checkArgument(
            currentVersion + 1 == version.getVersion());
      currentVersion = version.getVersion();
    }
    this.keyLocationVersions = versions;
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
    this.factor = factor;
    this.type = type;
    this.metadata = metadata;
    this.encInfo = encInfo;
    this.acls = acls;
    this.objectID = objectID;
    this.updateID = updateID;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public HddsProtos.ReplicationType getType() {
    return type;
  }

  public HddsProtos.ReplicationFactor getFactor() {
    return factor;
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

  public void setDataSize(long size) {
    this.dataSize = size;
  }

  public synchronized OmKeyLocationInfoGroup getLatestVersionLocations() {
    return keyLocationVersions.size() == 0? null :
        keyLocationVersions.get(keyLocationVersions.size() - 1);
  }

  public List<OmKeyLocationInfoGroup> getKeyLocationVersions() {
    return keyLocationVersions;
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
  public void updateLocationInfoList(List<OmKeyLocationInfo> locationInfoList) {
    long latestVersion = getLatestVersionLocations().getVersion();
    OmKeyLocationInfoGroup keyLocationInfoGroup = getLatestVersionLocations();
    // Updates the latest locationList in the latest version only with
    // given locationInfoList here.
    // TODO : The original allocated list and the updated list here may vary
    // as the containers on the Datanode on which the blocks were pre allocated
    // might get closed. The diff of blocks between these two lists here
    // need to be garbage collected in case the ozone client dies.
    keyLocationInfoGroup.removeBlocks(latestVersion);
    // set each of the locationInfo object to the latest version
    locationInfoList.forEach(omKeyLocationInfo -> omKeyLocationInfo
        .setCreateVersion(latestVersion));
    keyLocationInfoGroup.addAll(latestVersion, locationInfoList);
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
   * @throws IOException
   */
  public synchronized long addNewVersion(
      List<OmKeyLocationInfo> newLocationList, boolean updateTime)
      throws IOException {
    long latestVersionNum;
    if (keyLocationVersions.size() == 0) {
      // no version exist, these blocks are the very first version.
      keyLocationVersions.add(new OmKeyLocationInfoGroup(0, newLocationList));
      latestVersionNum = 0;
    } else {
      // it is important that the new version are always at the tail of the list
      OmKeyLocationInfoGroup currentLatestVersion =
          keyLocationVersions.get(keyLocationVersions.size() - 1);
      // the new version is created based on the current latest version
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

  public boolean addAcl(OzoneAcl acl) {
    return OzoneAclUtil.addAcl(acls, acl);
  }

  public boolean removeAcl(OzoneAcl acl) {
    return OzoneAclUtil.removeAcl(acls, acl);
  }

  public boolean setAcls(List<OzoneAcl> newAcls) {
    return OzoneAclUtil.setAcl(acls, newAcls);
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
    private HddsProtos.ReplicationType type;
    private HddsProtos.ReplicationFactor factor;
    private Map<String, String> metadata;
    private FileEncryptionInfo encInfo;
    private List<OzoneAcl> acls;
    private long objectID;
    private long updateID;

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

    public Builder setReplicationFactor(HddsProtos.ReplicationFactor replFact) {
      this.factor = replFact;
      return this;
    }

    public Builder setReplicationType(HddsProtos.ReplicationType replType) {
      this.type = replType;
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

    public OmKeyInfo build() {
      return new OmKeyInfo(
          volumeName, bucketName, keyName, omKeyLocationInfoGroups,
          dataSize, creationTime, modificationTime, type, factor, metadata,
          encInfo, acls, objectID, updateID);
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
   *
   * @param ignorePipeline true for persist to DB, false for network transmit.
   * @return
   */
  public KeyInfo getProtobuf(boolean ignorePipeline, int clientVersion) {
    long latestVersion = keyLocationVersions.size() == 0 ? -1 :
        keyLocationVersions.get(keyLocationVersions.size() - 1).getVersion();

    List<KeyLocationList> keyLocations = new ArrayList<>();
    for (OmKeyLocationInfoGroup locationInfoGroup : keyLocationVersions) {
      keyLocations.add(locationInfoGroup.getProtobuf(
          ignorePipeline, clientVersion));
    }

    KeyInfo.Builder kb = KeyInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(dataSize)
        .setFactor(factor)
        .setType(type)
        .setLatestVersion(latestVersion)
        .addAllKeyLocationList(keyLocations)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .addAllMetadata(KeyValueUtil.toProtobuf(metadata))
        .addAllAcls(OzoneAclUtil.toProtobuf(acls))
        .setObjectID(objectID)
        .setUpdateID(updateID);
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
        .setReplicationType(keyInfo.getType())
        .setReplicationFactor(keyInfo.getFactor())
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
        ", type='" + type + '\'' +
        ", factor='" + factor + '\'' +
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
        type == omKeyInfo.type &&
        factor == omKeyInfo.factor &&
        Objects.equals(metadata, omKeyInfo.metadata) &&
        Objects.equals(acls, omKeyInfo.acls) &&
        objectID == omKeyInfo.objectID &&
        updateID == omKeyInfo.updateID;
  }

  @Override
  public int hashCode() {
    return Objects.hash(volumeName, bucketName, keyName);
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
        .setReplicationType(type)
        .setReplicationFactor(factor)
        .setFileEncryptionInfo(encInfo)
        .setObjectID(objectID).setUpdateID(updateID);


    keyLocationVersions.forEach(keyLocationVersion ->
        builder.addOmKeyLocationInfoGroup(
            new OmKeyLocationInfoGroup(keyLocationVersion.getVersion(),
                keyLocationVersion.getLocationList())));

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
}
