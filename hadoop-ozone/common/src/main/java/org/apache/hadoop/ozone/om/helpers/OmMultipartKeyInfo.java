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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;

/**
 * This class represents multipart upload information for a key, which holds
 * upload part information of the key.
 */
public final class OmMultipartKeyInfo extends WithObjectID implements CopyObject<OmMultipartKeyInfo> {
  private static final Codec<OmMultipartKeyInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(MultipartKeyInfo.getDefaultInstance()),
      OmMultipartKeyInfo::getFromProto,
      OmMultipartKeyInfo::getProto,
      OmMultipartKeyInfo.class);

  private final String uploadID;
  private final String volumeName;
  private final String bucketName;
  private final String keyName;
  private final String ownerName;
  /**
   * ACL information inherited during MPU initiation.
   */
  private final ImmutableList<OzoneAcl> acls;
  private final long creationTime;
  private final ReplicationConfig replicationConfig;
  private PartKeyInfoMap partKeyInfoMap;

  /**
   * A pointer to parent directory used for path traversal. ParentID will be
   * used only when the multipart key is created into a FileSystemOptimized(FSO)
   * bucket.
   * <p>
   * For example, if a key "a/b/multiKey1" created into a FSOBucket then each
   * path component will be assigned an ObjectId and linked to its parent path
   * component using parent's objectID.
   * <p>
   * Say, Bucket's ObjectID = 512, which is the parent for its immediate child
   * element.
   * <p>
   * ------------------------------------------|
   * PathComponent |   ObjectID   |   ParentID |
   * ------------------------------------------|
   *      a        |     1024     |     512    |
   * ------------------------------------------|
   *      b        |     1025     |     1024   |
   * ------------------------------------------|
   *   multiKey1   |     1026     |     1025   |
   * ------------------------------------------|
   */
  private final long parentID;

  // This stores the schema version of the multipart key.
  // 0 - Legacy Schema -> Uses the same table to store the multipart part info
  // 1 - New Schema -> Uses a separate table to store the multipart part info
  private final byte schemaVersion;

  public static Codec<OmMultipartKeyInfo> getCodec() {
    return CODEC;
  }

  /**
   * An unmodifiable Array wrapper providing PartKeyInfo sorted by partNumber,
   * Whenever a PartKeyInfo is added, it returns a new shallow copy of
   * the PartKeyInfoMap instance.
   */
  public static class PartKeyInfoMap implements Iterable<PartKeyInfo> {
    static final Comparator<Object> PART_NUMBER_COMPARATOR = (o1, o2) -> {
      final int partNumber1 = o1 instanceof PartKeyInfo ?
          ((PartKeyInfo) o1).getPartNumber() : (int) o1;
      final int partNumber2 = o2 instanceof PartKeyInfo ?
          ((PartKeyInfo) o2).getPartNumber() : (int) o2;
      return Integer.compare(partNumber1, partNumber2);
    };

    private final List<PartKeyInfo> sorted;

    /**
     * Adds a PartKeyInfo to sortedPartKeyInfoList.
     * If a partKeyInfo with the same PartNumber is in the array, the old value
     * will be replaced, otherwise the PartNumber will be inserted in the right
     * place to ensure the array is ordered.
     * @param partKeyInfo the partKeyInfo will be added
     */
    static void put(PartKeyInfo partKeyInfo, List<PartKeyInfo> sortedList) {
      if (partKeyInfo == null) {
        return;
      }
      final int i = Collections.binarySearch(sortedList, partKeyInfo,
          Comparator.comparingInt(PartKeyInfo::getPartNumber));
      if (i >= 0) {
        sortedList.set(i, partKeyInfo);
      } else {
        sortedList.add(-(i + 1), partKeyInfo);
      }
    }

    static PartKeyInfoMap put(PartKeyInfo partKeyInfo,
        PartKeyInfoMap sortedMap) {
      if (partKeyInfo == null) {
        return sortedMap;
      }
      final List<PartKeyInfo> list = new ArrayList<>(sortedMap.sorted);
      put(partKeyInfo, list);
      return new PartKeyInfoMap(list);
    }

    public PartKeyInfoMap(List<PartKeyInfo> sorted) {
      this.sorted = Collections.unmodifiableList(sorted);
    }

    public PartKeyInfoMap(SortedMap<Integer, PartKeyInfo> sorted) {
      this(new ArrayList<>(sorted.values()));
    }

    /**
     * Retrieves a PartKeyInfo based on its part number.
     *
     * @param partNumber The part number of the PartKeyInfo to retrieve.
     * @return The PartKeyInfo with the specified part number.
     *         If no such PartKeyInfo exists, returns null.
     */
    public PartKeyInfo get(int partNumber) {
      final int i = Collections.binarySearch(
          sorted, partNumber, PART_NUMBER_COMPARATOR);
      return i >= 0 ? sorted.get(i) : null;
    }

    public int size() {
      return sorted.size();
    }

    @Override
    public Iterator<PartKeyInfo> iterator() {
      return sorted.iterator();
    }

    public PartKeyInfo lastEntry() {
      return sorted.get(size() - 1);
    }
  }

  /**
   * Construct OmMultipartKeyInfo object which holds multipart upload
   * information for a key.
   */
  private OmMultipartKeyInfo(Builder b) {
    super(b);
    this.uploadID = b.uploadID;
    this.volumeName = b.volumeName;
    this.bucketName = b.bucketName;
    this.keyName = b.keyName;
    this.ownerName = b.ownerName;
    this.acls = b.acls.build();
    this.creationTime = b.creationTime;
    this.replicationConfig = b.replicationConfig;
    this.partKeyInfoMap = new PartKeyInfoMap(b.partKeyInfoList);
    this.parentID = b.parentID;
    this.schemaVersion = b.schemaVersion;
  }

  /** Copy constructor. */
  private OmMultipartKeyInfo(OmMultipartKeyInfo b) {
    super(b);
    this.uploadID = b.uploadID;
    this.volumeName = b.volumeName;
    this.bucketName = b.bucketName;
    this.keyName = b.keyName;
    this.ownerName = b.ownerName;
    this.acls = b.acls;
    this.creationTime = b.creationTime;
    this.replicationConfig = b.replicationConfig;
    // PartKeyInfoMap is an immutable data structure. Whenever a PartKeyInfo
    // is added, it returns a new shallow copy of the PartKeyInfoMap Object
    // so here we can directly pass in partKeyInfoMap
    this.partKeyInfoMap = b.partKeyInfoMap;
    this.parentID = b.parentID;
    this.schemaVersion = b.schemaVersion;
  }

  /**
   * Returns parentID.
   *
   * @return long
   */
  public long getParentID() {
    return parentID;
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

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getKeyName() {
    return keyName;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public List<OzoneAcl> getAcls() {
    return acls;
  }

  public PartKeyInfoMap getPartKeyInfoMap() {
    return partKeyInfoMap;
  }

  public void addPartKeyInfo(PartKeyInfo partKeyInfo) {
    if (schemaVersion == 1) {
      throw new IllegalStateException(
          "PartKeyInfoMap is not supported for schemaVersion 1");
    }
    this.partKeyInfoMap = PartKeyInfoMap.put(partKeyInfo, partKeyInfoMap);
  }

  public PartKeyInfo getPartKeyInfo(int partNumber) {
    return partKeyInfoMap.get(partNumber);
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  public byte getSchemaVersion() {
    return schemaVersion;
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  /**
   * Builder of OmMultipartKeyInfo.
   */
  public static class Builder extends WithObjectID.Builder<OmMultipartKeyInfo> {
    private String uploadID;
    private String volumeName;
    private String bucketName;
    private String keyName;
    private String ownerName;
    private long creationTime;
    private ReplicationConfig replicationConfig;
    private final AclListBuilder acls;
    private final TreeMap<Integer, PartKeyInfo> partKeyInfoList;
    private long parentID;
    private byte schemaVersion;

    public Builder() {
      this.acls = AclListBuilder.empty();
      this.partKeyInfoList = new TreeMap<>();
    }

    public Builder(OmMultipartKeyInfo multipartKeyInfo) {
      super(multipartKeyInfo);
      this.uploadID = multipartKeyInfo.uploadID;
      this.volumeName = multipartKeyInfo.volumeName;
      this.bucketName = multipartKeyInfo.bucketName;
      this.keyName = multipartKeyInfo.keyName;
      this.ownerName = multipartKeyInfo.ownerName;
      this.creationTime = multipartKeyInfo.creationTime;
      this.replicationConfig = multipartKeyInfo.replicationConfig;
      this.acls = AclListBuilder.of(multipartKeyInfo.acls);
      this.partKeyInfoList = new TreeMap<>();

      if (multipartKeyInfo.getSchemaVersion() == 0) {
        for (PartKeyInfo partKeyInfo : multipartKeyInfo.partKeyInfoMap) {
          this.partKeyInfoList.put(partKeyInfo.getPartNumber(), partKeyInfo);
        }
      }

      this.parentID = multipartKeyInfo.parentID;
      this.schemaVersion = multipartKeyInfo.schemaVersion;
    }

    public Builder setUploadID(String uploadId) {
      this.uploadID = uploadId;
      return this;
    }

    public Builder setVolumeName(String volName) {
      this.volumeName = volName;
      return this;
    }

    public Builder setBucketName(String buckName) {
      this.bucketName = buckName;
      return this;
    }

    public Builder setKeyName(String keyObjName) {
      this.keyName = keyObjName;
      return this;
    }

    public Builder setOwnerName(String owner) {
      this.ownerName = owner;
      return this;
    }

    public Builder setCreationTime(long crTime) {
      this.creationTime = crTime;
      return this;
    }

    public Builder setReplicationConfig(ReplicationConfig replConfig) {
      this.replicationConfig = replConfig;
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

    public Builder setParentID(long parentObjId) {
      this.parentID = parentObjId;
      return this;
    }

    public Builder setSchemaVersion(byte schemaVersion) {
      this.schemaVersion = schemaVersion;
      return this;
    }

    @Override
    protected OmMultipartKeyInfo buildObject() {
      return new OmMultipartKeyInfo(this);
    }
  }

  /**
   * Construct OmMultipartInfo Builder from MultipartKeyInfo proto object.
   * @param multipartKeyInfo
   * @return Builder instance
   */
  public static Builder builderFromProto(
      MultipartKeyInfo multipartKeyInfo) {
    final SortedMap<Integer, PartKeyInfo> list = new TreeMap<>();
    if (!multipartKeyInfo.hasSchemaVersion() || multipartKeyInfo.getSchemaVersion() == 0) {
      multipartKeyInfo.getPartKeyInfoListList().forEach(partKeyInfo ->
          list.put(partKeyInfo.getPartNumber(), partKeyInfo));
    }

    final ReplicationConfig replicationConfig = ReplicationConfig.fromProto(
        multipartKeyInfo.getType(),
        multipartKeyInfo.getFactor(),
        multipartKeyInfo.getEcReplicationConfig()
    );

    return new Builder()
        .setUploadID(multipartKeyInfo.getUploadID())
        .setVolumeName(multipartKeyInfo.hasVolumeName() ?
            multipartKeyInfo.getVolumeName() : null)
        .setBucketName(multipartKeyInfo.hasBucketName() ?
            multipartKeyInfo.getBucketName() : null)
        .setKeyName(multipartKeyInfo.hasKeyName() ?
            multipartKeyInfo.getKeyName() : null)
        .setOwnerName(multipartKeyInfo.hasOwnerName() ?
            multipartKeyInfo.getOwnerName() : null)
        .setCreationTime(multipartKeyInfo.getCreationTime())
        .setReplicationConfig(replicationConfig)
        .setAcls(OzoneAclUtil.fromProtobuf(multipartKeyInfo.getAclsList()))
        .setPartKeyInfoList(list)
        .setObjectID(multipartKeyInfo.getObjectID())
        .setUpdateID(multipartKeyInfo.getUpdateID())
        .setParentID(multipartKeyInfo.getParentID())
        .setSchemaVersion((byte) multipartKeyInfo.getSchemaVersion());
  }

  /**
   * Construct OmMultipartInfo from MultipartKeyInfo proto object.
   * @param multipartKeyInfo
   * @return OmMultipartKeyInfo
   */
  public static OmMultipartKeyInfo getFromProto(
      MultipartKeyInfo multipartKeyInfo) {
    return builderFromProto(multipartKeyInfo).build();
  }

  /**
   * Construct MultipartKeyInfo from this object.
   * @return MultipartKeyInfo
   */
  public MultipartKeyInfo getProto() {
    if (schemaVersion == 1 && partKeyInfoMap != null && partKeyInfoMap.size() > 0) {
      throw new IllegalStateException(
          "PartKeyInfoMap must be empty for schemaVersion 1");
    }

    MultipartKeyInfo.Builder builder = MultipartKeyInfo.newBuilder()
        .setUploadID(uploadID)
        .setCreationTime(creationTime)
        .setType(replicationConfig.getReplicationType())
        .setObjectID(getObjectID())
        .setUpdateID(getUpdateID())
        .setParentID(parentID)
        .setSchemaVersion(schemaVersion);
    if (volumeName != null) {
      builder.setVolumeName(volumeName);
    }
    if (bucketName != null) {
      builder.setBucketName(bucketName);
    }
    if (keyName != null) {
      builder.setKeyName(keyName);
    }
    if (ownerName != null) {
      builder.setOwnerName(ownerName);
    }

    if (replicationConfig instanceof ECReplicationConfig) {
      ECReplicationConfig ecConf = (ECReplicationConfig) replicationConfig;
      builder.setEcReplicationConfig(ecConf.toProto());
    } else {
      builder.setFactor(ReplicationConfig.getLegacyFactor(replicationConfig));
    }

    builder.addAllAcls(OzoneAclUtil.toProtobuf(acls));
    if (schemaVersion == 0) {
      builder.addAllPartKeyInfoList(partKeyInfoMap);
    }
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

  @Override
  public OmMultipartKeyInfo copyObject() {
    return new OmMultipartKeyInfo(this);
  }

}
