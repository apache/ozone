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

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * This class represents multipart upload information for a key, which holds
 * upload part information of the key.
 */
public final class OmMultipartKeyInfo extends WithObjectID {
  private static final Codec<OmMultipartKeyInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(MultipartKeyInfo.getDefaultInstance()),
      OmMultipartKeyInfo::getFromProto,
      OmMultipartKeyInfo::getProto);

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

    private final List<PartKeyInfo> sorted;

    PartKeyInfoMap(List<PartKeyInfo> sorted) {
      this.sorted = Collections.unmodifiableList(sorted);
    }

    PartKeyInfoMap(SortedMap<Integer, PartKeyInfo> sorted) {
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

  private final String uploadID;
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
  private long parentID;

  /**
   * Construct OmMultipartKeyInfo object which holds multipart upload
   * information for a key.
   */
  @SuppressWarnings("parameternumber")
  private OmMultipartKeyInfo(String id, long creationTime,
      ReplicationConfig replicationConfig,
      PartKeyInfoMap sortedMap, long objectID, long updateID,
      long parentObjId) {
    this.uploadID = id;
    this.creationTime = creationTime;
    this.replicationConfig = replicationConfig;
    this.partKeyInfoMap = sortedMap;
    this.objectID = objectID;
    this.updateID = updateID;
    this.parentID = parentObjId;
  }

  /**
   * Construct OmMultipartKeyInfo object which holds multipart upload
   * information for a key.
   */
  @SuppressWarnings("parameternumber")
  private OmMultipartKeyInfo(String id, long creationTime,
      ReplicationConfig replicationConfig,
      SortedMap<Integer, PartKeyInfo> list, long objectID, long updateID,
      long parentObjId) {
    this(id, creationTime, replicationConfig, new PartKeyInfoMap(list),
        objectID, updateID, parentObjId);
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

  public PartKeyInfoMap getPartKeyInfoMap() {
    return partKeyInfoMap;
  }

  public void addPartKeyInfo(PartKeyInfo partKeyInfo) {
    this.partKeyInfoMap = PartKeyInfoMap.put(partKeyInfo, partKeyInfoMap);
  }

  public PartKeyInfo getPartKeyInfo(int partNumber) {
    return partKeyInfoMap.get(partNumber);
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  /**
   * Builder of OmMultipartKeyInfo.
   */
  public static class Builder {
    private String uploadID;
    private long creationTime;
    private ReplicationConfig replicationConfig;
    private TreeMap<Integer, PartKeyInfo> partKeyInfoList;
    private long objectID;
    private long updateID;
    private long parentID;

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

    public Builder setObjectID(long obId) {
      this.objectID = obId;
      return this;
    }

    public Builder setUpdateID(long id) {
      this.updateID = id;
      return this;
    }

    public Builder setParentID(long parentObjId) {
      this.parentID = parentObjId;
      return this;
    }

    public OmMultipartKeyInfo build() {
      return new OmMultipartKeyInfo(uploadID, creationTime, replicationConfig,
              partKeyInfoList, objectID, updateID, parentID);
    }
  }

  /**
   * Construct OmMultipartInfo from MultipartKeyInfo proto object.
   * @param multipartKeyInfo
   * @return OmMultipartKeyInfo
   */
  public static OmMultipartKeyInfo getFromProto(
      MultipartKeyInfo multipartKeyInfo) {
    final SortedMap<Integer, PartKeyInfo> list = new TreeMap<>();
    multipartKeyInfo.getPartKeyInfoListList().forEach(partKeyInfo ->
        list.put(partKeyInfo.getPartNumber(), partKeyInfo));

    final ReplicationConfig replicationConfig = ReplicationConfig.fromProto(
        multipartKeyInfo.getType(),
        multipartKeyInfo.getFactor(),
        multipartKeyInfo.getEcReplicationConfig()
    );

    return new OmMultipartKeyInfo(multipartKeyInfo.getUploadID(),
        multipartKeyInfo.getCreationTime(), replicationConfig,
        list, multipartKeyInfo.getObjectID(),
        multipartKeyInfo.getUpdateID(), multipartKeyInfo.getParentID());
  }

  /**
   * Construct MultipartKeyInfo from this object.
   * @return MultipartKeyInfo
   */
  public MultipartKeyInfo getProto() {
    MultipartKeyInfo.Builder builder = MultipartKeyInfo.newBuilder()
        .setUploadID(uploadID)
        .setCreationTime(creationTime)
        .setType(replicationConfig.getReplicationType())
        .setObjectID(objectID)
        .setUpdateID(updateID)
        .setParentID(parentID);

    if (replicationConfig instanceof ECReplicationConfig) {
      ECReplicationConfig ecConf = (ECReplicationConfig) replicationConfig;
      builder.setEcReplicationConfig(ecConf.toProto());
    } else {
      builder.setFactor(ReplicationConfig.getLegacyFactor(replicationConfig));
    }

    builder.addAllPartKeyInfoList(partKeyInfoMap);
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
    // PartKeyInfoMap is an immutable data structure. Whenever a PartKeyInfo
    // is added, it returns a new shallow copy of the PartKeyInfoMap Object
    // so here we can directly pass in partKeyInfoMap
    return new OmMultipartKeyInfo(uploadID, creationTime, replicationConfig,
        partKeyInfoMap, objectID, updateID, parentID);
  }

}
