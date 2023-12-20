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

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DirectoryInfo;

import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This class represents the directory information by keeping each component
 * in the user given path and a pointer to its parent directory element in the
 * path. Also, it stores directory node related metdata details.
 */
public class OmDirectoryInfo extends WithParentObjectId
    implements CopyObject<OmDirectoryInfo> {
  private static final Codec<OmDirectoryInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(DirectoryInfo.getDefaultInstance()),
      OmDirectoryInfo::getFromProtobuf,
      OmDirectoryInfo::getProtobuf);

  public static Codec<OmDirectoryInfo> getCodec() {
    return CODEC;
  }

  private final String name; // directory name

  private final long creationTime;
  private final long modificationTime;

  private final List<OzoneAcl> acls;

  public OmDirectoryInfo(Builder builder) {
    this.name = builder.name;
    this.acls = builder.acls;
    this.metadata = builder.metadata;
    this.objectID = builder.objectID;
    this.updateID = builder.updateID;
    this.parentObjectID = builder.parentObjectID;
    this.creationTime = builder.creationTime;
    this.modificationTime = builder.modificationTime;
  }

  /**
   * Returns new builder class that builds a OmPrefixInfo.
   *
   * @return Builder
   */
  public static OmDirectoryInfo.Builder newBuilder() {
    return new OmDirectoryInfo.Builder();
  }

  /**
   * Builder for Directory Info.
   */
  public static class Builder {
    private long parentObjectID; // pointer to parent directory

    private long objectID;
    private long updateID;

    private String name;

    private long creationTime;
    private long modificationTime;

    private final List<OzoneAcl> acls;
    private final Map<String, String> metadata;

    public Builder() {
      //Default values
      this.acls = new LinkedList<>();
      this.metadata = new HashMap<>();
    }

    public Builder setParentObjectID(long parentObjectId) {
      this.parentObjectID = parentObjectId;
      return this;
    }

    public Builder setObjectID(long objectId) {
      this.objectID = objectId;
      return this;
    }

    public Builder setUpdateID(long updateId) {
      this.updateID = updateId;
      return this;
    }

    public Builder setName(String dirName) {
      this.name = dirName;
      return this;
    }

    public Builder setCreationTime(long newCreationTime) {
      this.creationTime = newCreationTime;
      return this;
    }

    public Builder setModificationTime(long newModificationTime) {
      this.modificationTime = newModificationTime;
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

    public Builder addMetadata(String key, String value) {
      metadata.put(key, value);
      return this;
    }

    public Builder addAllMetadata(Map<String, String> additionalMetadata) {
      if (additionalMetadata != null) {
        metadata.putAll(additionalMetadata);
      }
      return this;
    }

    public OmDirectoryInfo build() {
      return new OmDirectoryInfo(this);
    }
  }

  @Override
  public String toString() {
    return getPath() + ":" + getObjectID();
  }

  public long getParentObjectID() {
    return parentObjectID;
  }

  public String getPath() {
    return getParentObjectID() + OzoneConsts.OM_KEY_PREFIX + getName();
  }

  public String getName() {
    return name;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public List<OzoneAcl> getAcls() {
    return acls;
  }

  /**
   * Creates DirectoryInfo protobuf from OmDirectoryInfo.
   */
  public DirectoryInfo getProtobuf() {
    final DirectoryInfo.Builder pib =
            DirectoryInfo.newBuilder().setName(name)
                    .setCreationTime(creationTime)
                    .setModificationTime(modificationTime)
                    .addAllMetadata(KeyValueUtil.toProtobuf(metadata))
                    .setObjectID(objectID)
                    .setUpdateID(updateID)
                    .setParentID(parentObjectID);
    if (acls != null) {
      pib.addAllAcls(OzoneAclUtil.toProtobuf(acls));
    }
    return pib.build();
  }

  /**
   * Parses DirectoryInfo protobuf and creates OmPrefixInfo.
   * @param dirInfo
   * @return instance of OmDirectoryInfo
   */
  public static OmDirectoryInfo getFromProtobuf(DirectoryInfo dirInfo) {
    OmDirectoryInfo.Builder opib = OmDirectoryInfo.newBuilder()
            .setName(dirInfo.getName())
            .setCreationTime(dirInfo.getCreationTime())
            .setModificationTime(dirInfo.getModificationTime())
            .setAcls(OzoneAclUtil.fromProtobuf(dirInfo.getAclsList()));
    if (dirInfo.getMetadataList() != null) {
      opib.addAllMetadata(KeyValueUtil
              .getFromProtobuf(dirInfo.getMetadataList()));
    }
    if (dirInfo.hasObjectID()) {
      opib.setObjectID(dirInfo.getObjectID());
    }
    if (dirInfo.hasParentID()) {
      opib.setParentObjectID(dirInfo.getParentID());
    }
    if (dirInfo.hasUpdateID()) {
      opib.setUpdateID(dirInfo.getUpdateID());
    }
    return opib.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OmDirectoryInfo omDirInfo = (OmDirectoryInfo) o;
    return creationTime == omDirInfo.creationTime &&
            modificationTime == omDirInfo.modificationTime &&
            name.equals(omDirInfo.name) &&
            Objects.equals(metadata, omDirInfo.metadata) &&
            Objects.equals(acls, omDirInfo.acls) &&
            objectID == omDirInfo.objectID &&
            updateID == omDirInfo.updateID &&
            parentObjectID == omDirInfo.parentObjectID;
  }

  @Override
  public int hashCode() {
    return Objects.hash(objectID, parentObjectID, name);
  }

  /**
   * Return a new copy of the object.
   */
  @Override
  public OmDirectoryInfo copyObject() {
    OmDirectoryInfo.Builder builder = new Builder()
            .setName(name)
            .setCreationTime(creationTime)
            .setModificationTime(modificationTime)
            .setParentObjectID(parentObjectID)
            .setObjectID(objectID)
            .setUpdateID(updateID);

    acls.forEach(acl -> builder.addAcl(new OzoneAcl(acl.getType(),
            acl.getName(), (BitSet) acl.getAclBitSet().clone(),
            acl.getAclScope())));

    if (metadata != null) {
      builder.addAllMetadata(metadata);
    }

    return builder.build();
  }
}
