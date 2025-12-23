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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DirectoryInfo;

/**
 * This class represents the directory information by keeping each component
 * in the user given path and a pointer to its parent directory element in the
 * path. Also, it stores directory node related metadata details.
 */
@Immutable
public final class OmDirectoryInfo extends WithParentObjectId {

  private static final Codec<OmDirectoryInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(DirectoryInfo.getDefaultInstance()),
      OmDirectoryInfo::getFromProtobuf,
      OmDirectoryInfo::getProtobuf,
      OmDirectoryInfo.class,
      DelegatedCodec.CopyType.SHALLOW);

  private final String name; // directory name
  private final String owner;

  private final long creationTime;
  private final long modificationTime;

  private final ImmutableList<OzoneAcl> acls;

  private OmDirectoryInfo(Builder builder) {
    super(builder);
    this.name = builder.name;
    this.owner = builder.owner;
    this.acls = builder.acls.build();
    this.creationTime = builder.creationTime;
    this.modificationTime = builder.modificationTime;
  }

  public static Codec<OmDirectoryInfo> getCodec() {
    return CODEC;
  }

  /** @return new {@code Builder} with default values */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** @return new {@code Builder} with values set from this {@code OmDirectoryInfo} */
  public Builder toBuilder() {
    return new Builder(this);
  }

  public static Builder builderFromOmKeyInfo(OmKeyInfo keyInfo) {
    return new Builder(keyInfo);
  }

  /**
   * Builder for Directory Info.
   */
  public static final class Builder extends WithParentObjectId.Builder<OmDirectoryInfo> {
    private String name;
    private String owner;

    private long creationTime;
    private long modificationTime;

    private final AclListBuilder acls;

    private Builder() {
      this.acls = AclListBuilder.empty();
    }

    private Builder(OmDirectoryInfo obj) {
      super(obj);
      this.name = obj.name;
      this.owner = obj.owner;
      this.creationTime = obj.creationTime;
      this.modificationTime = obj.modificationTime;
      this.acls = AclListBuilder.of(obj.acls);
    }

    private Builder(OmKeyInfo keyInfo) {
      super(keyInfo);
      this.name = keyInfo.getFileName();
      this.creationTime = keyInfo.getCreationTime();
      this.modificationTime = keyInfo.getModificationTime();
      this.acls = AclListBuilder.of(keyInfo.getAcls());
    }

    @Override
    public Builder setParentObjectID(long parentObjectId) {
      super.setParentObjectID(parentObjectId);
      return this;
    }

    @Override
    public Builder setObjectID(long objectId) {
      super.setObjectID(objectId);
      return this;
    }

    @Override
    public Builder setUpdateID(long updateId) {
      super.setUpdateID(updateId);
      return this;
    }

    public Builder setName(String dirName) {
      this.name = dirName;
      return this;
    }

    public Builder setOwner(String ownerName) {
      this.owner = ownerName;
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
      this.acls.addAll(listOfAcls);
      return this;
    }

    @Override
    public Builder addAllMetadata(Map<String, String> additionalMetadata) {
      super.addAllMetadata(additionalMetadata);
      return this;
    }

    @Override
    protected OmDirectoryInfo buildObject() {
      return new OmDirectoryInfo(this);
    }
  }

  @Override
  public String toString() {
    return getPath() + ":" + getObjectID();
  }

  public String getPath() {
    return getParentObjectID() + OzoneConsts.OM_KEY_PREFIX + getName();
  }

  public String getName() {
    return name;
  }

  public String getOwner() {
    return owner;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public ImmutableList<OzoneAcl> getAcls() {
    return acls;
  }

  /**
   * Creates DirectoryInfo protobuf from OmDirectoryInfo.
   */
  public DirectoryInfo getProtobuf() {
    final DirectoryInfo.Builder builder = DirectoryInfo.newBuilder()
        .setName(name)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .addAllAcls(OzoneAclUtil.toProtobuf(acls))
        .addAllMetadata(KeyValueUtil.toProtobuf(getMetadata()))
        .setObjectID(getObjectID())
        .setUpdateID(getUpdateID())
        .setParentID(getParentObjectID());
    if (owner != null) {
      builder.setOwnerName(owner);
    }
    return builder.build();
  }

  /**
   * Parses DirectoryInfo protobuf and creates OmDirectoryInfo Builder.
   * @return Builder instance
   */
  public static Builder builderFromProtobuf(DirectoryInfo dirInfo) {
    Builder builder = OmDirectoryInfo.newBuilder()
        .setName(dirInfo.getName())
        .setCreationTime(dirInfo.getCreationTime())
        .setModificationTime(dirInfo.getModificationTime())
        .setObjectID(dirInfo.getObjectID())
        .setUpdateID(dirInfo.getUpdateID())
        .setParentObjectID(dirInfo.getParentID())
        .setAcls(OzoneAclUtil.fromProtobuf(dirInfo.getAclsList()))
        .addAllMetadata(KeyValueUtil.getFromProtobuf(dirInfo.getMetadataList()));
    if (dirInfo.hasOwnerName()) {
      builder.setOwner(dirInfo.getOwnerName());
    }
    return builder;
  }

  /**
   * Parses DirectoryInfo protobuf and creates OmDirectoryInfo.
   * @return instance of OmDirectoryInfo
   */
  public static OmDirectoryInfo getFromProtobuf(DirectoryInfo dirInfo) {
    return builderFromProtobuf(dirInfo).build();
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
    return creationTime == omDirInfo.creationTime
        && modificationTime == omDirInfo.modificationTime
        && getObjectID() == omDirInfo.getObjectID()
        && getUpdateID() == omDirInfo.getUpdateID()
        && getParentObjectID() == omDirInfo.getParentObjectID()
        && Objects.equals(name, omDirInfo.name)
        && Objects.equals(owner, omDirInfo.owner)
        && Objects.equals(getMetadata(), omDirInfo.getMetadata())
        && Objects.equals(acls, omDirInfo.acls);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getObjectID(), getParentObjectID(), name);
  }
}
