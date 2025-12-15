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
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedPrefixInfo;

/**
 * Wrapper class for Ozone prefix path info, currently mainly target for ACL but
 * can be extended for other OzFS optimizations in future.
 */
// TODO: support Auditable interface
@Immutable
public final class OmPrefixInfo extends WithObjectID implements CopyObject<OmPrefixInfo> {
  private static final Codec<OmPrefixInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(PersistedPrefixInfo.getDefaultInstance()),
      OmPrefixInfo::getFromProtobuf,
      OmPrefixInfo::getProtobuf,
      OmPrefixInfo.class);

  private final String name;
  private final ImmutableList<OzoneAcl> acls;

  private OmPrefixInfo(Builder b) {
    super(b);
    this.name = b.name;
    this.acls = b.acls.build();
  }

  public static Codec<OmPrefixInfo> getCodec() {
    return CODEC;
  }

  /**
   * Returns the ACL's associated with this prefix.
   * @return {@literal List<OzoneAcl>}
   */
  public List<OzoneAcl> getAcls() {
    return acls;
  }

  /**
   * Returns the name of the prefix path.
   * @return name of the prefix path.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns new builder class that builds a OmPrefixInfo.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for OmPrefixInfo.
   */
  public static class Builder extends WithObjectID.Builder<OmPrefixInfo> {
    private String name;
    private final AclListBuilder acls;

    public Builder() {
      //Default values
      this(AclListBuilder.empty());
    }

    private Builder(AclListBuilder acls) {
      this.acls = acls;
    }

    public Builder(OmPrefixInfo obj) {
      super(obj);
      this.acls = AclListBuilder.of(obj.acls);
      this.name = obj.name;
    }

    public Builder setAcls(List<OzoneAcl> listOfAcls) {
      acls.set(listOfAcls);
      return this;
    }

    public Builder addAcls(List<OzoneAcl> listOfAcls) {
      acls.addAll(listOfAcls);
      return this;
    }

    public Builder addAcl(OzoneAcl acl) {
      acls.add(acl);
      return this;
    }

    public Builder removeAcl(OzoneAcl acl) {
      acls.remove(acl);
      return this;
    }

    public boolean isAclsChanged() {
      return acls.isChanged();
    }

    public Builder setName(String n) {
      this.name = n;
      return this;
    }

    @Override
    public OmPrefixInfo.Builder addAllMetadata(
        Map<String, String> additionalMetadata) {
      super.addAllMetadata(additionalMetadata);
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
    protected void validate() {
      super.validate();
      Objects.requireNonNull(name, "name == null");
    }

    @Override
    protected OmPrefixInfo buildObject() {
      return new OmPrefixInfo(this);
    }
  }

  /**
   * Creates PrefixInfo protobuf from OmPrefixInfo.
   */
  public PersistedPrefixInfo getProtobuf() {
    PersistedPrefixInfo.Builder pib =
        PersistedPrefixInfo.newBuilder().setName(name)
        .addAllMetadata(KeyValueUtil.toProtobuf(getMetadata()))
        .setObjectID(getObjectID())
        .setUpdateID(getUpdateID());
    if (acls != null) {
      pib.addAllAcls(OzoneAclStorageUtil.toProtobuf(acls));
    }
    return pib.build();
  }

  /**
   * Parses PrefixInfo protobuf and creates OmPrefixInfo Builder.
   * @param prefixInfo
   * @return Builder instance
   */
  public static Builder builderFromProtobuf(PersistedPrefixInfo prefixInfo) {
    OmPrefixInfo.Builder opib = OmPrefixInfo.newBuilder()
        .setName(prefixInfo.getName());
    if (prefixInfo.getMetadataList() != null) {
      opib.addAllMetadata(KeyValueUtil
          .getFromProtobuf(prefixInfo.getMetadataList()));
    }
    if (prefixInfo.getAclsList() != null) {
      opib.setAcls(OzoneAclStorageUtil.fromProtobuf(prefixInfo.getAclsList()));
    }

    if (prefixInfo.hasObjectID()) {
      opib.setObjectID(prefixInfo.getObjectID());
    }

    if (prefixInfo.hasUpdateID()) {
      opib.setUpdateID(prefixInfo.getUpdateID());
    }
    return opib;
  }

  /**
   * Parses PrefixInfo protobuf and creates OmPrefixInfo.
   * @param prefixInfo
   * @return instance of OmPrefixInfo
   */
  public static OmPrefixInfo getFromProtobuf(PersistedPrefixInfo prefixInfo) {
    return builderFromProtobuf(prefixInfo).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OmPrefixInfo that = (OmPrefixInfo) o;
    return name.equals(that.name) &&
        Objects.equals(acls, that.acls) &&
        Objects.equals(getMetadata(), that.getMetadata()) &&
        getObjectID() == that.getObjectID() &&
        getUpdateID() == that.getUpdateID();
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, acls, getMetadata(), getObjectID(), getUpdateID());
  }

  @Override
  public String toString() {
    return "OmPrefixInfo{" +
        "name='" + name + '\'' +
        ", acls=" + acls +
        ", metadata=" + getMetadata() +
        ", objectID=" + getObjectID() +
        ", updateID=" + getUpdateID() +
        '}';
  }

  @Override
  public OmPrefixInfo copyObject() {
    return toBuilder().build();
  }

  public Builder toBuilder() {
    return new Builder(this);
  }
}
