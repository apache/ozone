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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedPrefixInfo;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Wrapper class for Ozone prefix path info, currently mainly target for ACL but
 * can be extended for other OzFS optimizations in future.
 */
// TODO: support Auditable interface
public final class OmPrefixInfo extends WithObjectID implements CopyObject<OmPrefixInfo> {
  private static final Codec<OmPrefixInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(PersistedPrefixInfo.getDefaultInstance()),
      OmPrefixInfo::getFromProtobuf,
      OmPrefixInfo::getProtobuf,
      OmPrefixInfo.class);

  public static Codec<OmPrefixInfo> getCodec() {
    return CODEC;
  }

  private final String name;
  private final CopyOnWriteArrayList<OzoneAcl> acls;

  private OmPrefixInfo(Builder b) {
    super(b);
    name = b.name;
    acls = new CopyOnWriteArrayList<>(b.acls);
  }

  /**
   * Returns the ACL's associated with this prefix.
   * @return {@literal List<OzoneAcl>}
   */
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
  public static OmPrefixInfo.Builder newBuilder() {
    return new OmPrefixInfo.Builder();
  }

  /**
   * Builder for OmPrefixInfo.
   */
  public static class Builder extends WithObjectID.Builder {
    private String name;
    private final List<OzoneAcl> acls;

    public Builder() {
      //Default values
      this.acls = new LinkedList<>();
    }

    public Builder(OmPrefixInfo obj) {
      super(obj);
      setName(obj.name);
      acls = new ArrayList<>(obj.getAcls());
    }

    public Builder setAcls(List<OzoneAcl> listOfAcls) {
      if (listOfAcls != null) {
        acls.addAll(listOfAcls);
      }
      return this;
    }

    public Builder setName(String n) {
      this.name = n;
      return this;
    }

    @Override
    public OmPrefixInfo.Builder addMetadata(String key, String value) {
      super.addMetadata(key, value);
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

    /**
     * Constructs the OmPrefixInfo.
     * @return instance of OmPrefixInfo.
     */
    public OmPrefixInfo build() {
      Preconditions.checkNotNull(name);
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
   * Parses PrefixInfo protobuf and creates OmPrefixInfo.
   * @param prefixInfo
   * @return instance of OmPrefixInfo
   */
  public static OmPrefixInfo getFromProtobuf(PersistedPrefixInfo prefixInfo) {
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

