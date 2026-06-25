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

package org.apache.hadoop.ozone.security.acl;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneObj.StoreType.valueOf;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneObj.ObjectType;

/**
 * Class representing an unique ozone object.
 * */
public abstract class OzoneObj implements IOzoneObj {

  private final ResourceType resType;

  private final StoreType storeType;

  OzoneObj(ResourceType resType, StoreType storeType) {

    Objects.requireNonNull(resType, "resType == null");
    Objects.requireNonNull(storeType, "storeType == null");
    this.resType = resType;
    this.storeType = storeType;
  }

  public static OzoneManagerProtocolProtos.OzoneObj toProtobuf(OzoneObj obj) {
    return OzoneManagerProtocolProtos.OzoneObj.newBuilder()
        .setResType(ObjectType.valueOf(obj.getResourceType().name()))
        .setStoreType(valueOf(obj.getStoreType().name()))
        .setPath(obj.getPath()).build();
  }

  public ResourceType getResourceType() {
    return resType;
  }

  @Override
  public String toString() {
    return "OzoneObj{" +
        "resType=" + resType +
        ", storeType=" + storeType +
        ", path='" + getPath() + '\'' +
        '}';
  }

  public StoreType getStoreType() {
    return storeType;
  }

  public abstract String getVolumeName();

  public abstract String getBucketName();

  public abstract String getKeyName();

  public abstract OzonePrefixPath getOzonePrefixPathViewer();

  /**
   * Get PrefixName.
   * A prefix name is like a key name under the bucket but
   * are mainly used for ACL for now and persisted into a separate prefix table.
   *
   * @return prefix name.
   */
  public abstract String getPrefixName();

  /**
   * Get full path of a key or prefix including volume and bucket.
   * @return full path of a key or prefix.
   */
  public abstract String getPath();

  /**
   * Ozone Objects supported for ACL.
   */
  public enum ResourceType {
    VOLUME(OzoneConsts.VOLUME),
    BUCKET(OzoneConsts.BUCKET),
    KEY(OzoneConsts.KEY),
    PREFIX(OzoneConsts.PREFIX);

    /**
     * String value for this Enum.
     */
    private final String value;

    @Override
    public String toString() {
      return value;
    }

    ResourceType(String resType) {
      value = resType;
    }
  }

  /**
   * Ozone Objects supported for ACL.
   */
  public enum StoreType {
    OZONE(OzoneConsts.OZONE),
    S3(OzoneConsts.S3);

    /**
     * String value for this Enum.
     */
    private final String value;

    @Override
    public String toString() {
      return value;
    }

    StoreType(String objType) {
      value = objType;
    }
  }

  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.RESOURCE_TYPE, this.getResourceType().value);
    auditMap.put(OzoneConsts.STORAGE_TYPE, this.getStoreType().value);
    auditMap.put(OzoneConsts.VOLUME, this.getVolumeName());
    auditMap.put(OzoneConsts.BUCKET, this.getBucketName());
    auditMap.put(OzoneConsts.KEY, this.getKeyName());
    return auditMap;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final OzoneObj that = (OzoneObj) o;
    return resType == that.resType && storeType == that.storeType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(resType, storeType);
  }
}
