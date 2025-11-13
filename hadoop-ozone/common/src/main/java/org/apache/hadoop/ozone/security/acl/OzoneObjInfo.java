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

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Class representing an ozone object.
 * It can be a volume with non-null volumeName {@literal (bucketName=null & name=null)}
 * or a bucket with non-null volumeName and bucketName (name=null)
 * or a key with non-null volumeName, bucketName and key name
 * (via getKeyName)
 * or a prefix with non-null volumeName, bucketName and prefix name
 * (via getPrefixName)
 */
public final class OzoneObjInfo extends OzoneObj {

  private final String volumeName;
  private final String bucketName;
  private final String name;

  private OzonePrefixPath ozonePrefixPath;

  /**
   *
   * @param resType
   * @param storeType
   * @param volumeName
   * @param bucketName
   * @param name - keyName/PrefixName
   */
  private OzoneObjInfo(ResourceType resType, StoreType storeType,
      String volumeName, String bucketName, String name,
      OzonePrefixPath ozonePrefixPath) {
    super(resType, storeType);
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.name = name;
    this.ozonePrefixPath = ozonePrefixPath;
  }

  @Override
  public String getPath() {
    switch (getResourceType()) {
    case VOLUME:
      return OZONE_URI_DELIMITER + getVolumeName();
    case BUCKET:
      return OZONE_URI_DELIMITER + getVolumeName()
          + OZONE_URI_DELIMITER + getBucketName();
    case KEY:
      return OZONE_URI_DELIMITER + getVolumeName()
          + OZONE_URI_DELIMITER + getBucketName()
          + OZONE_URI_DELIMITER + getKeyName();
    case PREFIX:
      return OZONE_URI_DELIMITER + getVolumeName()
          + OZONE_URI_DELIMITER + getBucketName()
          + OZONE_URI_DELIMITER + getPrefixName();
    default:
      throw new IllegalArgumentException("Unknown resource " +
        "type" + getResourceType());
    }
  }

  @Override
  public String getVolumeName() {
    return volumeName;
  }

  @Override
  public String getBucketName() {
    return bucketName;
  }

  @Override
  public String getKeyName() {
    return name;
  }

  @Override
  public String getPrefixName() {
    return name;
  }

  @Override
  public OzonePrefixPath getOzonePrefixPathViewer() {
    return ozonePrefixPath;
  }

  public static OzoneObjInfo fromProtobuf(OzoneManagerProtocolProtos.OzoneObj
      proto) {
    Builder builder = new Builder()
        .setResType(ResourceType.valueOf(proto.getResType().name()))
        .setStoreType(StoreType.valueOf(proto.getStoreType().name()));
    String[] tokens = StringUtils.split(proto.getPath(),
        OZONE_URI_DELIMITER, 3);
    if (tokens == null) {
      throw new IllegalArgumentException("Unexpected path:" + proto.getPath());
    }
    // Set volume name.
    switch (proto.getResType()) {
    case VOLUME:
      builder.setVolumeName(tokens[0]);
      break;
    case BUCKET:
      if (tokens.length < 2) {
        throw new IllegalArgumentException("Unexpected argument for " +
            "Ozone bucket. Path:" + proto.getPath());
      }
      builder.setVolumeName(tokens[0]);
      builder.setBucketName(tokens[1]);
      break;
    case KEY:
      if (tokens.length < 3) {
        throw new IllegalArgumentException("Unexpected argument for " +
            "Ozone key. Path:" + proto.getPath());
      }
      builder.setVolumeName(tokens[0]);
      builder.setBucketName(tokens[1]);
      builder.setKeyName(tokens[2]);
      break;
    case PREFIX:
      if (tokens.length < 3) {
        throw new IllegalArgumentException("Unexpected argument for " +
            "Ozone Prefix. Path:" + proto.getPath());
      }
      builder.setVolumeName(tokens[0]);
      builder.setBucketName(tokens[1]);
      builder.setPrefixName(tokens[2]);
      break;
    default:
      throw new IllegalArgumentException("Unexpected type for " +
          "Ozone key. Type:" + proto.getResType());
    }
    return builder.build();
  }

  /**
   * Inner builder class.
   */
  public static class Builder {

    private OzoneObj.ResourceType resType;
    private OzoneObj.StoreType storeType;
    private String volumeName;
    private String bucketName;
    private String name;
    private OzonePrefixPath ozonePrefixPath;

    public static Builder newBuilder() {
      return new Builder();
    }

    public static Builder getBuilder(ResourceType resType,
        StoreType storeType, String vol, String bucket, String key) {
      return OzoneObjInfo.Builder.newBuilder()
          .setResType(resType)
          .setStoreType(storeType)
          .setVolumeName(vol)
          .setBucketName(bucket)
          .setKeyName(key);
    }

    public static Builder fromKeyArgs(OmKeyArgs args) {
      return new Builder()
          .setVolumeName(args.getVolumeName())
          .setBucketName(args.getBucketName())
          .setKeyName(args.getKeyName())
          .setResType(ResourceType.KEY);
    }

    public static Builder fromOzoneObj(OzoneObj obj) {
      return new Builder()
          .setVolumeName(obj.getVolumeName())
          .setBucketName(obj.getBucketName())
          .setKeyName(obj.getKeyName())
          .setResType(obj.getResourceType())
          .setStoreType(obj.getStoreType())
          .setOzonePrefixPath(obj.getOzonePrefixPathViewer());
    }

    public Builder setResType(OzoneObj.ResourceType res) {
      this.resType = res;
      return this;
    }

    public Builder setStoreType(OzoneObj.StoreType store) {
      this.storeType = store;
      return this;
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
      this.name = key;
      return this;
    }

    public Builder setPrefixName(String prefix) {
      this.name = prefix;
      return this;
    }

    public Builder setOzonePrefixPath(OzonePrefixPath ozonePrefixPathViewer) {
      this.ozonePrefixPath = ozonePrefixPathViewer;
      return this;
    }

    public OzoneObjInfo build() {
      return new OzoneObjInfo(resType, storeType, volumeName, bucketName,
          name, ozonePrefixPath);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!super.equals(o)) {
      return false;
    }

    final OzoneObjInfo that = (OzoneObjInfo) o;
    return Objects.equals(volumeName, that.volumeName) && Objects.equals(bucketName, that.bucketName) &&
        Objects.equals(name, that.name) && Objects.equals(ozonePrefixPath, that.ozonePrefixPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), volumeName, bucketName, name, ozonePrefixPath);
  }
}
