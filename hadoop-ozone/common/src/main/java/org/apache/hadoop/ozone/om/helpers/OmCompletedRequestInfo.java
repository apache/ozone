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

import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used for storing info related to completed operations.
 * These are the subset of operations which mutate the filesystem such
 * as create volume/bucket/key/file/dir and associated delete/rename
 * events where appropriate.
 *
 * The initial use case for this data is to create an event notification
 * feed which users can subscribe to and get a history of such changes
 * but the data is designed to be provided in a generic means that could
 * be suitable for other use cases.
 *
 * Each successfully completion operation has an associated
 * OmCompletedRequestInfo entry the trxLogIndex, cmdType, volumeName, bucketName,
 * keyName, creationTime and operationArgs
 */
public final class OmCompletedRequestInfo implements CopyObject<OmCompletedRequestInfo> {
  public static final Logger LOG =
      LoggerFactory.getLogger(OmCompletedRequestInfo.class);

  private static final Codec<OmCompletedRequestInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(OzoneManagerProtocolProtos.CompletedRequestInfo.getDefaultInstance()),
      OmCompletedRequestInfo::getFromProtobuf,
      OmCompletedRequestInfo::getProtobuf,
      OmCompletedRequestInfo.class);

  private long trxLogIndex;
  private final Type cmdType;
  private final String volumeName;
  private final String bucketName;
  private final String keyName;
  private final long creationTime;
  private final OperationArgs opArgs;

  /**
   * Private constructor, constructed via builder.
   * @param trxLogIndex - trxLogIndex.
   * @param cmdType - comand type.
   * @param volumeName - volume name.
   * @param bucketName - bucket name.
   * @param keyName - key name.
   * @param creationTime - creation time.
   * @param opArgs - operation specifc arguments.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private OmCompletedRequestInfo(Builder builder) {
    this.trxLogIndex = builder.trxLogIndex;
    this.cmdType = builder.cmdType;
    this.volumeName = builder.volumeName;
    this.bucketName = builder.bucketName;
    this.keyName = builder.keyName;
    this.creationTime = builder.creationTime;
    this.opArgs = builder.opArgs;
  }

  public static Codec<OmCompletedRequestInfo> getCodec() {
    return CODEC;
  }

  public void setTrxLogIndex(long trxLogIndex) {
    this.trxLogIndex = trxLogIndex;
  }

  public long getTrxLogIndex() {
    return trxLogIndex;
  }

  public Type getCmdType() {
    return cmdType;
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

  public long getCreationTime() {
    return creationTime;
  }

  public OperationArgs getOpArgs() {
    return opArgs;
  }

  public static org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo.Builder
      newBuilder() {
    return new org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo.Builder();
  }

  public OmCompletedRequestInfo.Builder toBuilder() {
    return new Builder()
        .setTrxLogIndex(trxLogIndex)
        .setCmdType(cmdType)
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setCreationTime(creationTime)
        .setOpArgs(opArgs);
  }

  /**
   * Builder of OmCompletedRequestInfo.
   */
  public static class Builder {
    private long trxLogIndex;
    private Type cmdType;
    private String volumeName;
    private String bucketName;
    private String keyName;
    private long creationTime;
    private OperationArgs opArgs;

    public Builder() {
      // default values
    }

    public Builder setTrxLogIndex(long trxLogIndex) {
      this.trxLogIndex = trxLogIndex;
      return this;
    }

    public Builder setCmdType(Type cmdType) {
      this.cmdType = cmdType;
      return this;
    }

    public Builder setVolumeName(String volumeName) {
      this.volumeName = volumeName;
      return this;
    }

    public Builder setBucketName(String bucketName) {
      this.bucketName = bucketName;
      return this;
    }

    public Builder setKeyName(String keyName) {
      this.keyName = keyName;
      return this;
    }

    public Builder setCreationTime(long crTime) {
      this.creationTime = crTime;
      return this;
    }

    public Builder setOpArgs(OperationArgs opArgs) {
      this.opArgs = opArgs;
      return this;
    }

    public OmCompletedRequestInfo build() {
      Objects.requireNonNull(trxLogIndex, "trxLogIndex == null");
      Objects.requireNonNull(cmdType, "cmdType == null");
      Objects.requireNonNull(volumeName, "volumeName == null");
      Objects.requireNonNull(creationTime, "creationTime == null");
      Objects.requireNonNull(opArgs, "opArgs == null");

      return new OmCompletedRequestInfo(this);
    }
  }

  /**
   * Creates OmCompletedRequestInfo protobuf from OmCompletedRequestInfo.
   */
  public OzoneManagerProtocolProtos.CompletedRequestInfo getProtobuf() {
    OzoneManagerProtocolProtos.CompletedRequestInfo.Builder sib =
        OzoneManagerProtocolProtos.CompletedRequestInfo.newBuilder()
            .setTrxLogIndex(trxLogIndex)
            .setCmdType(cmdType)
            .setVolumeName(volumeName)
            .setCreationTime(creationTime);

    // can be null e.g. CreateVolume
    if (bucketName != null) {
      sib.setBucketName(bucketName);
    }

    // can be null e.g. CreateBucket
    if (keyName != null) {
      sib.setKeyName(keyName);
    }

    switch (cmdType) {
    case RenameKey:
      sib.setRenameKeyArgs(OzoneManagerProtocolProtos.RenameKeyOperationArgs.newBuilder()
          .setToKeyName(((OperationArgs.RenameKeyArgs) opArgs).getToKeyName())
          .build());
      break;
    case CreateFile:
      sib.setCreateFileArgs(OzoneManagerProtocolProtos.CreateFileOperationArgs.newBuilder()
          .setIsRecursive(((OperationArgs.CreateFileArgs) opArgs).isRecursive())
          .setIsOverwrite(((OperationArgs.CreateFileArgs) opArgs).isOverwrite())
          .build());
      break;
    case CreateVolume:
    case DeleteVolume:
    case CreateBucket:
    case DeleteBucket:
    case CreateKey:
    case DeleteKey:
    case CommitKey:
    case CreateDirectory:
      break;
    default:
      LOG.error("Unexpected cmdType={}", cmdType);
      break;
    }

    return sib.build();
  }

  /**
   * Parses OmCompletedRequestInfo protobuf and creates OmCompletedRequestInfo.
   * @param completedRequestInfoProto protobuf
   * @return instance of OmCompletedRequestInfo
   */
  public static OmCompletedRequestInfo getFromProtobuf(
      OzoneManagerProtocolProtos.CompletedRequestInfo completedRequestInfoProto) {

    OmCompletedRequestInfo.Builder osib = OmCompletedRequestInfo.newBuilder()
        .setTrxLogIndex(completedRequestInfoProto.getTrxLogIndex())
        .setCmdType(completedRequestInfoProto.getCmdType())
        .setVolumeName(completedRequestInfoProto.getVolumeName())
        .setBucketName(completedRequestInfoProto.getBucketName())
        .setKeyName(completedRequestInfoProto.getKeyName())
        .setCreationTime(completedRequestInfoProto.getCreationTime());

    switch (completedRequestInfoProto.getCmdType()) {
    case RenameKey:
      OzoneManagerProtocolProtos.RenameKeyOperationArgs renameArgs
          = (OzoneManagerProtocolProtos.RenameKeyOperationArgs) completedRequestInfoProto.getRenameKeyArgs();

      osib.setOpArgs(new OperationArgs.RenameKeyArgs(renameArgs.getToKeyName()));
      break;
    case CreateFile:
      OzoneManagerProtocolProtos.CreateFileOperationArgs createFileArgs
          = (OzoneManagerProtocolProtos.CreateFileOperationArgs) completedRequestInfoProto.getCreateFileArgs();

      osib.setOpArgs(new OperationArgs.CreateFileArgs(createFileArgs.getIsOverwrite(),
                                                      createFileArgs.getIsRecursive()));
      break;
    case CreateVolume:
    case DeleteVolume:
    case CreateBucket:
    case DeleteBucket:
    case CreateKey:
    case DeleteKey:
    case CommitKey:
    case CreateDirectory:
      osib.setOpArgs(new OperationArgs.NoArgs());
      break;
    default:
      LOG.error("Unexpected cmdType={}", completedRequestInfoProto.getCmdType());
      break;
    }
    return osib.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OmCompletedRequestInfo that = (OmCompletedRequestInfo) o;
    return trxLogIndex == that.trxLogIndex &&
        cmdType == that.cmdType &&
        creationTime == that.creationTime &&
        Objects.equals(volumeName, that.volumeName) &&
        Objects.equals(bucketName, that.bucketName) &&
        Objects.equals(keyName, that.keyName) &&
        opArgs.equals(that.opArgs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(trxLogIndex, cmdType, volumeName, bucketName,
        keyName, creationTime, opArgs);
  }

  /**
   * Return a new copy of the object.
   */
  @Override
  public OmCompletedRequestInfo copyObject() {
    return new Builder()
        .setTrxLogIndex(trxLogIndex)
        .setCmdType(cmdType)
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setCreationTime(creationTime)
        .setOpArgs(opArgs)
        .build();
  }

  @Override
  public String toString() {
    return "OmCompletedRequestInfo{" +
        "trxLogIndex: '" + trxLogIndex + '\'' +
        ", cmdType: '" + cmdType + '\'' +
        ", volumeName: '" + volumeName + '\'' +
        ", bucketName: '" + bucketName + '\'' +
        ", keyName: '" + keyName + '\'' +
        ", creationTime: '" + creationTime + '\'' +
        ", opArgs : '" + opArgs + '\'' +
        '}';
  }

  /**
   * OperationArgs - common base class for operations specific
   * parameters.
   */
  public abstract static class OperationArgs {

    /**
     * RenameKeyArgs.
     */
    public static class RenameKeyArgs extends OperationArgs {
      private final String toKeyName;

      public RenameKeyArgs(String toKeyName) {
        this.toKeyName = toKeyName;
      }

      public String getToKeyName() {
        return toKeyName;
      }

      @Override
      public String toString() {
        return "RenameKeyArgs{" +
          "toKeyName: '" + toKeyName + '\'' +
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
        RenameKeyArgs that = (RenameKeyArgs) o;
        return Objects.equals(toKeyName, that.toKeyName);
      }

      @Override
      public int hashCode() {
        return Objects.hash(toKeyName);
      }
    }

    /**
     * CreateFileArgs.
     */
    public static class CreateFileArgs extends OperationArgs {
      // hsync?
      private final boolean recursive;
      private final boolean overwrite;

      public CreateFileArgs(boolean recursive, boolean overwrite) {
        this.recursive = recursive;
        this.overwrite = overwrite;
      }

      public boolean isRecursive() {
        return recursive;
      }

      public boolean isOverwrite() {
        return overwrite;
      }

      @Override
      public String toString() {
        return "CreateFileArgs{" +
          "recursive: '" + recursive + '\'' +
          ", overwrite: '" + overwrite + '\'' +
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
        CreateFileArgs that = (CreateFileArgs) o;
        return recursive == that.recursive &&
            overwrite == that.overwrite;
      }

      @Override
      public int hashCode() {
        return Objects.hash(recursive, overwrite);
      }
    }

    /**
     * NoArgs.
     */
    public static class NoArgs extends OperationArgs {
      @Override
      public String toString() {
        return "NoArgs{}";
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        return o != null && getClass() == o.getClass();
      }

      @Override
      public int hashCode() {
        return 42;
      }
    }
  }
}
