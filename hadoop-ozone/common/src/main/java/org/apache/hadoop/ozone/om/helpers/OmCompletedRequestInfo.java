package org.apache.hadoop.ozone.om.helpers;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import java.time.format.DateTimeFormatter;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.ZoneId;

import java.util.HashMap;
import java.util.Objects;
import java.util.UUID;
import java.util.Map;
import java.util.LinkedHashMap;

import static org.apache.hadoop.hdds.HddsUtils.fromProtobuf;
import static org.apache.hadoop.hdds.HddsUtils.toProtobuf;

/**
 * This class is used for storing info related to completed operations.
 *
 * Each successfully completion operation has an associated
 * OmCompletedRequestInfo entry the trxLogIndex, op, volumeName, bucketName,
 * keyName and creationTime
 */
public final class OmCompletedRequestInfo implements Auditable, CopyObject<OmCompletedRequestInfo> {
  public static final Logger LOG =
      LoggerFactory.getLogger(OmCompletedRequestInfo.class);

  private static final Codec<OmCompletedRequestInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(OzoneManagerProtocolProtos.CompletedRequestInfo.getDefaultInstance()),
      OmCompletedRequestInfo::getFromProtobuf,
      OmCompletedRequestInfo::getProtobuf,
      OmCompletedRequestInfo.class);

  /**
   * OperationType enum
   */
  public enum OperationType {
    CREATE_KEY,
    RENAME_KEY,
    DELETE_KEY,
    COMMIT_KEY,
    CREATE_DIRECTORY,
    CREATE_FILE;
  }

  private static final long INVALID_TIMESTAMP = -1;

  private long trxLogIndex;
  private final String volumeName;
  private final String bucketName;
  private final String keyName;
  private final long creationTime;
  private final OperationArgs opArgs;

  /**
   * Private constructor, constructed via builder.
   * @param snapshotId - Snapshot UUID.
   * @param name - snapshot name.
   * @param volumeName - volume name.
   * @param bucketName - bucket name.
   * @param snapshotStatus - status: SNAPSHOT_ACTIVE, SNAPSHOT_DELETED
   * @param creationTime - Snapshot creation time.
   * @param deletionTime - Snapshot deletion time.
   * @param pathPreviousSnapshotId - Snapshot path previous snapshot id.
   * @param globalPreviousSnapshotId - Snapshot global previous snapshot id.
   * @param snapshotPath - Snapshot path, bucket .snapshot path.
   * @param checkpointDir - Snapshot checkpoint directory.
   * @param dbTxSequenceNumber - RDB latest transaction sequence number.
   * @param deepCleaned - To be deep cleaned status for snapshot.
   * @param referencedSize - Snapshot referenced size.
   * @param referencedReplicatedSize - Snapshot referenced size w/ replication.
   * @param exclusiveSize - Snapshot exclusive size.
   * @param exclusiveReplicatedSize - Snapshot exclusive size w/ replication.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private OmCompletedRequestInfo(long trxLogIndex,
                        String volumeName,
                        String bucketName,
                        String keyName,
                        long creationTime,
                        OperationArgs opArgs) {
    this.trxLogIndex = trxLogIndex;
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.creationTime = creationTime;
    this.opArgs = opArgs;
  }

  public static Codec<OmCompletedRequestInfo> getCodec() {
    return CODEC;
  }

  public void setTrxLogIndex(long trxLogIndex) {
    this.trxLogIndex = trxLogIndex;
  }

  // the db version of the key is left padded with 0s so that it can be
  // "seeked" in in lexigroaphical order
  // TODO: is this an appropriate key?
  public String getDbKey() {
    return StringUtils.leftPad(String.valueOf(trxLogIndex), 20, '0');
  }

  public long getTrxLogIndex() {
    return trxLogIndex;
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
      //Preconditions.checkNotNull(name);
      return new OmCompletedRequestInfo(
          trxLogIndex,
          volumeName,
          bucketName,
          keyName,
          creationTime,
          opArgs
      );
    }
  }

  /**
   * Creates OmCompletedRequestInfo protobuf from OmCompletedRequestInfo.
   */
  public OzoneManagerProtocolProtos.CompletedRequestInfo getProtobuf() {
    OzoneManagerProtocolProtos.CompletedRequestInfo.Builder sib =
        OzoneManagerProtocolProtos.CompletedRequestInfo.newBuilder()
            .setTrxLogIndex(trxLogIndex)
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setCreationTime(creationTime);

    switch (opArgs.getOperationType()) {
      case CREATE_KEY:
        sib.setCmdType(OzoneManagerProtocolProtos.Type.CreateKey);
        sib.setCreateKeyArgs(OzoneManagerProtocolProtos.CreateKeyOperationArgs.newBuilder()
            .build());
        break;
      case RENAME_KEY:
        sib.setCmdType(OzoneManagerProtocolProtos.Type.RenameKey);
        sib.setRenameKeyArgs(OzoneManagerProtocolProtos.RenameKeyOperationArgs.newBuilder()
            .setToKeyName(((OperationArgs.RenameKeyArgs) opArgs).getToKeyName())
            .build());
        break;
      case DELETE_KEY:
        sib.setCmdType(OzoneManagerProtocolProtos.Type.DeleteKey);
        sib.setDeleteKeyArgs(OzoneManagerProtocolProtos.DeleteKeyOperationArgs.newBuilder()
            .build());
        break;
      case COMMIT_KEY:
        sib.setCmdType(OzoneManagerProtocolProtos.Type.CommitKey);
        sib.setCommitKeyArgs(OzoneManagerProtocolProtos.CommitKeyOperationArgs.newBuilder()
            .build());
        break;
      case CREATE_DIRECTORY:
        sib.setCmdType(OzoneManagerProtocolProtos.Type.CreateDirectory);
        sib.setCreateDirectoryArgs(OzoneManagerProtocolProtos.CreateDirectoryOperationArgs.newBuilder()
            .build());
        break;
      case CREATE_FILE:
        sib.setCmdType(OzoneManagerProtocolProtos.Type.CreateFile);
        sib.setCreateFileArgs(OzoneManagerProtocolProtos.CreateFileOperationArgs.newBuilder()
            .setIsRecursive(((OperationArgs.CreateFileArgs) opArgs).isRecursive())
            .setIsOverwrite(((OperationArgs.CreateFileArgs) opArgs).isOverwrite())
            .build());
        break;
      default:
        LOG.error("Unexpected operationType={}", opArgs.getOperationType());
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
        .setVolumeName(completedRequestInfoProto.getVolumeName())
        .setBucketName(completedRequestInfoProto.getBucketName())
        .setKeyName(completedRequestInfoProto.getKeyName())
        .setCreationTime(completedRequestInfoProto.getCreationTime());

    switch (completedRequestInfoProto.getCmdType()) {
      case CreateKey:
        osib.setOpArgs(new OperationArgs.CreateKeyArgs());
        break;
      case RenameKey:
        OzoneManagerProtocolProtos.RenameKeyOperationArgs renameArgs
            = (OzoneManagerProtocolProtos.RenameKeyOperationArgs) completedRequestInfoProto.getRenameKeyArgs();

        osib.setOpArgs(new OperationArgs.RenameKeyArgs(renameArgs.getToKeyName()));
        break;
      case DeleteKey:
        osib.setOpArgs(new OperationArgs.DeleteKeyArgs());
        break;
      case CommitKey:
        osib.setOpArgs(new OperationArgs.CommitKeyArgs());
        break;
      case CreateDirectory:
        osib.setOpArgs(new OperationArgs.CreateDirectoryArgs());
        break;
      case CreateFile:
        OzoneManagerProtocolProtos.CreateFileOperationArgs createFileArgs
            = (OzoneManagerProtocolProtos.CreateFileOperationArgs) completedRequestInfoProto.getCreateFileArgs();

        osib.setOpArgs(new OperationArgs.CreateFileArgs(createFileArgs.getIsOverwrite(), createFileArgs.getIsRecursive()));
        break;
      default:
        LOG.error("Unexpected cmdType={}", completedRequestInfoProto.getCmdType());
        break;
    }

    return osib.build();
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    //auditMap.put(OzoneConsts.VOLUME, getVolumeName());
    //auditMap.put(OzoneConsts.BUCKET, getBucketName());
    //auditMap.put(OzoneConsts.OM_SNAPSHOT_NAME, this.name);
    return auditMap;
  }

  /**
   * Factory for making standard instance.
   */
  /*
  public static OmCompletedRequestInfo newInstance(long trxLogIndex,
                                          Operation op,
                                          long creationTime) {
    OmCompletedRequestInfo.Builder builder = new OmCompletedRequestInfo.Builder();
    builder.setTrxLogIndex(trxLogIndex)
        .setOp(op)
        .setCreationTime(creationTime);

    return builder.build();
  }
  */

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
        creationTime == that.creationTime &&
        volumeName.equals(that.volumeName) &&
        bucketName.equals(that.bucketName) &&
        keyName.equals(that.keyName) &&
        volumeName == that.bucketName &&
        opArgs.equals(that.opArgs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(trxLogIndex, volumeName, bucketName,
        keyName, creationTime, opArgs);
  }

  /**
   * Return a new copy of the object.
   */
  @Override
  public OmCompletedRequestInfo copyObject() {
    return new Builder()
        .setTrxLogIndex(trxLogIndex)
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
        ", volumeName: '" + volumeName + '\'' +
        ", bucketName: '" + bucketName + '\'' +
        ", keyName: '" + keyName + '\'' +
        ", creationTime: '" + creationTime + '\'' +
        ", opArgs : '" + opArgs + '\'' +
        '}';
  }

  public static abstract class OperationArgs {

    public abstract OperationType getOperationType();

    public static class CreateKeyArgs extends OperationArgs {

      @Override
      public OperationType getOperationType() {
        return OperationType.CREATE_KEY;
      }

      @Override
      public String toString() {
        return "CreateKeyArgs{}";
      }
    }

    public static class RenameKeyArgs extends OperationArgs {
      private final String toKeyName;

      public RenameKeyArgs(String toKeyName) {
        this.toKeyName = toKeyName;
      }

      @Override
      public OperationType getOperationType() {
        return OperationType.RENAME_KEY;
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
    }

    public static class DeleteKeyArgs extends OperationArgs {

      @Override
      public OperationType getOperationType() {
        return OperationType.DELETE_KEY;
      }

      @Override
      public String toString() {
        return "DeleteKeyArgs{}";
      }
    }

    public static class CommitKeyArgs extends OperationArgs {

      @Override
      public OperationType getOperationType() {
        return OperationType.COMMIT_KEY;
      }

      @Override
      public String toString() {
        return "CommitKeyArgs{}";
      }
    }

    public static class CreateDirectoryArgs extends OperationArgs {

      @Override
      public OperationType getOperationType() {
        return OperationType.CREATE_DIRECTORY;
      }

      @Override
      public String toString() {
        return "CreateDirectoryArgs{}";
      }
    }

    public static class CreateFileArgs extends OperationArgs {
      // hsync?
      private final boolean recursive;
      private final boolean overwrite;

      public CreateFileArgs(boolean recursive, boolean overwrite) {
        this.recursive = recursive;
        this.overwrite = overwrite;
      }

      @Override
      public OperationType getOperationType() {
        return OperationType.CREATE_FILE;
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
    }
  }
}
