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
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotStatusProto;

import com.google.common.base.Preconditions;

import java.time.format.DateTimeFormatter;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.ZoneId;

import java.util.Objects;
import java.util.UUID;
import java.util.Map;
import java.util.LinkedHashMap;

import static org.apache.hadoop.hdds.HddsUtils.fromProtobuf;
import static org.apache.hadoop.hdds.HddsUtils.toProtobuf;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * This class is used for storing info related to Snapshots.
 *
 * Each snapshot created has an associated SnapshotInfo entry
 * containing the snapshotid, snapshot path,
 * snapshot checkpoint directory, previous snapshotid
 * for the snapshot path & global amongst other necessary fields.
 */
public final class SnapshotInfo implements Auditable {
  private static final Codec<SnapshotInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(OzoneManagerProtocolProtos.SnapshotInfo.class),
      SnapshotInfo::getFromProtobuf,
      SnapshotInfo::getProtobuf,
      // FIXME: HDDS-8665 Deep copy will cause failures
      //        - TestOMSnapshotDeleteRequest           NullPointerException
      //        - TestOMSnapshotPurgeRequestAndResponse AssertionFailedError
      DelegatedCodec.CopyType.SHALLOW);

  public static Codec<SnapshotInfo> getCodec() {
    return CODEC;
  }

  /**
   * SnapshotStatus enum composed of
   * active, deleted and reclaimed statues.
   */
  public enum SnapshotStatus {
    SNAPSHOT_ACTIVE,
    SNAPSHOT_DELETED,
    SNAPSHOT_RECLAIMED;

    public static final SnapshotStatus DEFAULT = SNAPSHOT_ACTIVE;

    public SnapshotStatusProto toProto() {
      switch (this) {
      case SNAPSHOT_ACTIVE:
        return SnapshotStatusProto.SNAPSHOT_ACTIVE;
      case SNAPSHOT_DELETED:
        return SnapshotStatusProto.SNAPSHOT_DELETED;
      case SNAPSHOT_RECLAIMED:
        return SnapshotStatusProto.SNAPSHOT_RECLAIMED;
      default:
        throw new IllegalStateException(
            "BUG: missing valid SnapshotStatus, found status=" + this);
      }
    }

    public static SnapshotStatus valueOf(SnapshotStatusProto status) {
      switch (status) {
      case SNAPSHOT_ACTIVE:
        return SNAPSHOT_ACTIVE;
      case SNAPSHOT_DELETED:
        return SNAPSHOT_DELETED;
      case SNAPSHOT_RECLAIMED:
        return SNAPSHOT_RECLAIMED;
      default:
        throw new IllegalStateException(
            "BUG: missing valid SnapshotStatus, found status=" + status);
      }
    }
  }

  private static final String SEPARATOR = "-";
  private static final long INVALID_TIMESTAMP = -1;
  private static final UUID INITIAL_SNAPSHOT_ID = UUID.randomUUID();

  private final UUID snapshotId;
  private String name;
  private String volumeName;
  private String bucketName;
  private SnapshotStatus snapshotStatus;
  private final long creationTime;
  private long deletionTime;
  private UUID pathPreviousSnapshotId;
  private UUID globalPreviousSnapshotId;
  private String snapshotPath; // snapshot mask
  private String checkpointDir;
  /**
   * RocksDB's transaction sequence number at the time of checkpoint creation.
   */
  private long dbTxSequenceNumber;

  /**
   * Private constructor, constructed via builder.
   * @param snapshotId - Snapshot UUID.
   * @param name - snapshot name.
   * @param volumeName - volume name.
   * @param bucketName - bucket name.
   * @param snapshotStatus - status: SNAPSHOT_ACTIVE, SNAPSHOT_DELETED,
   *                      SNAPSHOT_RECLAIMED
   * @param creationTime - Snapshot creation time.
   * @param deletionTime - Snapshot deletion time.
   * @param pathPreviousSnapshotId - Snapshot path previous snapshot id.
   * @param globalPreviousSnapshotId - Snapshot global previous snapshot id.
   * @param snapshotPath - Snapshot path, bucket .snapshot path.
   * @param checkpointDir - Snapshot checkpoint directory.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private SnapshotInfo(UUID snapshotId,
                       String name,
                       String volumeName,
                       String bucketName,
                       SnapshotStatus snapshotStatus,
                       long creationTime,
                       long deletionTime,
                       UUID pathPreviousSnapshotId,
                       UUID globalPreviousSnapshotId,
                       String snapshotPath,
                       String checkpointDir,
                       long dbTxSequenceNumber) {
    this.snapshotId = snapshotId;
    this.name = name;
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.snapshotStatus = snapshotStatus;
    this.creationTime = creationTime;
    this.deletionTime = deletionTime;
    this.pathPreviousSnapshotId = pathPreviousSnapshotId;
    this.globalPreviousSnapshotId = globalPreviousSnapshotId;
    this.snapshotPath = snapshotPath;
    this.checkpointDir = checkpointDir;
    this.dbTxSequenceNumber = dbTxSequenceNumber;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public void setSnapshotStatus(SnapshotStatus snapshotStatus) {
    this.snapshotStatus = snapshotStatus;
  }

  public void setDeletionTime(long delTime) {
    this.deletionTime = delTime;
  }

  public void setPathPreviousSnapshotId(UUID pathPreviousSnapshotId) {
    this.pathPreviousSnapshotId = pathPreviousSnapshotId;
  }

  public void setGlobalPreviousSnapshotId(UUID globalPreviousSnapshotId) {
    this.globalPreviousSnapshotId = globalPreviousSnapshotId;
  }

  public void setSnapshotPath(String snapshotPath) {
    this.snapshotPath = snapshotPath;
  }

  public void setCheckpointDir(String checkpointDir) {
    this.checkpointDir = checkpointDir;
  }

  public UUID getSnapshotId() {
    return snapshotId;
  }

  public String getName() {
    return name;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public SnapshotStatus getSnapshotStatus() {
    return snapshotStatus;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public long getDeletionTime() {
    return deletionTime;
  }

  public UUID getPathPreviousSnapshotId() {
    return pathPreviousSnapshotId;
  }

  public UUID getGlobalPreviousSnapshotId() {
    return globalPreviousSnapshotId;
  }

  public String getSnapshotPath() {
    return snapshotPath;
  }

  public String getCheckpointDir() {
    return checkpointDir;
  }

  public static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.Builder
      newBuilder() {
    return new org.apache.hadoop.ozone.om.helpers.SnapshotInfo.Builder();
  }

  public SnapshotInfo.Builder toBuilder() {
    return new SnapshotInfo.Builder()
        .setSnapshotId(snapshotId)
        .setName(name)
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setSnapshotStatus(snapshotStatus)
        .setCreationTime(creationTime)
        .setDeletionTime(deletionTime)
        .setPathPreviousSnapshotId(pathPreviousSnapshotId)
        .setGlobalPreviousSnapshotId(globalPreviousSnapshotId)
        .setSnapshotPath(snapshotPath)
        .setCheckpointDir(checkpointDir);
  }

  /**
   * Builder of SnapshotInfo.
   */
  public static class Builder {
    private UUID snapshotId;
    private String name;
    private String volumeName;
    private String bucketName;
    private SnapshotStatus snapshotStatus;
    private long creationTime;
    private long deletionTime;
    private UUID pathPreviousSnapshotId;
    private UUID globalPreviousSnapshotId;
    private String snapshotPath;
    private String checkpointDir;
    private long dbTxSequenceNumber;

    public Builder() {
      // default values
      this.snapshotStatus = SnapshotStatus.DEFAULT;
    }

    public Builder setSnapshotId(UUID snapshotId) {
      this.snapshotId = snapshotId;
      return this;
    }

    public Builder setName(String name) {
      this.name = name;
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

    public Builder setSnapshotStatus(SnapshotStatus snapshotStatus) {
      this.snapshotStatus = snapshotStatus;
      return this;
    }

    public Builder setCreationTime(long crTime) {
      this.creationTime = crTime;
      return this;
    }

    public Builder setDeletionTime(long delTime) {
      this.deletionTime = delTime;
      return this;
    }

    public Builder setPathPreviousSnapshotId(UUID pathPreviousSnapshotId) {
      this.pathPreviousSnapshotId = pathPreviousSnapshotId;
      return this;
    }

    public Builder setGlobalPreviousSnapshotId(UUID globalPreviousSnapshotId) {
      this.globalPreviousSnapshotId = globalPreviousSnapshotId;
      return this;
    }

    public Builder setSnapshotPath(String snapshotPath) {
      this.snapshotPath = snapshotPath;
      return this;
    }

    public Builder setCheckpointDir(String checkpointDir) {
      this.checkpointDir = checkpointDir;
      return this;
    }

    public Builder setDbTxSequenceNumber(long dbTxSequenceNumber) {
      this.dbTxSequenceNumber = dbTxSequenceNumber;
      return this;
    }

    public SnapshotInfo build() {
      Preconditions.checkNotNull(name);
      return new SnapshotInfo(
          snapshotId,
          name,
          volumeName,
          bucketName,
          snapshotStatus,
          creationTime,
          deletionTime,
          pathPreviousSnapshotId,
          globalPreviousSnapshotId,
          snapshotPath,
          checkpointDir,
          dbTxSequenceNumber
      );
    }
  }

  /**
   * Creates SnapshotInfo protobuf from SnapshotInfo.
   */
  public OzoneManagerProtocolProtos.SnapshotInfo getProtobuf() {
    OzoneManagerProtocolProtos.SnapshotInfo.Builder sib =
        OzoneManagerProtocolProtos.SnapshotInfo.newBuilder()
        .setSnapshotID(toProtobuf(snapshotId))
        .setName(name)
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setSnapshotStatus(snapshotStatus.toProto())
        .setCreationTime(creationTime)
        .setDeletionTime(deletionTime);

    if (pathPreviousSnapshotId != null) {
      sib.setPathPreviousSnapshotID(toProtobuf(pathPreviousSnapshotId));
    }

    if (globalPreviousSnapshotId != null) {
      sib.setGlobalPreviousSnapshotID(toProtobuf(globalPreviousSnapshotId));
    }

    sib.setSnapshotPath(snapshotPath)
        .setCheckpointDir(checkpointDir)
        .setDbTxSequenceNumber(dbTxSequenceNumber);
    return sib.build();
  }

  /**
   * Parses SnapshotInfo protobuf and creates SnapshotInfo.
   * @param snapshotInfoProto protobuf
   * @return instance of SnapshotInfo
   */
  public static SnapshotInfo getFromProtobuf(
      OzoneManagerProtocolProtos.SnapshotInfo snapshotInfoProto) {

    SnapshotInfo.Builder osib = SnapshotInfo.newBuilder()
        .setSnapshotId(fromProtobuf(snapshotInfoProto.getSnapshotID()))
        .setName(snapshotInfoProto.getName())
        .setVolumeName(snapshotInfoProto.getVolumeName())
        .setBucketName(snapshotInfoProto.getBucketName())
        .setSnapshotStatus(SnapshotStatus.valueOf(snapshotInfoProto
            .getSnapshotStatus()))
        .setCreationTime(snapshotInfoProto.getCreationTime())
        .setDeletionTime(snapshotInfoProto.getDeletionTime());

    if (snapshotInfoProto.hasPathPreviousSnapshotID()) {
      osib.setPathPreviousSnapshotId(
          fromProtobuf(snapshotInfoProto.getPathPreviousSnapshotID()));
    }

    if (snapshotInfoProto.hasGlobalPreviousSnapshotID()) {
      osib.setGlobalPreviousSnapshotId(
          fromProtobuf(snapshotInfoProto.getGlobalPreviousSnapshotID()));
    }

    osib.setSnapshotPath(snapshotInfoProto.getSnapshotPath())
        .setCheckpointDir(snapshotInfoProto.getCheckpointDir())
        .setDbTxSequenceNumber(snapshotInfoProto.getDbTxSequenceNumber());

    return osib.build();
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, getVolumeName());
    auditMap.put(OzoneConsts.BUCKET, getBucketName());
    auditMap.put(OzoneConsts.OM_SNAPSHOT_NAME, this.name);
    return auditMap;
  }

  /**
   * Get the name of the checkpoint directory.
   */
  public static String getCheckpointDirName(UUID snapshotId) {
    Objects.requireNonNull(snapshotId,
        "SnapshotId is needed to create checkpoint directory");
    return SEPARATOR + snapshotId;
  }
  /**
   * Get the name of the checkpoint directory, (non-static).
   */
  public String getCheckpointDirName() {
    return getCheckpointDirName(getSnapshotId());
  }

  public long getDbTxSequenceNumber() {
    return dbTxSequenceNumber;
  }

  public void setDbTxSequenceNumber(long dbTxSequenceNumber) {
    this.dbTxSequenceNumber = dbTxSequenceNumber;
  }

  /**
   * Get the table key for this snapshot.
   */
  public String getTableKey() {
    return getTableKey(volumeName, bucketName, name);
  }

  public static String getTableKey(String volumeName, String bucketName,
      String snapshotName) {
    return OM_KEY_PREFIX + volumeName + OM_KEY_PREFIX + bucketName
        + OM_KEY_PREFIX + snapshotName;
  }

  /**
   * Generate default name of snapshot, (used if user doesn't provide one).
   */
  @VisibleForTesting
  public static String generateName(long initialTime) {
    String timePattern = "yyyyMMdd-HHmmss.SSS";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timePattern);
    Instant instant = Instant.ofEpochMilli(initialTime);
    return "s" + formatter.format(
        ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")));
  }
  
  /**
   * Factory for making standard instance.
   */
  public static SnapshotInfo newInstance(String volumeName,
                                         String bucketName,
                                         String snapshotName,
                                         UUID snapshotId,
                                         long creationTime) {
    SnapshotInfo.Builder builder = new SnapshotInfo.Builder();
    if (StringUtils.isBlank(snapshotName)) {
      snapshotName = generateName(creationTime);
    }
    builder.setSnapshotId(snapshotId)
        .setName(snapshotName)
        .setCreationTime(creationTime)
        .setDeletionTime(INVALID_TIMESTAMP)
        .setPathPreviousSnapshotId(INITIAL_SNAPSHOT_ID)
        .setGlobalPreviousSnapshotId(INITIAL_SNAPSHOT_ID)
        .setSnapshotPath(volumeName + OM_KEY_PREFIX + bucketName)
        .setVolumeName(volumeName)
        .setBucketName(bucketName);

    if (snapshotId != null) {
      builder.setCheckpointDir(getCheckpointDirName(snapshotId));
    }
    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SnapshotInfo that = (SnapshotInfo) o;
    return creationTime == that.creationTime &&
        deletionTime == that.deletionTime &&
        snapshotId.equals(that.snapshotId) &&
        name.equals(that.name) && volumeName.equals(that.volumeName) &&
        bucketName.equals(that.bucketName) &&
        snapshotStatus == that.snapshotStatus &&
        Objects.equals(pathPreviousSnapshotId, that.pathPreviousSnapshotId) &&
        Objects.equals(
            globalPreviousSnapshotId, that.globalPreviousSnapshotId) &&
        snapshotPath.equals(that.snapshotPath) &&
        checkpointDir.equals(that.checkpointDir);
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshotId, name, volumeName, bucketName,
        snapshotStatus,
        creationTime, deletionTime, pathPreviousSnapshotId,
        globalPreviousSnapshotId, snapshotPath, checkpointDir);
  }
}
