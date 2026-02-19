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

import static org.apache.hadoop.hdds.HddsUtils.fromProtobuf;
import static org.apache.hadoop.hdds.HddsUtils.toProtobuf;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_SEPARATOR;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotStatusProto;

/**
 * This class is used for storing info related to Snapshots.
 *
 * Each snapshot created has an associated SnapshotInfo entry
 * containing the snapshotId, snapshot path,
 * snapshot checkpoint directory, previous snapshotId
 * for the snapshot path and global amongst other necessary fields.
 */
public final class SnapshotInfo implements Auditable, CopyObject<SnapshotInfo> {
  private static final Codec<SnapshotInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(OzoneManagerProtocolProtos.SnapshotInfo.getDefaultInstance()),
      SnapshotInfo::getFromProtobuf,
      SnapshotInfo::getProtobuf,
      SnapshotInfo.class);

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
  private boolean deepClean;
  private boolean sstFiltered;
  private long referencedSize;
  private long referencedReplicatedSize;
  private long exclusiveSize;
  private long exclusiveReplicatedSize;
  private long exclusiveSizeDeltaFromDirDeepCleaning;
  private long exclusiveReplicatedSizeDeltaFromDirDeepCleaning;
  private boolean deepCleanedDeletedDir;
  private ByteString createTransactionInfo;
  private ByteString lastTransactionInfo;

  private SnapshotInfo(Builder b) {
    this.snapshotId = b.snapshotId;
    this.name = b.name;
    this.volumeName = b.volumeName;
    this.bucketName = b.bucketName;
    this.snapshotStatus = b.snapshotStatus;
    this.creationTime = b.creationTime;
    this.deletionTime = b.deletionTime;
    this.pathPreviousSnapshotId = b.pathPreviousSnapshotId;
    this.globalPreviousSnapshotId = b.globalPreviousSnapshotId;
    this.snapshotPath = b.snapshotPath;
    this.deepClean = b.deepClean;
    this.sstFiltered = b.sstFiltered;
    this.referencedSize = b.referencedSize;
    this.referencedReplicatedSize = b.referencedReplicatedSize;
    this.exclusiveSize = b.exclusiveSize;
    this.exclusiveReplicatedSize = b.exclusiveReplicatedSize;
    this.exclusiveSizeDeltaFromDirDeepCleaning = b.exclusiveSizeDeltaFromDirDeepCleaning;
    this.exclusiveReplicatedSizeDeltaFromDirDeepCleaning = b.exclusiveReplicatedSizeDeltaFromDirDeepCleaning;
    this.deepCleanedDeletedDir = b.deepCleanedDeletedDir;
    this.lastTransactionInfo = b.lastTransactionInfo;
    this.createTransactionInfo = b.createTransactionInfo;
  }

  public static Codec<SnapshotInfo> getCodec() {
    return CODEC;
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

  public boolean isDeepCleaned() {
    return deepClean;
  }

  public void setDeepClean(boolean deepClean) {
    this.deepClean = deepClean;
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

  public boolean isSstFiltered() {
    return sstFiltered;
  }

  public void setSstFiltered(boolean sstFiltered) {
    this.sstFiltered = sstFiltered;
  }

  public static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.Builder
      newBuilder() {
    return new org.apache.hadoop.ozone.om.helpers.SnapshotInfo.Builder();
  }

  public SnapshotInfo.Builder toBuilder() {
    return new Builder()
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
        .setDeepClean(deepClean)
        .setSstFiltered(sstFiltered)
        .setReferencedSize(referencedSize)
        .setReferencedReplicatedSize(referencedReplicatedSize)
        .setExclusiveSize(exclusiveSize)
        .setExclusiveReplicatedSize(exclusiveReplicatedSize)
        .setExclusiveSizeDeltaFromDirDeepCleaning(exclusiveSizeDeltaFromDirDeepCleaning)
        .setExclusiveReplicatedSizeDeltaFromDirDeepCleaning(exclusiveReplicatedSizeDeltaFromDirDeepCleaning)
        .setDeepCleanedDeletedDir(deepCleanedDeletedDir)
        .setLastTransactionInfo(lastTransactionInfo)
        .setCreateTransactionInfo(createTransactionInfo);
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
    private boolean deepClean;
    private boolean sstFiltered;
    private long referencedSize;
    private long referencedReplicatedSize;
    private long exclusiveSize;
    private long exclusiveReplicatedSize;
    private long exclusiveSizeDeltaFromDirDeepCleaning;
    private long exclusiveReplicatedSizeDeltaFromDirDeepCleaning;
    private boolean deepCleanedDeletedDir;
    private ByteString createTransactionInfo;
    private ByteString lastTransactionInfo;

    public Builder() {
      // default values
      this.snapshotStatus = SnapshotStatus.DEFAULT;
    }

    /** @param snapshotId - Snapshot UUID. */
    public Builder setSnapshotId(UUID snapshotId) {
      this.snapshotId = snapshotId;
      return this;
    }

    /** @param name - snapshot name. */
    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    /** @param volumeName - volume name. */
    public Builder setVolumeName(String volumeName) {
      this.volumeName = volumeName;
      return this;
    }

    /** @param bucketName - bucket name. */
    public Builder setBucketName(String bucketName) {
      this.bucketName = bucketName;
      return this;
    }

    /** @param snapshotStatus - status: SNAPSHOT_ACTIVE, SNAPSHOT_DELETED */
    public Builder setSnapshotStatus(SnapshotStatus snapshotStatus) {
      this.snapshotStatus = snapshotStatus;
      return this;
    }

    /** @param crTime - Snapshot creation time. */
    public Builder setCreationTime(long crTime) {
      this.creationTime = crTime;
      return this;
    }

    /** @param delTime - Snapshot deletion time. */
    public Builder setDeletionTime(long delTime) {
      this.deletionTime = delTime;
      return this;
    }

    /** @param pathPreviousSnapshotId - Snapshot path previous snapshot id. */
    public Builder setPathPreviousSnapshotId(UUID pathPreviousSnapshotId) {
      this.pathPreviousSnapshotId = pathPreviousSnapshotId;
      return this;
    }

    /** @param globalPreviousSnapshotId - Snapshot global previous snapshot id. */
    public Builder setGlobalPreviousSnapshotId(UUID globalPreviousSnapshotId) {
      this.globalPreviousSnapshotId = globalPreviousSnapshotId;
      return this;
    }

    /** @param snapshotPath - Snapshot path, bucket .snapshot path. */
    public Builder setSnapshotPath(String snapshotPath) {
      this.snapshotPath = snapshotPath;
      return this;
    }

    /** @param deepClean - To be deep cleaned status for snapshot. */
    public Builder setDeepClean(boolean deepClean) {
      this.deepClean = deepClean;
      return this;
    }

    public Builder setSstFiltered(boolean sstFiltered) {
      this.sstFiltered = sstFiltered;
      return this;
    }

    /** @param referencedSize - Snapshot referenced size. */
    public Builder setReferencedSize(long referencedSize) {
      this.referencedSize = referencedSize;
      return this;
    }

    /** @param referencedReplicatedSize - Snapshot referenced size w/ replication. */
    public Builder setReferencedReplicatedSize(long referencedReplicatedSize) {
      this.referencedReplicatedSize = referencedReplicatedSize;
      return this;
    }

    /** @param exclusiveSize - Snapshot exclusive size. */
    public Builder setExclusiveSize(long exclusiveSize) {
      this.exclusiveSize = exclusiveSize;
      return this;
    }

    /** @param exclusiveReplicatedSize - Snapshot exclusive size w/ replication. */
    public Builder setExclusiveReplicatedSize(long exclusiveReplicatedSize) {
      this.exclusiveReplicatedSize = exclusiveReplicatedSize;
      return this;
    }

    /** @param exclusiveSizeDeltaFromDirDeepCleaning - Snapshot exclusive size. */
    public Builder setExclusiveSizeDeltaFromDirDeepCleaning(long exclusiveSizeDeltaFromDirDeepCleaning) {
      this.exclusiveSizeDeltaFromDirDeepCleaning = exclusiveSizeDeltaFromDirDeepCleaning;
      return this;
    }

    /** @param exclusiveReplicatedSizeDeltaFromDirDeepCleaning - Snapshot exclusive size w/ replication. */
    public Builder setExclusiveReplicatedSizeDeltaFromDirDeepCleaning(
        long exclusiveReplicatedSizeDeltaFromDirDeepCleaning) {
      this.exclusiveReplicatedSizeDeltaFromDirDeepCleaning = exclusiveReplicatedSizeDeltaFromDirDeepCleaning;
      return this;
    }

    public Builder setDeepCleanedDeletedDir(boolean deepCleanedDeletedDir) {
      this.deepCleanedDeletedDir = deepCleanedDeletedDir;
      return this;
    }

    public Builder setCreateTransactionInfo(ByteString createTransactionInfo) {
      this.createTransactionInfo = createTransactionInfo;
      return this;
    }

    public Builder setLastTransactionInfo(ByteString lastTransactionInfo) {
      this.lastTransactionInfo = lastTransactionInfo;
      return this;
    }

    public SnapshotInfo build() {
      Objects.requireNonNull(name, "name == null");
      return new SnapshotInfo(this);
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
            .setDeletionTime(deletionTime)
            .setSstFiltered(sstFiltered)
            .setReferencedSize(referencedSize)
            .setReferencedReplicatedSize(referencedReplicatedSize)
            .setExclusiveSize(exclusiveSize)
            .setExclusiveReplicatedSize(exclusiveReplicatedSize)
            .setExclusiveSizeDeltaFromDirDeepCleaning(exclusiveSizeDeltaFromDirDeepCleaning)
            .setExclusiveReplicatedSizeDeltaFromDirDeepCleaning(exclusiveReplicatedSizeDeltaFromDirDeepCleaning)
            .setDeepCleanedDeletedDir(deepCleanedDeletedDir);

    if (pathPreviousSnapshotId != null) {
      sib.setPathPreviousSnapshotID(toProtobuf(pathPreviousSnapshotId));
    }

    if (globalPreviousSnapshotId != null) {
      sib.setGlobalPreviousSnapshotID(toProtobuf(globalPreviousSnapshotId));
    }

    if (lastTransactionInfo != null) {
      sib.setLastTransactionInfo(lastTransactionInfo);
    }

    if (createTransactionInfo != null) {
      sib.setCreateTransactionInfo(createTransactionInfo);
    }

    sib.setSnapshotPath(snapshotPath)
        .setDeepClean(deepClean);
    return sib.build();
  }

  /**
   * Parses SnapshotInfo protobuf and creates SnapshotInfo Builder.
   * @param snapshotInfoProto protobuf
   * @return Builder instance
   */
  public static Builder builderFromProtobuf(
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

    if (snapshotInfoProto.hasDeepClean()) {
      osib.setDeepClean(snapshotInfoProto.getDeepClean());
    }

    if (snapshotInfoProto.hasSstFiltered()) {
      osib.setSstFiltered(snapshotInfoProto.getSstFiltered());
    }

    if (snapshotInfoProto.hasReferencedSize()) {
      osib.setReferencedSize(
          snapshotInfoProto.getReferencedSize());
    }

    if (snapshotInfoProto.hasReferencedReplicatedSize()) {
      osib.setReferencedReplicatedSize(
          snapshotInfoProto.getReferencedReplicatedSize());
    }

    if (snapshotInfoProto.hasExclusiveSize()) {
      osib.setExclusiveSize(
          snapshotInfoProto.getExclusiveSize());
    }

    if (snapshotInfoProto.hasExclusiveReplicatedSize()) {
      osib.setExclusiveReplicatedSize(
          snapshotInfoProto.getExclusiveReplicatedSize());
    }

    if (snapshotInfoProto.hasExclusiveSizeDeltaFromDirDeepCleaning()) {
      osib.setExclusiveSizeDeltaFromDirDeepCleaning(snapshotInfoProto.getExclusiveSizeDeltaFromDirDeepCleaning());
    }

    if (snapshotInfoProto.hasExclusiveReplicatedSizeDeltaFromDirDeepCleaning()) {
      osib.setExclusiveReplicatedSizeDeltaFromDirDeepCleaning(
          snapshotInfoProto.getExclusiveReplicatedSizeDeltaFromDirDeepCleaning());
    }

    if (snapshotInfoProto.hasDeepCleanedDeletedDir()) {
      osib.setDeepCleanedDeletedDir(
          snapshotInfoProto.getDeepCleanedDeletedDir());
    }

    if (snapshotInfoProto.hasLastTransactionInfo()) {
      osib.setLastTransactionInfo(snapshotInfoProto.getLastTransactionInfo());
    }

    if (snapshotInfoProto.hasCreateTransactionInfo()) {
      osib.setCreateTransactionInfo(snapshotInfoProto.getCreateTransactionInfo());
    }
    osib.setSnapshotPath(snapshotInfoProto.getSnapshotPath());

    return osib;
  }

  /**
   * Parses SnapshotInfo protobuf and creates SnapshotInfo.
   * @param snapshotInfoProto protobuf
   * @return instance of SnapshotInfo
   */
  public static SnapshotInfo getFromProtobuf(
      OzoneManagerProtocolProtos.SnapshotInfo snapshotInfoProto) {
    return builderFromProtobuf(snapshotInfoProto).build();
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
  public static String getCheckpointDirName(UUID snapshotId, int version) {
    Objects.requireNonNull(snapshotId, "SnapshotId is needed to create checkpoint directory");
    if (version == 0) {
      return OM_SNAPSHOT_SEPARATOR + snapshotId;
    }
    return OM_SNAPSHOT_SEPARATOR + snapshotId + OM_SNAPSHOT_SEPARATOR + version;
  }

  /**
   * Get the name of the checkpoint directory, (non-static).
   */
  public String getCheckpointDirName(int version) {
    return getCheckpointDirName(getSnapshotId(), version);
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

  public void setReferencedSize(long referencedSize) {
    this.referencedSize = referencedSize;
  }

  public long getReferencedSize() {
    return referencedSize;
  }

  public void setReferencedReplicatedSize(long referencedReplicatedSize) {
    this.referencedReplicatedSize = referencedReplicatedSize;
  }

  public long getReferencedReplicatedSize() {
    return referencedReplicatedSize;
  }

  public void setExclusiveSize(long exclusiveSize) {
    this.exclusiveSize = exclusiveSize;
  }

  public long getExclusiveSize() {
    return exclusiveSize;
  }

  public void setExclusiveSizeDeltaFromDirDeepCleaning(long exclusiveSizeDeltaFromDirDeepCleaning) {
    this.exclusiveSizeDeltaFromDirDeepCleaning = exclusiveSizeDeltaFromDirDeepCleaning;
  }

  public long getExclusiveSizeDeltaFromDirDeepCleaning() {
    return exclusiveSizeDeltaFromDirDeepCleaning;
  }

  public void setExclusiveReplicatedSize(long exclusiveReplicatedSize) {
    this.exclusiveReplicatedSize = exclusiveReplicatedSize;
  }

  public void setExclusiveReplicatedSizeDeltaFromDirDeepCleaning(long exclusiveReplicatedSizeDeltaFromDirDeepCleaning) {
    this.exclusiveReplicatedSizeDeltaFromDirDeepCleaning = exclusiveReplicatedSizeDeltaFromDirDeepCleaning;
  }

  public long getExclusiveReplicatedSizeDeltaFromDirDeepCleaning() {
    return exclusiveReplicatedSizeDeltaFromDirDeepCleaning;
  }

  public long getExclusiveReplicatedSize() {
    return exclusiveReplicatedSize;
  }

  public boolean isDeepCleanedDeletedDir() {
    return deepCleanedDeletedDir;
  }

  public void setDeepCleanedDeletedDir(boolean deepCleanedDeletedDir) {
    this.deepCleanedDeletedDir = deepCleanedDeletedDir;
  }

  public ByteString getLastTransactionInfo() {
    return lastTransactionInfo;
  }

  public ByteString getCreateTransactionInfo() {
    return createTransactionInfo;
  }

  public void setCreateTransactionInfo(ByteString createTransactionInfo) {
    this.createTransactionInfo = createTransactionInfo;
  }

  public void setLastTransactionInfo(ByteString lastTransactionInfo) {
    this.lastTransactionInfo = lastTransactionInfo;
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
        .setBucketName(bucketName)
        .setDeepClean(false)
        .setDeepCleanedDeletedDir(false);
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
        deepClean == that.deepClean &&
        sstFiltered == that.sstFiltered &&
        referencedSize == that.referencedSize &&
        referencedReplicatedSize == that.referencedReplicatedSize &&
        exclusiveSize == that.exclusiveSize &&
        exclusiveReplicatedSize == that.exclusiveReplicatedSize &&
        deepCleanedDeletedDir == that.deepCleanedDeletedDir &&
        Objects.equals(lastTransactionInfo, that.lastTransactionInfo) &&
        Objects.equals(createTransactionInfo, that.createTransactionInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshotId, name, volumeName, bucketName,
        snapshotStatus,
        creationTime, deletionTime, pathPreviousSnapshotId,
        globalPreviousSnapshotId, snapshotPath,
        deepClean, sstFiltered,
        referencedSize, referencedReplicatedSize,
        exclusiveSize, exclusiveReplicatedSize, deepCleanedDeletedDir, lastTransactionInfo, createTransactionInfo);
  }

  /**
   * Return a new copy of the object.
   */
  @Override
  public SnapshotInfo copyObject() {
    return this.toBuilder().build();
  }

  @Override
  public String toString() {
    return "SnapshotInfo{" +
        "snapshotId: '" + snapshotId + '\'' +
        ", name: '" + name + '\'' +
        ", volumeName: '" + volumeName + '\'' +
        ", bucketName: '" + bucketName + '\'' +
        ", snapshotStatus: '" + snapshotStatus + '\'' +
        ", creationTime: '" + creationTime + '\'' +
        ", deletionTime: '" + deletionTime + '\'' +
        ", pathPreviousSnapshotId: '" + pathPreviousSnapshotId + '\'' +
        ", globalPreviousSnapshotId: '" + globalPreviousSnapshotId + '\'' +
        ", snapshotPath: '" + snapshotPath + '\'' +
        ", deepClean: '" + deepClean + '\'' +
        ", sstFiltered: '" + sstFiltered + '\'' +
        ", referencedSize: '" + referencedSize + '\'' +
        ", referencedReplicatedSize: '" + referencedReplicatedSize + '\'' +
        ", exclusiveSize: '" + exclusiveSize + '\'' +
        ", exclusiveReplicatedSize: '" + exclusiveReplicatedSize + '\'' +
        ", exclusiveSizeDeltaFromDirDeepCleaning: '" + exclusiveSizeDeltaFromDirDeepCleaning + '\'' +
        ", exclusiveReplicatedSizeDeltaFromDirDeepCleaning: '" + exclusiveReplicatedSizeDeltaFromDirDeepCleaning +
        "', deepCleanedDeletedDir: '" + deepCleanedDeletedDir + '\'' +
        ", lastTransactionInfo: '" + lastTransactionInfo + '\'' +
        ", createTransactionInfo: '" + createTransactionInfo + '\'' +
        '}';
  }

  /**
   * SnapshotStatus enum composed of active and deleted statuses.
   */
  public enum SnapshotStatus {
    SNAPSHOT_ACTIVE,
    SNAPSHOT_DELETED;

    public static final SnapshotStatus DEFAULT = SNAPSHOT_ACTIVE;

    public SnapshotStatusProto toProto() {
      switch (this) {
      case SNAPSHOT_ACTIVE:
        return SnapshotStatusProto.SNAPSHOT_ACTIVE;
      case SNAPSHOT_DELETED:
        return SnapshotStatusProto.SNAPSHOT_DELETED;
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
      default:
        throw new IllegalStateException(
            "BUG: missing valid SnapshotStatus, found status=" + status);
      }
    }
  }
}
