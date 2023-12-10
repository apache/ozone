/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.client;

import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus;

import java.util.UUID;

/**
 * A class that encapsulates OzoneSnapshot.
 */
public class OzoneSnapshot {

  private final String volumeName;
  private final String bucketName;
  private final String name;
  private final long creationTime;
  private final SnapshotStatus snapshotStatus;
  private final UUID snapshotId;  // UUID
  private final String snapshotPath; // snapshot mask
  private final String checkpointDir;
  private final long referencedSize;
  private final long referencedReplicatedSize;
  private final long exclusiveSize;
  private final long exclusiveReplicatedSize;

  /**
   * Constructs OzoneSnapshot from SnapshotInfo.
   *
   * @param volumeName     Name of the Volume the snapshot belongs to.
   * @param bucketName     Name of the Bucket the snapshot belongs to.
   * @param name           Name of the snapshot.
   * @param creationTime   Creation time of the snapshot.
   * @param snapshotStatus Status of the snapshot.
   * @param snapshotId     ID of the snapshot.
   * @param snapshotPath   Path of the snapshot.
   * @param checkpointDir  Snapshot checkpoint directory.
   * @param referencedSize Snapshot referenced size.
   * @param referencedReplicatedSize Snapshot referenced size after replication.
   * @param exclusiveSize  Snapshot exclusive size.
   * @param exclusiveReplicatedSize  Snapshot exclusive size after replication.
   */
  @SuppressWarnings("parameternumber")
  public OzoneSnapshot(String volumeName,
                       String bucketName,
                       String name,
                       long creationTime,
                       SnapshotStatus snapshotStatus,
                       UUID snapshotId,
                       String snapshotPath,
                       String checkpointDir,
                       long referencedSize,
                       long referencedReplicatedSize,
                       long exclusiveSize,
                       long exclusiveReplicatedSize) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.name = name;
    this.creationTime = creationTime;
    this.snapshotStatus = snapshotStatus;
    this.snapshotId = snapshotId;
    this.snapshotPath = snapshotPath;
    this.checkpointDir = checkpointDir;
    this.referencedSize = referencedSize;
    this.referencedReplicatedSize = referencedReplicatedSize;
    this.exclusiveSize = exclusiveSize;
    this.exclusiveReplicatedSize = exclusiveReplicatedSize;
  }

  /**
   * Returns volume name associated with the snapshot.
   *
   * @return volumeName
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Returns bucket name associated with the snapshot.
   *
   * @return bucketName
   */
  public String getBucketName() {
    return bucketName;
  }

  /**
   * Returns name associated with the snapshot.
   *
   * @return name
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the creation time of the snapshot.
   *
   * @return creationTime
   */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * Returns the status of the snapshot.
   *
   * @return snapshotStatus
   */
  public String getSnapshotStatus() {
    return snapshotStatus.name();
  }

  /**
   * Returns ID of the snapshot.
   *
   * @return snapshotID
   */
  public UUID getSnapshotId() {
    return snapshotId;
  }

  /**
   * Returns path of the snapshot.
   *
   * @return snapshotPath
   */
  public String getSnapshotPath() {
    return snapshotPath;
  }

  /**
   * Return snapshot checkpoint directory.
   *
   * @return snapshotCheckpointDir
   */
  public String getCheckpointDir() {
    return checkpointDir;
  }

  /**
   * @return Referenced size of the snapshot.
   */
  public long getReferencedSize() {
    return referencedSize;
  }

  /**
   * @return Reference size after replication/EC of the snapshot
   */
  public long getReferencedReplicatedSize() {
    return referencedReplicatedSize;
  }

  /**
   * @return Exclusive size of the snapshot.
   */
  public long getExclusiveSize() {
    return exclusiveSize;
  }

  /**
   * @return Exclusive size after replication/EC of the snapshot.
   */
  public long getExclusiveReplicatedSize() {
    return exclusiveReplicatedSize;
  }

  public static OzoneSnapshot fromSnapshotInfo(SnapshotInfo snapshotInfo) {
    return new OzoneSnapshot(
        snapshotInfo.getVolumeName(),
        snapshotInfo.getBucketName(),
        snapshotInfo.getName(),
        snapshotInfo.getCreationTime(),
        snapshotInfo.getSnapshotStatus(),
        snapshotInfo.getSnapshotId(),
        snapshotInfo.getSnapshotPath(),
        snapshotInfo.getCheckpointDir(),
        snapshotInfo.getReferencedSize(),
        snapshotInfo.getReferencedReplicatedSize(),
        snapshotInfo.getExclusiveSize(),
        snapshotInfo.getExclusiveReplicatedSize()
    );
  }
}
