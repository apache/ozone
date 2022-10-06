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
import org.apache.hadoop.ozone.om.helpers.WithMetadata;

/**
 * A class that encapsulates OzoneSnapshot.
 */
public class OzoneSnapshot extends WithMetadata {

  private final String volumeName;
  private final String bucketName;
  private final String name;
  private final long creationTime;
  private final SnapshotInfo.SnapshotStatus snapshotStatus;
  private final String snapshotID;  // UUID
  private final String snapshotPath; // snapshot mask

  /**
   * Constructs OzoneSnapshot from SnapshotInfo.
   * @param volumeName Name of the Volume the snapshot belongs to.
   * @param bucketName Name of the Bucket the snapshot belongs to.
   * @param name Name of the snapshot.
   * @param creationTime Creation time of the snapshot.
   * @param snapshotStatus Status of the snapshot.
   * @param snapshotID ID of the snapshot.
   * @param snapshotPath Path of the snapshot.
   */
  public OzoneSnapshot(String volumeName, String bucketName,
                       String name, long creationTime,
                       SnapshotInfo.SnapshotStatus snapshotStatus,
                       String snapshotID, String snapshotPath) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.name = name;
    this.creationTime = creationTime;
    this.snapshotStatus = snapshotStatus;
    this.snapshotID = snapshotID;
    this.snapshotPath = snapshotPath;
  }

  /**
   * Returns Volume Name associated with the snapshot.
   *
   * @return volumeName
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Returns Bucket Name associated with the snapshot.
   *
   * @return bucketName
   */
  public String getBucketName() {
    return bucketName;
  }

  /**
   * Returns Name associated with the snapshot.
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
  public String getSnapshotID() {
    return snapshotID;
  }

  /**
   * Returns Path of the snapshot.
   *
   * @return snapshotPath
   */
  public String getSnapshotPath() {
    return snapshotPath;
  }
}
