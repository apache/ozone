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
package org.apache.hadoop.ozone.debug;

import java.util.Objects;

/**
 * Class that holds basic key data in relation to container it is in.
 */
public class ContainerKeyInfo {

  private final long containerID;
  private final String volumeName;
  private final long volumeId;
  private final String bucketName;
  private final long bucketId;
  private final String keyName;
  private final long parentId;

  public ContainerKeyInfo(long containerID, String volumeName, long volumeId,
                          String bucketName, long bucketId, String keyName,
                          long parentId) {
    this.containerID = containerID;
    this.volumeName = volumeName;
    this.volumeId = volumeId;
    this.bucketName = bucketName;
    this.bucketId = bucketId;
    this.keyName = keyName;
    this.parentId = parentId;
  }

  public long getContainerID() {
    return containerID;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ContainerKeyInfo that = (ContainerKeyInfo) o;
    return containerID == that.containerID && volumeId == that.volumeId &&
        bucketId == that.bucketId && parentId == that.parentId &&
        Objects.equals(volumeName, that.volumeName) &&
        Objects.equals(bucketName, that.bucketName) &&
        Objects.equals(keyName, that.keyName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(containerID, volumeName, volumeId, bucketName, bucketId,
        keyName, parentId);
  }
}
