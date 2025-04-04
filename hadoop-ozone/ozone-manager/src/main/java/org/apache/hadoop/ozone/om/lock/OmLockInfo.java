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

package org.apache.hadoop.ozone.om.lock;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Lock information.
 */
public final class OmLockInfo {
  private final LockInfo volumeLock;
  private final LockInfo bucketLock;
  private final Set<LockInfo> keyLocks;

  private OmLockInfo(Builder builder) {
    volumeLock = builder.volumeLock;
    bucketLock = builder.bucketLock;
    keyLocks = builder.keyLocks;
  }

  public Optional<LockInfo> getVolumeLock() {
    return Optional.ofNullable(volumeLock);
  }

  public Optional<LockInfo> getBucketLock() {
    return Optional.ofNullable(bucketLock);
  }

  public Optional<Set<LockInfo>> getKeyLocks() {
    return Optional.ofNullable(keyLocks);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (volumeLock != null) {
      sb.append("Volume:").append(volumeLock);
    }
    if (bucketLock != null) {
      sb.append("Bucket:").append(bucketLock);
    }
    if (keyLocks != null) {
      sb.append("Keys:").append(keyLocks);
    }
    return sb.toString();
  }

  /**
   * Builds an {@link OmLockInfo} object with optional volume, bucket or key locks.
   */
  public static final class Builder {
    private LockInfo volumeLock;
    private LockInfo bucketLock;
    private Set<LockInfo> keyLocks;

    public Builder() {
    }

    public Builder addVolumeReadLock(String volume) {
      volumeLock = LockInfo.writeLockInfo(volume);
      return this;
    }

    public Builder addVolumeWriteLock(String volume) {
      volumeLock = LockInfo.readLockInfo(volume);
      return this;
    }

    public Builder addBucketReadLock(String volume, String bucket) {
      bucketLock = LockInfo.readLockInfo(joinStrings(volume, bucket));
      return this;
    }

    public Builder addBucketWriteLock(String volume, String bucket) {
      bucketLock = LockInfo.writeLockInfo(joinStrings(volume, bucket));
      return this;
    }

    // Currently there is no use case for key level read locks.
    public Builder addKeyWriteLock(String volume, String bucket, String key) {
      // Lazy init keys.
      if (keyLocks == null) {
        keyLocks = new HashSet<>();
      }
      keyLocks.add(LockInfo.writeLockInfo(joinStrings(volume, bucket, key)));
      return this;
    }

    private String joinStrings(String... parts) {
      return String.join(OzoneConsts.OZONE_URI_DELIMITER, parts);
    }

    public OmLockInfo build() {
      return new OmLockInfo(this);
    }
  }

  /**
   * This class provides specifications about a lock's requirements.
   */
  public static final class LockInfo implements Comparable<LockInfo> {
    private final String name;
    private final boolean isWriteLock;
    
    private LockInfo(String name, boolean isWriteLock) {
      this.name = name;
      this.isWriteLock = isWriteLock;
    }

    public static LockInfo writeLockInfo(String key) {
      return new LockInfo(key, true);
    }

    public static LockInfo readLockInfo(String key) {
      return new LockInfo(key, false);
    }

    public String getName() {
      return name;
    }

    public boolean isWriteLock() {
      return isWriteLock;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof LockInfo)) {
        return false;
      }
      LockInfo lockInfo = (LockInfo) o;
      return isWriteLock == lockInfo.isWriteLock && Objects.equals(name, lockInfo.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, isWriteLock);
    }

    @Override
    public int compareTo(LockInfo other) {
      return Integer.compare(hashCode(), other.hashCode());
    }

    @Override
    public String toString() {
      return "LockInfo{" + "name=" + name + ", isWriteLock=" + isWriteLock + '}';
    }
  }
}
