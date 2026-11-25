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

/**
 * Bucket-level deleted-data split for a bucket at query time.
 * <p>
 * {@code snapshotTrappedBytes + purgeableBytes} represents the replicated size
 * currently visible to the on-demand calculator for deleted keys and files
 * discovered in deleted directory traversal for that bucket.
 * <p>
 * These values are not the same as OM global metrics such as
 * {@code totalPendingDeletionBytes} / {@code totalPendingDeletionKeyCount}
 * because those are cluster-wide service counters with different timing and
 * accounting scope.
 */
public final class BucketDeletedBytes {
  private final long snapshotTrappedBytes;
  private final long purgeableBytes;
  private final long snapshotTrappedKeys;
  private final long purgeableKeys;
  private final long snapshotTrappedDirs;
  private final long purgeableDirs;
  private final long purgeableKeyBytes;
  private final long purgeableDirBytes;

  private BucketDeletedBytes(Builder b) {
    this.snapshotTrappedBytes = b.snapshotTrappedBytes;
    this.purgeableBytes = b.purgeableBytes;
    this.snapshotTrappedKeys = b.snapshotTrappedKeys;
    this.purgeableKeys = b.purgeableKeys;
    this.snapshotTrappedDirs = b.snapshotTrappedDirs;
    this.purgeableDirs = b.purgeableDirs;
    this.purgeableKeyBytes = b.purgeableKeyBytes;
    this.purgeableDirBytes = b.purgeableDirBytes;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public long getSnapshotTrappedBytes() {
    return snapshotTrappedBytes;
  }

  public long getPurgeableBytes() {
    return purgeableBytes;
  }

  public long getSnapshotTrappedKeys() {
    return snapshotTrappedKeys;
  }

  public long getPurgeableKeys() {
    return purgeableKeys;
  }

  public long getSnapshotTrappedDirs() {
    return snapshotTrappedDirs;
  }

  public long getPurgeableDirs() {
    return purgeableDirs;
  }

  /**
   * Purgeable bytes of entries in the deleted table, or -1 if OM did not report it.
   */
  public long getPurgeableKeyBytes() {
    return purgeableKeyBytes;
  }

  /**
   * Purgeable bytes of files found under deleted directories, or -1 if OM did not report it.
   */
  public long getPurgeableDirBytes() {
    return purgeableDirBytes;
  }

  /**
   * Builder for BucketDeletedBytes.
   */
  public static final class Builder {
    private long snapshotTrappedBytes;
    private long purgeableBytes;
    private long snapshotTrappedKeys;
    private long purgeableKeys;
    private long snapshotTrappedDirs;
    private long purgeableDirs;
    private long purgeableKeyBytes = -1L;
    private long purgeableDirBytes = -1L;

    private Builder() {
    }

    public Builder setSnapshotTrappedBytes(long snapshotTrappedBytes) {
      this.snapshotTrappedBytes = snapshotTrappedBytes;
      return this;
    }

    public Builder setPurgeableBytes(long purgeableBytes) {
      this.purgeableBytes = purgeableBytes;
      return this;
    }

    public Builder setSnapshotTrappedKeys(long snapshotTrappedKeys) {
      this.snapshotTrappedKeys = snapshotTrappedKeys;
      return this;
    }

    public Builder setPurgeableKeys(long purgeableKeys) {
      this.purgeableKeys = purgeableKeys;
      return this;
    }

    public Builder setSnapshotTrappedDirs(long snapshotTrappedDirs) {
      this.snapshotTrappedDirs = snapshotTrappedDirs;
      return this;
    }

    public Builder setPurgeableDirs(long purgeableDirs) {
      this.purgeableDirs = purgeableDirs;
      return this;
    }

    public Builder setPurgeableKeyBytes(long purgeableKeyBytes) {
      this.purgeableKeyBytes = purgeableKeyBytes;
      return this;
    }

    public Builder setPurgeableDirBytes(long purgeableDirBytes) {
      this.purgeableDirBytes = purgeableDirBytes;
      return this;
    }

    public BucketDeletedBytes build() {
      return new BucketDeletedBytes(this);
    }
  }
}
