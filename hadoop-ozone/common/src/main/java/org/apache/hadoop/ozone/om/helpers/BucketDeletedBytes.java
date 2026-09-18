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
 * Bucket-level deleted bytes split.
 */
public final class BucketDeletedBytes {
  private final long snapshotTrappedBytes;
  private final long purgeableBytes;
  private final long snapshotTrappedKeys;
  private final long purgeableKeys;
  private final long snapshotTrappedDirs;
  private final long purgeableDirs;

  public BucketDeletedBytes(
      long snapshotTrappedBytes,
      long purgeableBytes,
      long snapshotTrappedKeys,
      long purgeableKeys,
      long snapshotTrappedDirs,
      long purgeableDirs) {
    this.snapshotTrappedBytes = snapshotTrappedBytes;
    this.purgeableBytes = purgeableBytes;
    this.snapshotTrappedKeys = snapshotTrappedKeys;
    this.purgeableKeys = purgeableKeys;
    this.snapshotTrappedDirs = snapshotTrappedDirs;
    this.purgeableDirs = purgeableDirs;
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
}

