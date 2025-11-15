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

package org.apache.hadoop.ozone.recon.api.types;

import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.removeTrailingSlashIfNeeded;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.ozone.recon.ReconConstants;

/**
 * Class to encapsulate namespace metadata summaries from OM.
 * <p>
 * IMPORTANT: As of the materialized optimization, sizeOfFiles and numOfFiles
 * now represent TOTAL values (including this directory and ALL subdirectories)
 * rather than just direct files in this directory, for O(1) disk usage queries.
 */

public class NSSummary {
  // TOTALS over this directory + all descendants
  private int numOfFiles;
  private long sizeOfFiles;
  private long replicatedSizeOfFiles;

  // Backing array allocated once; mutate in place via incBucket/decBucket
  private final int[] fileSizeBucket;

  // Child directory IDs; lazily allocated to avoid unnecessary churn
  private Set<Long> childDir;

  private String dirName;
  private long parentId = 0;

  public NSSummary() {
    this(0, 0L, 0L, null, null, "", 0);
  }

  public NSSummary(int numOfFiles,
                   long sizeOfFiles,
                   long replicatedSizeOfFiles,
                   int[] bucket,
                   Set<Long> childDir,
                   String dirName,
                   long parentId) {
    this.numOfFiles = numOfFiles;
    this.sizeOfFiles = sizeOfFiles;
    this.replicatedSizeOfFiles = replicatedSizeOfFiles;
    this.fileSizeBucket = new int[ReconConstants.NUM_OF_FILE_SIZE_BINS];
    if (bucket != null) {
      System.arraycopy(bucket, 0, this.fileSizeBucket, 0,
          Math.min(bucket.length, this.fileSizeBucket.length));
    }
    if (childDir != null && !childDir.isEmpty()) {
      // capacity with headroom; avoids rehashing on addChildDir()
      this.childDir = new HashSet<>(Math.max(16, childDir.size() * 2));
      this.childDir.addAll(childDir);
    } else {
      this.childDir = null; // lazy
    }
    this.dirName = removeTrailingSlashIfNeeded(dirName == null ? "" : dirName);
    this.parentId = parentId;
  }

  // ---------------- Public API (preserve semantics) ----------------

  /**
   * TOTAL files (this dir + all subdirs).
   */
  public int getNumOfFiles() {
    return numOfFiles;
  }

  /**
   * TOTAL size (this dir + all subdirs).
   */
  public long getSizeOfFiles() {
    return sizeOfFiles;
  }

  /**
   * TOTAL replicated size (this dir + all subdirs).
   */
  public long getReplicatedSizeOfFiles() {
    return replicatedSizeOfFiles;
  }

  /**
   * Returns a COPY; avoid in hot paths—use incBucket/decBucket instead.
   */
  public int[] getFileSizeBucket() {
    return Arrays.copyOf(fileSizeBucket, fileSizeBucket.length);
  }

  /**
   * Direct children (directory IDs).
   */
  public Set<Long> getChildDir() {
    return childDir == null ? Collections.emptySet() : childDir;
  }

  public String getDirName() {
    return dirName;
  }

  public long getParentId() {
    return parentId;
  }

  /**
   * Set TOTAL files; prefer in-place inc/dec in hot paths.
   */
  public void setNumOfFiles(int numOfFiles) {
    this.numOfFiles = numOfFiles;
  }

  /**
   * Set TOTAL size; prefer in-place inc/dec in hot paths.
   */
  public void setSizeOfFiles(long sizeOfFiles) {
    this.sizeOfFiles = sizeOfFiles;
  }

  /**
   * Set TOTAL replicated size; prefer in-place inc/dec in hot paths.
   */
  public void setReplicatedSizeOfFiles(long replicatedSizeOfFiles) {
    this.replicatedSizeOfFiles = replicatedSizeOfFiles;
  }

  /**
   * Copies into backing array; avoid on hot paths.
   */
  public void setFileSizeBucket(int[] fileSizeBucket) {
    if (fileSizeBucket == null) {
      Arrays.fill(this.fileSizeBucket, 0);
    } else {
      System.arraycopy(fileSizeBucket, 0, this.fileSizeBucket, 0,
          Math.min(fileSizeBucket.length, this.fileSizeBucket.length));
    }
  }

  public void setChildDir(Set<Long> childDir) {
    if (childDir == null || childDir.isEmpty()) {
      this.childDir = null;
    } else {
      this.childDir = new HashSet<>(Math.max(16, childDir.size() * 2));
      this.childDir.addAll(childDir);
    }
  }

  public void setDirName(String dirName) {
    this.dirName = removeTrailingSlashIfNeeded(dirName == null ? "" : dirName);
  }

  public void setParentId(long parentId) {
    this.parentId = parentId;
  }

  /**
   * Dedup handled by Set#add; lazy allocate to cut GC churn.
   */
  public void addChildDir(long childId) {
    if (this.childDir == null) {
      this.childDir = new HashSet<>(16);
    }
    this.childDir.add(childId);
  }

  public void removeChildDir(long childId) {
    if (this.childDir != null) {
      this.childDir.remove(childId);
      if (this.childDir.isEmpty()) {
        this.childDir = null; // free memory
      }
    }
  }

  // ---------------- Hot-path helpers (allocation-free) ----------------

  /**
   * Increment totals in place (use for put-key and upward propagation).
   */
  public void incFilesAndBytes(int files, long bytes) {
    this.numOfFiles += files;
    this.sizeOfFiles += bytes;
  }

  /**
   * Decrement totals in place with clamping to zero (use for delete-key).
   */
  public void decFilesAndBytes(int files, long bytes) {
    this.numOfFiles = clampNonNegativeInt(this.numOfFiles - files);
    this.sizeOfFiles = clampNonNegativeLong(this.sizeOfFiles - bytes);
  }

  /**
   * Bucket increment for immediate files; bounds-safe.
   */
  public void incBucket(int idx) {
    if (idx >= 0 && idx < fileSizeBucket.length) {
      fileSizeBucket[idx]++;
    }
  }

  /**
   * Bucket decrement for immediate files; bounds &amp; underflow-safe.
   */
  public void decBucket(int idx) {
    if (idx >= 0 && idx < fileSizeBucket.length && fileSizeBucket[idx] > 0) {
      fileSizeBucket[idx]--;
    }
  }

  private static int clampNonNegativeInt(int v) {
    return v < 0 ? 0 : v;
  }

  private static long clampNonNegativeLong(long v) {
    return v < 0L ? 0L : v;
  }

  @Override
  public String toString() {
    return "NSSummary{dirName='" + dirName + '\'' +
        ", parentId=" + parentId +
        ", childDir=" + (childDir == null ? "[]" : childDir) +
        ", numOfFiles=" + numOfFiles +
        ", sizeOfFiles=" + sizeOfFiles +
        ", replicatedSizeOfFiles=" + replicatedSizeOfFiles +
        ", fileSizeBucket=" + Arrays.toString(fileSizeBucket) +
        '}';
  }
}
