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

package org.apache.hadoop.ozone.recon.tasks;

/**
 * Key class used for grouping file size counts in RocksDB storage.
 * Represents a composite key of (volume, bucket, fileSizeUpperBound) for
 * FILE_COUNT_BY_SIZE column family.
 */
public class FileSizeCountKey {
  private final String volume;
  private final String bucket;
  private final Long fileSizeUpperBound;

  public FileSizeCountKey(String volume, String bucket, Long fileSizeUpperBound) {
    this.volume = volume;
    this.bucket = bucket;
    this.fileSizeUpperBound = fileSizeUpperBound;
  }

  public String getVolume() {
    return volume;
  }

  public String getBucket() {
    return bucket;
  }

  public Long getFileSizeUpperBound() {
    return fileSizeUpperBound;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FileSizeCountKey) {
      FileSizeCountKey other = (FileSizeCountKey) obj;
      return volume.equals(other.volume) &&
          bucket.equals(other.bucket) &&
          fileSizeUpperBound.equals(other.fileSizeUpperBound);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return (volume + bucket + fileSizeUpperBound).hashCode();
  }

  @Override
  public String toString() {
    return "FileSizeCountKey{" +
        "volume='" + volume + '\'' +
        ", bucket='" + bucket + '\'' +
        ", fileSizeUpperBound=" + fileSizeUpperBound +
        '}';
  }
}
