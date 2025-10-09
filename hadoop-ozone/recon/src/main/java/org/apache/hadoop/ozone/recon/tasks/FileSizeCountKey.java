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

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.FileSizeCountKeyProto;

/**
 * Key class used for grouping file size counts in RocksDB storage.
 * Represents a composite key of (volume, bucket, fileSizeUpperBound) for
 * FILE_COUNT_BY_SIZE column family.
 */
public class FileSizeCountKey {
  private static final Codec<FileSizeCountKey> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(FileSizeCountKeyProto.getDefaultInstance()),
      FileSizeCountKey::fromProto,
      FileSizeCountKey::toProto,
      FileSizeCountKey.class);

  private final String volume;
  private final String bucket;
  private final Long fileSizeUpperBound;

  public FileSizeCountKey(String volume, String bucket, Long fileSizeUpperBound) {
    this.volume = volume;
    this.bucket = bucket;
    this.fileSizeUpperBound = fileSizeUpperBound;
  }

  public static Codec<FileSizeCountKey> getCodec() {
    return CODEC;
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

  public FileSizeCountKeyProto toProto() {
    return FileSizeCountKeyProto.newBuilder()
        .setVolume(volume)
        .setBucket(bucket)
        .setFileSizeUpperBound(fileSizeUpperBound)
        .build();
  }

  public static FileSizeCountKey fromProto(FileSizeCountKeyProto proto) {
    return new FileSizeCountKey(
        proto.getVolume(),
        proto.getBucket(),
        proto.getFileSizeUpperBound()
    );
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileSizeCountKey that = (FileSizeCountKey) o;
    if (!volume.equals(that.volume)) {
      return false;
    }
    if (!bucket.equals(that.bucket)) {
      return false;
    }
    return fileSizeUpperBound.equals(that.fileSizeUpperBound);
  }

  @Override
  public int hashCode() {
    int result = volume.hashCode();
    result = 31 * result + bucket.hashCode();
    result = 31 * result + fileSizeUpperBound.hashCode();
    return result;
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
