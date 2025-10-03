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

package org.apache.hadoop.ozone.recon.spi.impl;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.recon.tasks.FileSizeCountKey;

/**
 * Codec to serialize/deserialize {@link FileSizeCountKey}.
 */
public final class FileCountBySizeKeyCodec implements Codec<FileSizeCountKey> {

  private static final String KEY_DELIMITER = "/";

  private static final Codec<FileSizeCountKey> INSTANCE = new FileCountBySizeKeyCodec();

  public static Codec<FileSizeCountKey> get() {
    return INSTANCE;
  }

  private FileCountBySizeKeyCodec() {
    // singleton
  }

  @Override
  public Class<FileSizeCountKey> getTypeClass() {
    return FileSizeCountKey.class;
  }

  @Override
  public byte[] toPersistedFormat(FileSizeCountKey key) {
    Preconditions.checkNotNull(key, "Null object can't be converted to byte array.");
    
    // Serialize: volume + "/" + bucket + "/" + fileSize (8 bytes)
    // Using "/" as delimiter (consistent with OZONE_URI_DELIMITER and OM_KEY_PREFIX)
    byte[] volumeBytes = key.getVolume().getBytes(UTF_8);
    byte[] bucketBytes = key.getBucket().getBytes(UTF_8);
    byte[] fileSizeBytes = Longs.toByteArray(key.getFileSizeUpperBound());
    byte[] delimiterBytes = KEY_DELIMITER.getBytes(UTF_8);

    byte[] result = ArrayUtils.addAll(volumeBytes, delimiterBytes);
    result = ArrayUtils.addAll(result, bucketBytes);
    result = ArrayUtils.addAll(result, delimiterBytes);
    result = ArrayUtils.addAll(result, fileSizeBytes);

    return result;
  }

  @Override
  public FileSizeCountKey fromPersistedFormat(byte[] rawData) {
    // Last 8 bytes is the file size (long)
    byte[] fileSizeBytes = ArrayUtils.subarray(rawData,
        rawData.length - Long.BYTES,
        rawData.length);
    long fileSize = Longs.fromByteArray(fileSizeBytes);
    
    // Everything before the last 8 bytes contains volume + delimiter + bucket + delimiter
    byte[] volumeBucketBytes = ArrayUtils.subarray(rawData, 0, rawData.length - Long.BYTES);
    String volumeBucketString = new String(volumeBucketBytes, UTF_8);
    
    // Find the first delimiter to separate volume from bucket
    int firstDelimiter = volumeBucketString.indexOf(KEY_DELIMITER);
    int secondDelimiter = volumeBucketString.indexOf(KEY_DELIMITER, firstDelimiter + 1);
    
    String volume = volumeBucketString.substring(0, firstDelimiter);
    String bucket = volumeBucketString.substring(firstDelimiter + 1, secondDelimiter);
    
    return new FileSizeCountKey(volume, bucket, fileSize);
  }

  @Override
  public FileSizeCountKey copyObject(FileSizeCountKey object) {
    return new FileSizeCountKey(
        object.getVolume(),
        object.getBucket(), 
        object.getFileSizeUpperBound()
    );
  }
}
