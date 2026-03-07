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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.Codec;

/**
 * Typed key for multipart parts table.
 * Key encoding:
 * <pre>
 *   uploadId(utf8) + '/' + partNumber(int32, big-endian)
 * </pre>
 * Prefix encoding for iteration:
 * <pre>
 *   uploadId(utf8) + '/'
 * </pre>
 */
public final class OmMultipartPartKey {
  private static final byte SEPARATOR = (byte) '/';
  private static final Codec<OmMultipartPartKey> CODEC =
      new OmMultipartPartKeyCodec();

  private final String uploadId;
  private final Integer partNumber;

  private OmMultipartPartKey(String uploadId, Integer partNumber) {
    this.uploadId = Objects.requireNonNull(uploadId, "uploadId is null");
    this.partNumber = partNumber;
  }

  public static OmMultipartPartKey of(String uploadId, int partNumber) {
    return new OmMultipartPartKey(uploadId, partNumber);
  }

  public static OmMultipartPartKey prefix(String uploadId) {
    return new OmMultipartPartKey(uploadId, null);
  }

  public static Codec<OmMultipartPartKey> getCodec() {
    return CODEC;
  }

  public String getUploadId() {
    return uploadId;
  }

  public Integer getPartNumber() {
    return partNumber;
  }

  public boolean hasPartNumber() {
    return partNumber != null;
  }

  @Override
  public String toString() {
    return hasPartNumber() ? uploadId + "/" + partNumber : uploadId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OmMultipartPartKey)) {
      return false;
    }
    OmMultipartPartKey that = (OmMultipartPartKey) o;
    return Objects.equals(uploadId, that.uploadId)
        && Objects.equals(partNumber, that.partNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hash(uploadId, partNumber);
  }

  private static final class OmMultipartPartKeyCodec
      implements Codec<OmMultipartPartKey> {

    @Override
    public Class<OmMultipartPartKey> getTypeClass() {
      return OmMultipartPartKey.class;
    }

    /**
     * Encodes the OmMultipartPartKey object into a byte array for storage in the key/value store.
     * Key format:
     *   prefix key: uploadId + '/'
     *   full key:   uploadId + '/' + int32(partNumber)
     * @param key The original java object. Should not be null.
     * @return Byte array representation of the object for storage in the key/value store.
     */
    @Override
    public byte[] toPersistedFormat(OmMultipartPartKey key) {
      byte[] uploadBytes = key.uploadId.getBytes(StandardCharsets.UTF_8);
      int size = uploadBytes.length + 1
          + (key.hasPartNumber() ? Integer.BYTES : 0);
      ByteBuffer buffer = ByteBuffer.allocate(size);
      buffer.put(uploadBytes);
      buffer.put(SEPARATOR);
      if (key.hasPartNumber()) {
        buffer.putInt(key.partNumber);
      }
      return buffer.array();
    }

    /**
     * Decodes the raw byte array from the key/value store into an OmMultipartPartKey object.
     * @param rawData Byte array from the key/value store. Should not be null.
     * @return OmMultipartPartKey object represented by the raw byte array.
     * @throws IllegalArgumentException if the rawData format is invalid
     */
    @Override
    public OmMultipartPartKey fromPersistedFormat(byte[] rawData) throws IllegalArgumentException {
      if (rawData.length == 0) {
        throw new IllegalArgumentException(
            "Invalid multipart part key: empty key");
      }

      //   prefix key: uploadId + '/'
      //   full key:   uploadId + '/' + int32(partNumber)
      int suffixLength = getSuffixLength(rawData);

      int separatorIndex = rawData.length - suffixLength - 1;
      if (separatorIndex < 0) {
        throw new IllegalArgumentException(
            "Invalid multipart part key: invalid separator position");
      }
      String uploadId = new String(rawData, 0, separatorIndex,
          StandardCharsets.UTF_8);
      if (suffixLength == 0) {
        return new OmMultipartPartKey(uploadId, null);
      }
      if (rawData.length - (separatorIndex + 1) != Integer.BYTES) {
        throw new IllegalArgumentException(
            "Invalid multipart part key: unexpected part suffix length");
      }

      int part = ByteBuffer.wrap(
          rawData, separatorIndex + 1, Integer.BYTES).getInt();
      return of(uploadId, part);
    }

    @Override
    public OmMultipartPartKey copyObject(OmMultipartPartKey object) {
      return object;
    }
  }

  /**
   * Determines the length of the suffix (part number) in the raw key data.
   * Check full-key first: if byte at len - 5 (size of int is 4) is /, decode as full key (used to identify full row)
   * Else, if byte at len - 1 is /, decode as prefix. (this is used for prefix scan for iterating)
   * Else invalid.
   * @param rawData the raw byte array representing the key
   * @return the length of the suffix (0 for prefix keys, Integer.BYTES for full keys)
   * @throws IllegalArgumentException if the key format is invalid (missing separator or unexpected suffix length)
   */
  private static int getSuffixLength(byte[] rawData) throws IllegalArgumentException {
    int suffixLength = -1;
    // Check full-key layout first. Otherwise, part numbers whose low byte is
    // '/' (for example 47 -> 0x0000002f) are mis-classified as prefix keys.
    if (rawData.length > Integer.BYTES
        && rawData[rawData.length - Integer.BYTES - 1] == SEPARATOR) {
      suffixLength = Integer.BYTES;
    } else if (rawData[rawData.length - 1] == SEPARATOR) {
      suffixLength = 0;
    }
    if (suffixLength < 0) {
      throw new IllegalArgumentException(
          "Invalid multipart part key: missing separator");
    }
    return suffixLength;
  }
}
