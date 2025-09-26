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
import jakarta.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.ozone.recon.api.types.KeyPrefixContainer;

/**
 * Codec to serialize/deserialize {@link KeyPrefixContainer}.
 */
public final class KeyPrefixContainerCodec
    implements Codec<KeyPrefixContainer> {

  private static final Codec<KeyPrefixContainer> INSTANCE =
      new KeyPrefixContainerCodec();

  private static final String KEY_DELIMITER = "_";
  private static final byte[] KEY_DELIMITER_BYTES = KEY_DELIMITER.getBytes(UTF_8);
  private static final ByteBuffer KEY_DELIMITER_BUFFER = ByteBuffer.wrap(KEY_DELIMITER_BYTES).asReadOnlyBuffer();
  private static final int LONG_SERIALIZED_SIZE = KEY_DELIMITER_BYTES.length + Long.BYTES;

  public static Codec<KeyPrefixContainer> get() {
    return INSTANCE;
  }

  private KeyPrefixContainerCodec() {
    // singleton
  }

  @Override
  public Class<KeyPrefixContainer> getTypeClass() {
    return KeyPrefixContainer.class;
  }

  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull KeyPrefixContainer object, CodecBuffer.Allocator allocator) {
    Preconditions.checkNotNull(object, "Null object can't be converted to CodecBuffer.");

    final byte[] keyPrefixBytes = object.getKeyPrefix().getBytes(UTF_8);
    int totalSize = keyPrefixBytes.length;

    if (object.getKeyVersion() != -1) {
      totalSize += LONG_SERIALIZED_SIZE;
    }
    if (object.getContainerId() != -1) {
      totalSize += LONG_SERIALIZED_SIZE;
    }

    final CodecBuffer buffer = allocator.apply(totalSize);
    buffer.put(ByteBuffer.wrap(keyPrefixBytes));

    if (object.getKeyVersion() != -1) {
      buffer.put(KEY_DELIMITER_BUFFER.duplicate());
      buffer.putLong(object.getKeyVersion());
    }

    if (object.getContainerId() != -1) {
      buffer.put(KEY_DELIMITER_BUFFER.duplicate());
      buffer.putLong(object.getContainerId());
    }

    return buffer;
  }

  @Override
  public KeyPrefixContainer fromCodecBuffer(@Nonnull CodecBuffer buffer) throws CodecException {

    final ByteBuffer byteBuffer = buffer.asReadOnlyByteBuffer();
    final int totalLength = byteBuffer.remaining();
    final int startPosition = byteBuffer.position();
    final int delimiterLength = KEY_DELIMITER_BYTES.length;

    // Check for two delimiters + two longs (version and containerId)
    if (totalLength >= 2 * (delimiterLength + Long.BYTES)) {
      // Extract keyPrefix (everything except last 2 delimiters + 2 longs)
      int keyPrefixLength = totalLength - 2 * delimiterLength - 2 * Long.BYTES;
      byteBuffer.position(startPosition);
      String keyPrefix = decodeStringFromBuffer(byteBuffer, keyPrefixLength);

      // Skip delimiter and read version
      byteBuffer.position(startPosition + keyPrefixLength + delimiterLength);
      long version = byteBuffer.getLong();

      // Skip delimiter and read containerId
      byteBuffer.position(startPosition + keyPrefixLength + delimiterLength + Long.BYTES + delimiterLength);
      long containerId = byteBuffer.getLong();

      return KeyPrefixContainer.get(keyPrefix, version, containerId);
    }

    // Check for one delimiter + one long
    if (totalLength >= delimiterLength + Long.BYTES) {
      // Extract keyPrefix (everything except last delimiter + long)
      int keyPrefixLength = totalLength - delimiterLength - Long.BYTES;
      byteBuffer.position(startPosition);
      String keyPrefix = decodeStringFromBuffer(byteBuffer, keyPrefixLength);

      // Skip delimiter and read the long value
      byteBuffer.position(startPosition + keyPrefixLength + delimiterLength);
      long longValue = byteBuffer.getLong();

      // Based on encoding logic: if keyVersion != -1, it's encoded first, then containerId
      // So if we have only one long value, it should be the keyVersion
      return KeyPrefixContainer.get(keyPrefix, longValue, -1);
    }

    // If we reach here, the buffer contains only the key prefix (no delimiters found)
    byteBuffer.position(startPosition);
    String keyPrefix = decodeStringFromBuffer(byteBuffer, totalLength);
    return KeyPrefixContainer.get(keyPrefix, -1, -1);
  }

  /**
   * Decode string from ByteBuffer without copying to intermediate byte array.
   * Uses CharsetDecoder for efficient decoding.
   */
  private String decodeStringFromBuffer(ByteBuffer buffer, int length) throws CodecException {
    if (length == 0) {
      return "";
    }

    try {
      ByteBuffer slice = buffer.duplicate();
      slice.limit(slice.position() + length);

      buffer.position(buffer.position() + length);

      CharsetDecoder decoder = UTF_8.newDecoder();
      CharBuffer charBuffer = decoder.decode(slice);
      return charBuffer.toString();

    } catch (CharacterCodingException e) {
      throw new CodecException("Failed to decode UTF-8 string from buffer", e);
    }
  }

  @Override
  public byte[] toPersistedFormat(KeyPrefixContainer keyPrefixContainer) {
    Preconditions.checkNotNull(keyPrefixContainer,
            "Null object can't be converted to byte array.");
    byte[] keyPrefixBytes = keyPrefixContainer.getKeyPrefix().getBytes(UTF_8);

    //Prefix seek can be done only with keyPrefix. In that case, we can
    // expect the version and the containerId to be undefined.
    if (keyPrefixContainer.getKeyVersion() != -1) {
      keyPrefixBytes = ArrayUtils.addAll(keyPrefixBytes, KEY_DELIMITER_BYTES);
      keyPrefixBytes = ArrayUtils.addAll(keyPrefixBytes, Longs.toByteArray(
          keyPrefixContainer.getKeyVersion()));
    }

    if (keyPrefixContainer.getContainerId() != -1) {
      keyPrefixBytes = ArrayUtils.addAll(keyPrefixBytes, KEY_DELIMITER_BYTES);
      keyPrefixBytes = ArrayUtils.addAll(keyPrefixBytes, Longs.toByteArray(
          keyPrefixContainer.getContainerId()));
    }

    return keyPrefixBytes;
  }

  @Override
  public KeyPrefixContainer fromPersistedFormat(byte[] rawData) {
    int totalLength = rawData.length;
    int delimiterLength = KEY_DELIMITER_BYTES.length;

    // Check for two delimiters + two longs (version and containerId)
    if (totalLength >= 2 * (delimiterLength + Long.BYTES)) {
      // Extract keyPrefix (everything except last 2 delimiters + 2 longs)
      int keyPrefixLength = totalLength - 2 * delimiterLength - 2 * Long.BYTES;
      String keyPrefix = new String(ArrayUtils.subarray(rawData, 0, keyPrefixLength), UTF_8);

      // Read version (skip delimiter)
      int versionStart = keyPrefixLength + delimiterLength;
      byte[] versionBytes = ArrayUtils.subarray(rawData, versionStart, versionStart + Long.BYTES);
      long version = ByteBuffer.wrap(versionBytes).getLong();

      // Read containerId (skip delimiter)
      int containerIdStart = versionStart + Long.BYTES + delimiterLength;
      byte[] containerIdBytes = ArrayUtils.subarray(rawData, containerIdStart, containerIdStart + Long.BYTES);
      long containerId = ByteBuffer.wrap(containerIdBytes).getLong();

      return KeyPrefixContainer.get(keyPrefix, version, containerId);
    }

    // Check for one delimiter + one long (version only)
    if (totalLength >= delimiterLength + Long.BYTES) {
      // Extract keyPrefix (everything except last delimiter + long)
      int keyPrefixLength = totalLength - delimiterLength - Long.BYTES;
      String keyPrefix = new String(ArrayUtils.subarray(rawData, 0, keyPrefixLength), UTF_8);

      // Read version (skip delimiter)
      int versionStart = keyPrefixLength + delimiterLength;
      byte[] versionBytes = ArrayUtils.subarray(rawData, versionStart, versionStart + Long.BYTES);
      long version = ByteBuffer.wrap(versionBytes).getLong();

      return KeyPrefixContainer.get(keyPrefix, version, -1);
    }

    // If we reach here, the buffer contains only the key prefix (no delimiters found)
    String keyPrefix = new String(rawData, UTF_8);
    return KeyPrefixContainer.get(keyPrefix, -1, -1);
  }

  @Override
  public KeyPrefixContainer copyObject(KeyPrefixContainer object) {
    return object;
  }
}
