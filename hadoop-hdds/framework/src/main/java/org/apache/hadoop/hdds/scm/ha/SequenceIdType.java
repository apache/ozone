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

package org.apache.hadoop.hdds.scm.ha;

import jakarta.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.StringCodec;

/**
 * Represents the sequence ID types managed by
 * {@code org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator}
 * The enum constant names are kept exactly as their persisted RocksDB keys.
 */
public enum SequenceIdType {

  localId,
  delTxnId,
  containerId,

  /**
   * Certificate ID for all services, including root certificates.
   */
  CertificateId,

  /**
   * @deprecated Use {@link #CertificateId} instead.
   */
  @Deprecated
  rootCertificateId;

  private static final Codec<SequenceIdType> INSTANCE = new Codec<SequenceIdType>() {
    @Override
    public Class<SequenceIdType> getTypeClass() {
      return SequenceIdType.class;
    }

    @Override
    public boolean supportCodecBuffer() {
      return true;
    }

    @Override
    public byte[] toPersistedFormat(SequenceIdType type) {
      return type.getByteArray();
    }

    @Override
    public SequenceIdType fromPersistedFormat(byte[] bytes) throws CodecException {
      final SequenceIdType type = SEQUENCE_ID_TYPES.get(bytes[0]);
      if (type != null && Arrays.equals(type.getByteArray(), bytes)) {
        return type;
      }
      throw new CodecException("Failed to decode " + StringUtils.bytes2Hex(ByteBuffer.wrap(bytes), 20));
    }

    @Override
    public CodecBuffer toCodecBuffer(@Nonnull SequenceIdType object, CodecBuffer.Allocator allocator) {
      final ByteBuffer buffer = object.getByteBuffer();
      final CodecBuffer cb = allocator.apply(buffer.remaining());
      cb.put(buffer);
      return cb;
    }

    @Override
    public SequenceIdType fromCodecBuffer(@Nonnull CodecBuffer bytes) throws CodecException {
      final ByteBuffer buffer = bytes.asReadOnlyByteBuffer();
      final SequenceIdType type = SEQUENCE_ID_TYPES.get(buffer.get(buffer.position()));
      if (type != null && type.getByteBuffer().equals(buffer)) {
        return type;
      }
      throw new CodecException("Failed to decode " + StringUtils.bytes2Hex(buffer, 20));

    }

    @Override
    public SequenceIdType copyObject(SequenceIdType object) {
      return object;
    }
  };

  /** Only use the first byte in the name since they are all distinct. */
  private static final Map<Byte, SequenceIdType> SEQUENCE_ID_TYPES;

  private final byte[] byteArray;
  private final ByteBuffer byteBuffer;

  SequenceIdType() {
    try {
      this.byteArray = StringCodec.getCodecNoFallback().toPersistedFormat(name());
    } catch (CodecException e) {
      throw new IllegalStateException("Failed to construct " + this, e);
    }

    this.byteBuffer = ByteBuffer.wrap(byteArray).asReadOnlyBuffer();
  }

  public byte[] getByteArray() {
    return byteArray.clone();
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer.duplicate();
  }

  static {
    final Map<Byte, SequenceIdType> map = new HashMap<>();
    for (SequenceIdType type : SequenceIdType.values()) {
      final byte first =  type.getByteArray()[0];
      final SequenceIdType previous = map.put(first, type);
      if (previous != null) {
        throw new IllegalStateException("Duplicated first byte: " + type + " and " + previous);
      }
    }
    SEQUENCE_ID_TYPES = Collections.unmodifiableMap(map);
  }

  public static Codec<SequenceIdType> getCodec() {
    return INSTANCE;
  }
}
