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

package org.apache.hadoop.hdds.scm.metadata;

import jakarta.annotation.Nonnull;
import org.apache.hadoop.hdds.scm.ha.SequenceIdType;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.StringCodec;

/**
 * Codec to serialize/deserialize {@link SequenceIdType}.
 */
public final class SequenceIdTypeCodec implements Codec<SequenceIdType> {

  private static final Codec<SequenceIdType> INSTANCE = new SequenceIdTypeCodec();
  private static final Codec<String> STRING_CODEC = StringCodec.get();

  public static Codec<SequenceIdType> get() {
    return INSTANCE;
  }

  private SequenceIdTypeCodec() {
    // singleton
  }

  @Override
  public Class<SequenceIdType> getTypeClass() {
    return SequenceIdType.class;
  }

  @Override
  public byte[] toPersistedFormat(SequenceIdType object) throws CodecException {
    return STRING_CODEC.toPersistedFormat(object.name());
  }

  @Override
  public SequenceIdType fromPersistedFormat(byte[] rawData) throws CodecException {
    return SequenceIdType.valueOf(STRING_CODEC.fromPersistedFormat(rawData));
  }

  @Override
  public boolean supportCodecBuffer() {
    return STRING_CODEC.supportCodecBuffer();
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull SequenceIdType object,
                                   CodecBuffer.Allocator allocator) throws CodecException {
    return STRING_CODEC.toCodecBuffer(object.name(), allocator);
  }

  @Override
  public SequenceIdType fromCodecBuffer(@Nonnull CodecBuffer buffer) throws CodecException {
    return SequenceIdType.valueOf(STRING_CODEC.fromCodecBuffer(buffer));
  }

  @Override
  public SequenceIdType copyObject(SequenceIdType object) {
    return object;
  }
}
