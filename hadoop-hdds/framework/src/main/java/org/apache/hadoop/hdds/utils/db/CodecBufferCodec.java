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

package org.apache.hadoop.hdds.utils.db;

import jakarta.annotation.Nonnull;
import java.nio.ByteBuffer;

/**
 * A concrete implementation of the Codec interface for the CodecBuffer type.
 * This class provides methods to serialize and deserialize CodecBuffer
 * objects to and from byte arrays and to interact with CodecBuffer instances
 * using different allocation strategies (direct or heap).
 *
 * The CodecBufferCodec distinguishes between direct and non-direct
 * (heap-based) buffers, ensuring compatibility between the provided allocator
 * and buffer type during serialization and deserialization.
 *
 * This codec supports CodecBuffer-based methods.
 * NOTE: This codec does not create copies of CodecBuffer objects and it returns the CodecBuffer object itself
 * consumer of this codec and thus the caller should not close the CodecBuffer object in case of
 * {@link #copyObject(CodecBuffer)} and {@link #toCodecBuffer(CodecBuffer, CodecBuffer.Allocator)} methods. This has
 * been done to avoid unnecessary memory allocations.
 * However, it still has to handle lifecyle of CodecBuffer returned by {@link #fromPersistedFormat(byte[])} method.
 */
public final class CodecBufferCodec implements Codec<CodecBuffer> {

  private static final Codec<CodecBuffer> DIRECT_INSTANCE = new CodecBufferCodec(true);
  private static final Codec<CodecBuffer> NON_DIRECT_INSTANCE = new CodecBufferCodec(false);

  private final CodecBuffer.Allocator allocator;

  public static Codec<CodecBuffer> get(boolean direct) {
    return direct ? DIRECT_INSTANCE : NON_DIRECT_INSTANCE;
  }

  private CodecBufferCodec(boolean direct) {
    this.allocator = direct ? CodecBuffer.Allocator.getDirect() : CodecBuffer.Allocator.getHeap();
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull CodecBuffer object, CodecBuffer.Allocator customAllocator) {
    if (customAllocator.isDirect() != object.isDirect()) {
      throw new IllegalArgumentException("Custom allocator must be of the same type as the object");
    }
    return object;
  }

  @Override
  public CodecBuffer fromCodecBuffer(@Nonnull CodecBuffer buffer) {
    return buffer;
  }

  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  @Override
  public Class<CodecBuffer> getTypeClass() {
    return CodecBuffer.class;
  }

  @Override
  public byte[] toPersistedFormat(CodecBuffer buffer) {
    return buffer.getArray();
  }

  @Override
  public CodecBuffer fromPersistedFormat(byte[] bytes) {
    return this.allocator.apply(bytes.length).put(ByteBuffer.wrap(bytes));
  }

  @Override
  public CodecBuffer copyObject(CodecBuffer buffer) {
    return buffer;
  }
}
