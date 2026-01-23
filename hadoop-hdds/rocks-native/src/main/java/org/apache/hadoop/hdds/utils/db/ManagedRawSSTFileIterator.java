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

import com.google.common.primitives.UnsignedLong;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.hadoop.ozone.util.ClosableIterator;

/**
 * Iterator for SSTFileReader which would read all entries including tombstones.
 */
public class ManagedRawSSTFileIterator<T> implements ClosableIterator<T> {
  // Native address of pointer to the object.
  private final long nativeHandle;
  private final Function<KeyValue, T> transformer;
  private final IteratorType type;
  private boolean closed;
  private final Buffer keyBuffer;
  private final Buffer valueBuffer;

  ManagedRawSSTFileIterator(String name, long nativeHandle, Function<KeyValue, T> transformer, IteratorType type) {
    this.nativeHandle = nativeHandle;
    this.transformer = transformer;
    this.type = type;
    this.closed = false;
    this.keyBuffer = new Buffer(
        new CodecBuffer.Capacity(name + " iterator-key", 1 << 10),
        this.type.readKey() ? buffer -> this.getKey(this.nativeHandle, buffer, buffer.position(),
            buffer.remaining()) : null);
    this.valueBuffer = new Buffer(
        new CodecBuffer.Capacity(name + " iterator-value", 4 << 10),
        this.type.readValue() ? buffer -> this.getValue(this.nativeHandle, buffer, buffer.position(),
            buffer.remaining()) : null);
  }

  private native boolean hasNext(long handle);

  private native void next(long handle);

  private native int getKey(long handle, ByteBuffer buffer, int bufferOffset, int bufferLen);

  private native int getValue(long handle, ByteBuffer buffer, int bufferOffset, int bufferLen);

  private native long getSequenceNumber(long handle);

  private native int getType(long handle);

  @Override
  public boolean hasNext() {
    return this.hasNext(nativeHandle);
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    KeyValue keyValue = new KeyValue(this.type.readKey() ? this.keyBuffer.getFromDb() : null,
        UnsignedLong.fromLongBits(this.getSequenceNumber(this.nativeHandle)),
        this.getType(nativeHandle),
        this.type.readValue() ? this.valueBuffer.getFromDb() : null);
    this.next(nativeHandle);
    return this.transformer.apply(keyValue);
  }

  private native void closeInternal(long handle);

  @Override
  public synchronized void close() {
    if (!closed) {
      this.closeInternal(this.nativeHandle);
      keyBuffer.release();
      valueBuffer.release();
    }
    closed = true;
  }

  /**
   * Class containing Parsed KeyValue Record from RawSstReader output.
   */
  public static final class KeyValue {

    private final CodecBuffer key;
    private final UnsignedLong sequence;
    private final Integer type;
    private final CodecBuffer value;

    private KeyValue(CodecBuffer key, UnsignedLong sequence, Integer type,
                     CodecBuffer value) {
      this.key = key;
      this.sequence = sequence;
      this.type = type;
      this.value = value;
    }

    public CodecBuffer getKey() {
      return this.key;
    }

    public UnsignedLong getSequence() {
      return sequence;
    }

    public Integer getType() {
      return type;
    }

    public CodecBuffer getValue() {
      return value;
    }

    @Override
    public String toString() {
      return "KeyValue{" +
          "key=" + (key == null ? null : StringCodec.get().fromCodecBuffer(key)) +
          ", sequence=" + sequence +
          ", type=" + type +
          ", value=" + (value == null ? null : StringCodec.get().fromCodecBuffer(value)) +
          '}';
    }
  }
}
