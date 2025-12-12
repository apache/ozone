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

package org.apache.hadoop.hdds.utils.db.managed;

import com.google.common.primitives.UnsignedLong;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.IteratorType;
import org.apache.hadoop.ozone.util.ClosableIterator;

/**
 * Iterator for SSTFileReader which would read all entries including tombstones.
 */
public class ManagedRawSSTFileIterator<T> implements ClosableIterator<T> {
  // Native address of pointer to the object.
  private final long nativeHandle;
  private final Function<KeyValue, T> transformer;
  private final IteratorType type;

  ManagedRawSSTFileIterator(long nativeHandle, Function<KeyValue, T> transformer, IteratorType type) {
    this.nativeHandle = nativeHandle;
    this.transformer = transformer;
    this.type = type;
  }

  private native boolean hasNext(long handle);

  private native void next(long handle);

  private native byte[] getKey(long handle);

  private native byte[] getValue(long handle);

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

    KeyValue keyValue = new KeyValue(this.type.readKey() ? this.getKey(nativeHandle) : null,
        UnsignedLong.fromLongBits(this.getSequenceNumber(this.nativeHandle)),
        this.getType(nativeHandle),
        this.type.readValue() ? this.getValue(nativeHandle) : null);
    this.next(nativeHandle);
    return this.transformer.apply(keyValue);
  }

  private native void closeInternal(long handle);

  @Override
  public void close() {
    this.closeInternal(this.nativeHandle);
  }

  /**
   * Class containing Parsed KeyValue Record from RawSstReader output.
   */
  public static final class KeyValue {

    private final byte[] key;
    private final UnsignedLong sequence;
    private final Integer type;
    private final byte[] value;

    private KeyValue(byte[] key, UnsignedLong sequence, Integer type,
                     byte[] value) {
      this.key = key;
      this.sequence = sequence;
      this.type = type;
      this.value = value;
    }

    public byte[] getKey() {
      return key == null ? null : Arrays.copyOf(key, key.length);
    }

    public UnsignedLong getSequence() {
      return sequence;
    }

    public Integer getType() {
      return type;
    }

    public byte[] getValue() {
      return value == null ? null : Arrays.copyOf(value, value.length);
    }

    @Override
    public String toString() {
      return "KeyValue{" +
          "key=" + (key == null ? null : StringUtils.bytes2String(key)) +
          ", sequence=" + sequence +
          ", type=" + type +
          ", value=" + (value == null ? null : StringUtils.bytes2String(value)) +
          '}';
    }
  }
}
