/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.utils.db;

import org.apache.ratis.util.function.CheckedFunction;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.function.IntFunction;

/**
 * A {@link Codec} to serialize/deserialize objects by delegation.
 *
 * @param <T>        The object type of this {@link Codec}.
 * @param <DELEGATE> The object type of the {@link #delegate}.
 */
public class DelegatedCodec<T, DELEGATE> implements Codec<T> {
  private final Codec<DELEGATE> delegate;
  private final CheckedFunction<DELEGATE, T, IOException> forward;
  private final CheckedFunction<T, DELEGATE, IOException> backward;
  private final boolean shallowCopy;

  /**
   * Construct a {@link Codec} using the given delegate.
   *
   * @param delegate the delegate {@link Codec}
   * @param forward a function to convert {@link DELEGATE} to {@link T}.
   * @param backward a function to convert {@link T} back to {@link DELEGATE}.
   * @param shallowCopy Should it use shallow copy
   *                    in {@link #copyObject(Object)}?
   */
  public DelegatedCodec(Codec<DELEGATE> delegate,
      CheckedFunction<DELEGATE, T, IOException> forward,
      CheckedFunction<T, DELEGATE, IOException> backward,
      boolean shallowCopy) {
    this.delegate = delegate;
    this.forward = forward;
    this.backward = backward;
    this.shallowCopy = shallowCopy;
  }

  /** The same as new DelegatedCodec(delegate, forward, backward, false). */
  public DelegatedCodec(Codec<DELEGATE> delegate,
      CheckedFunction<DELEGATE, T, IOException> forward,
      CheckedFunction<T, DELEGATE, IOException> backward) {
    this(delegate, forward, backward, false);
  }

  @Override
  public final boolean supportCodecBuffer() {
    return delegate.supportCodecBuffer();
  }

  @Override
  public final CodecBuffer toCodecBuffer(@Nonnull T message,
      IntFunction<CodecBuffer> allocator) throws IOException {
    return delegate.toCodecBuffer(backward.apply(message), allocator);
  }

  @Override
  public final T fromCodecBuffer(@Nonnull CodecBuffer buffer)
      throws IOException {
    return forward.apply(delegate.fromCodecBuffer(buffer));
  }

  @Override
  public final byte[] toPersistedFormat(T message) throws IOException {
    return delegate.toPersistedFormat(backward.apply(message));
  }

  @Override
  public final T fromPersistedFormat(byte[] bytes) throws IOException {
    return forward.apply(delegate.fromPersistedFormat(bytes));
  }

  @Override
  public T copyObject(T message) {
    if (shallowCopy) {
      return message;
    }
    try {
      return forward.apply(delegate.copyObject(backward.apply(message)));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to copyObject", e);
    }
  }
}
