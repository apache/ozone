/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.protobuf;

/** Utilities for protobuf v2. */
public final class Proto2Utils {
  /**
   * Similar to {@link ByteString#copyFrom(byte[])} except that this method does not copy.
   * This method is safe only if the content of the array remains unchanged.
   * Otherwise, it violates the immutability of {@link ByteString}.
   */
  public static ByteString unsafeByteString(byte[] array) {
    return array != null && array.length > 0 ? new LiteralByteString(array) : ByteString.EMPTY;
  }

  private Proto2Utils() { }
}
