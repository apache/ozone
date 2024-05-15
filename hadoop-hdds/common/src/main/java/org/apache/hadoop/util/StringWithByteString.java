/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.util;

import com.google.protobuf.ByteString;
import net.jcip.annotations.Immutable;

import java.util.Objects;

/**
 * Class to encapsulate and cache the conversion of a Java String to a ByteString.
 */
@Immutable
public final class StringWithByteString {
  public static StringWithByteString valueOf(String string) {
    return string != null ? new StringWithByteString(string, ByteString.copyFromUtf8(string)) : null;
  }

  private final String string;
  private final ByteString bytes;

  public StringWithByteString(String string, ByteString bytes) {
    this.string = Objects.requireNonNull(string, "string == null");
    this.bytes = Objects.requireNonNull(bytes, "bytes == null");
  }

  public String getString() {
    return string;
  }

  public ByteString getBytes() {
    return bytes;
  }

  @Override
  public String toString() {
    return getString();
  }
}
