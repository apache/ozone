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

import java.util.Arrays;
import java.util.Comparator;

/**
 * An immutable half-open key range {@code [start, end)} in RocksDB byte order
 * (unsigned lexicographic). A null {@code start} means unbounded below and a
 * null {@code end} means unbounded above.
 */
public final class KeyRange {
  /** Unsigned lexicographic comparator, the default RocksDB comparator. */
  public static final Comparator<byte[]> COMPARATOR = KeyRange::compare;

  private final byte[] start;
  private final byte[] end;

  public KeyRange(byte[] start, byte[] end) {
    this.start = copy(start);
    this.end = copy(end);
    if (this.start != null && this.end != null
        && compare(this.start, this.end) > 0) {
      throw new IllegalArgumentException("start > end: " + this);
    }
  }

  /** @return the inclusive start key, or null if unbounded. */
  public byte[] getStart() {
    return copy(start);
  }

  /** @return the exclusive end key, or null if unbounded. */
  public byte[] getEnd() {
    return copy(end);
  }

  /** @return true iff {@code key} is strictly inside {@code (start, end)}. */
  boolean isStrictlyInside(byte[] key) {
    return (start == null || compare(key, start) > 0)
        && (end == null || compare(key, end) < 0);
  }

  public boolean contains(byte[] key) {
    return (start == null || compare(key, start) >= 0)
        && (end == null || compare(key, end) < 0);
  }

  static int compare(byte[] a, byte[] b) {
    final int n = Math.min(a.length, b.length);
    for (int i = 0; i < n; i++) {
      final int c = (a[i] & 0xff) - (b[i] & 0xff);
      if (c != 0) {
        return c;
      }
    }
    return a.length - b.length;
  }

  private static byte[] copy(byte[] b) {
    return b == null ? null : Arrays.copyOf(b, b.length);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KeyRange)) {
      return false;
    }
    final KeyRange that = (KeyRange) o;
    return Arrays.equals(start, that.start) && Arrays.equals(end, that.end);
  }

  @Override
  public int hashCode() {
    return 31 * Arrays.hashCode(start) + Arrays.hashCode(end);
  }

  @Override
  public String toString() {
    return "[" + (start == null ? "-inf" : toHex(start)) + ", "
        + (end == null ? "+inf" : toHex(end)) + ")";
  }

  private static String toHex(byte[] b) {
    final StringBuilder sb = new StringBuilder();
    for (byte x : b) {
      sb.append(String.format("%02x", x & 0xFF));
    }
    return sb.toString();
  }
}
