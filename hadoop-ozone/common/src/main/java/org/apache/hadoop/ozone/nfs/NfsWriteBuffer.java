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

package org.apache.hadoop.ozone.nfs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/** Buffers bounded out-of-order writes and releases only the contiguous prefix. */
public final class NfsWriteBuffer {
  private final int maxBufferedBytes;
  private final int maxRanges;
  private final NavigableMap<Long, byte[]> pending = new TreeMap<>();
  private long nextOffset;
  private int bufferedBytes;

  public NfsWriteBuffer(int maxBufferedBytes, int maxRanges) {
    if (maxBufferedBytes < 1 || maxRanges < 1) {
      throw new IllegalArgumentException("NFS write buffer limits must be positive");
    }
    this.maxBufferedBytes = maxBufferedBytes;
    this.maxRanges = maxRanges;
  }

  /** Add a write and return the contiguous prefix available for the Ozone output stream. */
  public synchronized byte[] offer(long offset, byte[] data) {
    if (offset < nextOffset || offset < 0 || data == null || data.length == 0
        || offset > Long.MAX_VALUE - data.length) {
      throw new IllegalArgumentException("Invalid or already streamed NFS write range");
    }
    long mergedStart = offset;
    long mergedEnd = offset + data.length;
    List<Long> mergedRanges = new ArrayList<>();
    int mergedBytes = 0;
    Map.Entry<Long, byte[]> entry = pending.floorEntry(mergedStart);
    if (entry == null || end(entry) < mergedStart) {
      entry = pending.ceilingEntry(mergedStart);
    }
    while (entry != null && entry.getKey() <= mergedEnd) {
      long rangeStart = entry.getKey();
      long rangeEnd = end(entry);
      if (rangeEnd >= mergedStart) {
        mergedRanges.add(rangeStart);
        mergedBytes += entry.getValue().length;
        mergedStart = Math.min(mergedStart, rangeStart);
        mergedEnd = Math.max(mergedEnd, rangeEnd);
      }
      entry = pending.higherEntry(rangeStart);
    }

    long mergedLength = mergedEnd - mergedStart;
    int remainingBytes = bufferedBytes - mergedBytes;
    int remainingRanges = pending.size() - mergedRanges.size();
    if (mergedLength > Integer.MAX_VALUE || remainingBytes + mergedLength > maxBufferedBytes
        || remainingRanges + 1 > maxRanges) {
      throw new IllegalArgumentException("NFS write reorder buffer limit exceeded");
    }

    byte[] merged = new byte[(int) mergedLength];
    for (Long rangeStart : mergedRanges) {
      byte[] range = pending.get(rangeStart);
      int mergedOffset = (int) (rangeStart - mergedStart);
      System.arraycopy(range, 0, merged, mergedOffset, range.length);
      long overlapStart = Math.max(offset, rangeStart);
      long overlapEnd = Math.min(offset + data.length, rangeStart + range.length);
      for (long position = overlapStart; position < overlapEnd; position++) {
        if (data[(int) (position - offset)] != range[(int) (position - rangeStart)]) {
          throw new IllegalArgumentException("Conflicting overlapping NFS writes");
        }
      }
    }
    System.arraycopy(data, 0, merged, (int) (offset - mergedStart), data.length);
    for (Long rangeStart : mergedRanges) {
      pending.remove(rangeStart);
    }
    pending.put(mergedStart, merged);
    bufferedBytes = remainingBytes + merged.length;

    return contiguousBytes();
  }

  /** Confirm bytes only after the Ozone output stream has accepted them. */
  public synchronized void acknowledgeWritten(int count) {
    Map.Entry<Long, byte[]> entry = pending.firstEntry();
    if (count < 1 || entry == null || entry.getKey() != nextOffset || count > entry.getValue().length) {
      throw new IllegalArgumentException("Invalid acknowledged NFS write length");
    }
    byte[] range = entry.getValue();
    pending.pollFirstEntry();
    if (count < range.length) {
      byte[] remaining = new byte[range.length - count];
      System.arraycopy(range, count, remaining, 0, remaining.length);
      pending.put(nextOffset + count, remaining);
    }
    bufferedBytes -= count;
    nextOffset += count;
  }

  private byte[] contiguousBytes() {
    Map.Entry<Long, byte[]> entry = pending.firstEntry();
    return entry != null && entry.getKey() == nextOffset ? entry.getValue().clone() : new byte[0];
  }

  public synchronized long getNextOffset() {
    return nextOffset;
  }

  public synchronized int getBufferedBytes() {
    return bufferedBytes;
  }

  private static long end(Map.Entry<Long, byte[]> entry) {
    return entry.getKey() + entry.getValue().length;
  }
}
