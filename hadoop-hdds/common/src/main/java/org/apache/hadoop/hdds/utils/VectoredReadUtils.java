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

package org.apache.hadoop.hdds.utils;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;

/**
 * Utility class for vectored read operations.
 */
@InterfaceAudience.Private
public final class VectoredReadUtils {
  private VectoredReadUtils() {
    // Utility class, no instances
  }

  /**
   * Functional interface for reading data into a buffer from a specific offset.
   */
  @FunctionalInterface
  public interface RangeReader {
    /**
     * Read data from the specified offset into the buffer.
     *
     * @param offset the offset to read from
     * @param buffer the buffer to read into
     * @throws IOException if an error occurs during reading
     */
    void readRange(long offset, ByteBuffer buffer) throws IOException;
  }

  /**
   * Validates the ranges for vectored read operations.
   * Checks for null ranges, negative offsets, negative lengths, and overlapping ranges.
   *
   * @param ranges list of file ranges to validate
   * @throws NullPointerException if ranges list is null or contains null elements
   * @throws EOFException if any range has a negative offset
   * @throws IllegalArgumentException if any range has negative length or ranges overlap
   */
  public static void validateRanges(List<? extends FileRange> ranges) throws IOException {
    if (ranges == null) {
      throw new NullPointerException("Ranges list cannot be null");
    }
    for (int i = 0; i < ranges.size(); i++) {
      FileRange range = ranges.get(i);
      if (range == null) {
        throw new NullPointerException("Range at index " + i + " is null");
      }
      // Check for negative offset
      if (range.getOffset() < 0) {
        throw new EOFException("Range " + i + " has negative offset: " + range.getOffset());
      }
      // Check for negative length
      if (range.getLength() < 0) {
        throw new IllegalArgumentException("Range " + i + " has negative length: " + range.getLength());
      }
    }
    // Check for overlapping ranges
    for (int i = 0; i < ranges.size(); i++) {
      FileRange current = ranges.get(i);
      long currentEnd = current.getOffset() + current.getLength();
      for (int j = i + 1; j < ranges.size(); j++) {
        FileRange other = ranges.get(j);
        long otherEnd = other.getOffset() + other.getLength();
        // Check if ranges overlap
        boolean overlaps = (current.getOffset() < otherEnd && currentEnd > other.getOffset());
        if (overlaps) {
          throw new IllegalArgumentException(
              "Range[" + i + "] (" + current.getOffset() + ", " + current.getLength() +
                  ") overlaps with Range[" + j + "] (" + other.getOffset() + ", " + other.getLength() + ")");
        }
      }
    }
  }

  /**
   * Performs vectored read by reading each range asynchronously.
   * This method handles the common logic of setting up CompletableFutures
   * and submitting async read tasks for each range.
   *
   * @param ranges list of file ranges to read
   * @param allocate function to allocate ByteBuffer for each range
   * @param reader the function that performs the actual read operation
   * @throws IOException if there is an error during validation
   */
  public static void performVectoredRead(
      List<? extends FileRange> ranges,
      IntFunction<ByteBuffer> allocate,
      RangeReader reader) throws IOException {

    // Validate ranges before processing
    validateRanges(ranges);
    // Perform vectored read using positioned read operations
    for (FileRange range : ranges) {
      CompletableFuture<ByteBuffer> result = range.getData();
      if (result == null) {
        result = new CompletableFuture<>();
        range.setData(result);
      }
      final CompletableFuture<ByteBuffer> finalResult = result;
      final long offset = range.getOffset();
      final int length = range.getLength();
      // Submit async read task for this range
      CompletableFuture.runAsync(() -> {
        try {
          ByteBuffer buffer = allocate.apply(length);
          reader.readRange(offset, buffer);
          buffer.flip();
          finalResult.complete(buffer);
        } catch (Exception e) {
          finalResult.completeExceptionally(e);
        }
      });
    }
  }
}
