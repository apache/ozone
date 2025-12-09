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

import static java.util.Objects.requireNonNull;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;

/**
 * Utility class for vectored read operations.
 * Based on Hadoop's org.apache.hadoop.fs.VectoredReadUtils.
 */
@InterfaceAudience.Private
public final class VectoredReadUtils {
  private VectoredReadUtils() {
    // Utility class, no instances
  }

  /**
   * Functional interface for reading a range of data.
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
   * Validate a single range.
   * @param range range to validate.
   * @return the range.
   * @param <T> range type
   * @throws IllegalArgumentException the range length is negative
   * @throws EOFException the range offset is negative
   * @throws NullPointerException if the range is null.
   */
  public static <T extends FileRange> T validateRangeRequest(T range)
      throws EOFException {
    requireNonNull(range, "range is null");

    if (range.getLength() < 0) {
      throw new IllegalArgumentException("length is negative in " + range);
    }
    if (range.getOffset() < 0) {
      throw new EOFException("position is negative in range " + range);
    }
    return range;
  }

  /**
   * Sort the input ranges by offset; no validation is done.
   * @param input input ranges.
   * @return a new list of the ranges, sorted by offset.
   */
  public static List<? extends FileRange> sortRangeList(List<? extends FileRange> input) {
    final List<? extends FileRange> l = new ArrayList<>(input);
    l.sort(Comparator.comparingLong(FileRange::getOffset));
    return l;
  }

  /**
   * Validate a list of ranges (including overlapping checks).
   * Based on Hadoop's validateAndSortRanges.
   * Two ranges overlap when the start offset of second is less than
   * the end offset of first. End offset is calculated as start offset + length.
   *
   * @param ranges list of file ranges to validate
   * @throws NullPointerException if ranges list is null or contains null elements
   * @throws EOFException if any range has a negative offset
   * @throws IllegalArgumentException if there are overlapping ranges or a range element is invalid
   */
  public static void validateRanges(List<? extends FileRange> ranges) throws IOException {
    requireNonNull(ranges, "Null input list");

    if (ranges.isEmpty()) {
      return;
    }

    if (ranges.size() == 1) {
      validateRangeRequest(ranges.get(0));
      return;
    }

    // Sort ranges to check for overlaps efficiently
    List<? extends FileRange> sortedRanges = sortRangeList(ranges);
    FileRange prev = null;
    for (final FileRange current : sortedRanges) {
      validateRangeRequest(current);
      if (prev != null) {
        // Check for overlap: current start < prev end
        if (current.getOffset() < prev.getOffset() + prev.getLength()) {
          throw new IllegalArgumentException(
              "Overlapping ranges " + prev + " and " + current);
        }
      }
      prev = current;
    }
  }

  /**
   * Common implementation for vectored read operations.
   * Validates ranges and submits async read tasks for each range.
   *
   * @param ranges list of file ranges to read
   * @param allocate function to allocate ByteBuffer for each range
   * @param reader function that performs the actual read operation
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
