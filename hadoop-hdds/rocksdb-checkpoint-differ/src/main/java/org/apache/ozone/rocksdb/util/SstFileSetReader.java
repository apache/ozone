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

package org.apache.ozone.rocksdb.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRawSSTFileIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRawSSTFileReader;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSlice;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileReader;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileReaderIterator;
import org.apache.hadoop.ozone.util.ClosableIterator;
import org.rocksdb.RocksDBException;

/**
 * Provides an abstraction layer using which we can iterate over multiple
 * underlying SST files transparently.
 */
public class SstFileSetReader {

  private final Collection<String> sstFiles;

  private volatile long estimatedTotalKeys = -1;

  public SstFileSetReader(final Collection<String> sstFiles) {
    this.sstFiles = sstFiles;
  }

  public static <T> Stream<T> getStreamFromIterator(ClosableIterator<T> itr) {
    final Spliterator<T> spliterator =
        Spliterators.spliteratorUnknownSize(itr, 0);
    return StreamSupport.stream(spliterator, false).onClose(itr::close);
  }

  public long getEstimatedTotalKeys() throws RocksDBException {
    if (estimatedTotalKeys != -1) {
      return estimatedTotalKeys;
    }

    long estimatedSize = 0;
    synchronized (this) {
      if (estimatedTotalKeys != -1) {
        return estimatedTotalKeys;
      }

      try (ManagedOptions options = new ManagedOptions()) {
        for (String sstFile : sstFiles) {
          try (ManagedSstFileReader fileReader = new ManagedSstFileReader(options)) {
            fileReader.open(sstFile);
            estimatedSize += fileReader.getTableProperties().getNumEntries();
          }
        }
      }
      estimatedTotalKeys = estimatedSize;
    }

    return estimatedTotalKeys;
  }

  public Stream<String> getKeyStream(String lowerBound,
                                     String upperBound) throws RocksDBException {
    // TODO: [SNAPSHOT] Check if default Options and ReadOptions is enough.
    final MultipleSstFileIterator<String> itr = new MultipleSstFileIterator<String>(sstFiles) {
      private ManagedOptions options;
      private ManagedReadOptions readOptions;

      private ManagedSlice lowerBoundSLice;

      private ManagedSlice upperBoundSlice;

      @Override
      protected void init() {
        this.options = new ManagedOptions();
        this.readOptions = new ManagedReadOptions();
        if (Objects.nonNull(lowerBound)) {
          this.lowerBoundSLice = new ManagedSlice(
              StringUtils.string2Bytes(lowerBound));
          readOptions.setIterateLowerBound(lowerBoundSLice);
        }

        if (Objects.nonNull(upperBound)) {
          this.upperBoundSlice = new ManagedSlice(
              StringUtils.string2Bytes(upperBound));
          readOptions.setIterateUpperBound(upperBoundSlice);
        }
      }

      @Override
      protected ClosableIterator<String> getKeyIteratorForFile(String file) throws RocksDBException {
        return new ManagedSstFileIterator(file, options, readOptions) {
          @Override
          protected String getIteratorValue(ManagedSstFileReaderIterator iterator) {
            return new String(iterator.get().key(), UTF_8);
          }
        };
      }

      @Override
      public void close() throws UncheckedIOException {
        super.close();
        options.close();
        readOptions.close();
        IOUtils.closeQuietly(lowerBoundSLice, upperBoundSlice);
      }
    };
    return getStreamFromIterator(itr);
  }

  public Stream<String> getKeyStreamWithTombstone(String lowerBound, String upperBound) throws RocksDBException {
    final MultipleSstFileIterator<String> itr = new MultipleSstFileIterator<String>(sstFiles) {
      //TODO: [SNAPSHOT] Check if default Options is enough.
      private ManagedOptions options;
      private ManagedSlice lowerBoundSlice;
      private ManagedSlice upperBoundSlice;

      @Override
      protected void init() {
        this.options = new ManagedOptions();
        if (Objects.nonNull(lowerBound)) {
          this.lowerBoundSlice = new ManagedSlice(
              StringUtils.string2Bytes(lowerBound));
        }
        if (Objects.nonNull(upperBound)) {
          this.upperBoundSlice = new ManagedSlice(
              StringUtils.string2Bytes(upperBound));
        }
      }

      @Override
      protected ClosableIterator<String> getKeyIteratorForFile(String file) {
        return new ManagedRawSstFileIterator(file, options, lowerBoundSlice, upperBoundSlice,
            keyValue -> StringUtils.bytes2String(keyValue.getKey()));
      }

      @Override
      public void close() throws UncheckedIOException {
        super.close();
        options.close();
        IOUtils.closeQuietly(lowerBoundSlice, upperBoundSlice);
      }
    };
    return getStreamFromIterator(itr);
  }

  private abstract static class ManagedSstFileIterator implements ClosableIterator<String> {
    private final ManagedSstFileReader fileReader;
    private final ManagedSstFileReaderIterator fileReaderIterator;

    ManagedSstFileIterator(String path, ManagedOptions options, ManagedReadOptions readOptions)
        throws RocksDBException {
      this.fileReader = new ManagedSstFileReader(options);
      this.fileReader.open(path);
      this.fileReaderIterator = ManagedSstFileReaderIterator.managed(fileReader.newIterator(readOptions));
      fileReaderIterator.get().seekToFirst();
    }

    @Override
    public void close() {
      this.fileReaderIterator.close();
      this.fileReader.close();
    }

    @Override
    public boolean hasNext() {
      return fileReaderIterator.get().isValid();
    }

    protected abstract String getIteratorValue(ManagedSstFileReaderIterator iterator);

    @Override
    public String next() {
      String value = getIteratorValue(fileReaderIterator);
      fileReaderIterator.get().next();
      return value;
    }
  }

  private static class ManagedRawSstFileIterator implements ClosableIterator<String> {
    private final ManagedRawSSTFileReader<String> fileReader;
    private final ManagedRawSSTFileIterator<String> fileReaderIterator;
    private static final int READ_AHEAD_SIZE = 2 * 1024 * 1024;

    ManagedRawSstFileIterator(String path, ManagedOptions options, ManagedSlice lowerBound, ManagedSlice upperBound,
                              Function<ManagedRawSSTFileIterator.KeyValue, String> keyValueFunction) {
      this.fileReader = new ManagedRawSSTFileReader<>(options, path, READ_AHEAD_SIZE);
      this.fileReaderIterator = fileReader.newIterator(keyValueFunction, lowerBound, upperBound);
    }

    @Override
    public void close() {
      this.fileReaderIterator.close();
      this.fileReader.close();
    }

    @Override
    public boolean hasNext() {
      return fileReaderIterator.hasNext();
    }

    @Override
    public String next() {
      return fileReaderIterator.next();
    }
  }

  /**
   * A wrapper class that holds an iterator and its current value for heap operations.
   */
  private static class HeapEntryWithFileIdx<T extends Comparable<T>>
      implements Comparable<HeapEntryWithFileIdx<T>> {
    private final ClosableIterator<T> iterator;
    private T current;
    // To ensure stable ordering for identical keys
    private final int fileIndex;

    HeapEntryWithFileIdx(ClosableIterator<T> iterator, int fileIndex) {
      this.iterator = iterator;
      this.fileIndex = fileIndex;
      advance();
    }

    void close() {
      iterator.close();
    }

    boolean advance() {
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      } else {
        current = null;
        return false;
      }
    }

    T getCurrent() {
      return current;
    }

    @Override
    public int compareTo(@Nonnull HeapEntryWithFileIdx<T> other) {
      if (this.current == null && other.current == null) {
        return 0;
      }
      if (this.current == null) {
        return 1;
      }
      if (other.current == null) {
        return -1;
      }

      int result = this.current.compareTo(other.current);
      if (result == 0) {
        return Integer.compare(this.fileIndex, other.fileIndex);
      }
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }

      HeapEntryWithFileIdx<T> other = (HeapEntryWithFileIdx<T>) obj;

      if (this.current == null && other.current == null) {
        return this.fileIndex == other.fileIndex;
      }
      if (this.current == null || other.current == null) {
        return false;
      }

      return this.current.equals(other.current) && this.fileIndex == other.fileIndex;
    }

    @Override
    public int hashCode() {
      return Objects.hash(iterator, current, fileIndex);
    }
  }

  private abstract static class MultipleSstFileIterator<T extends Comparable<T>> implements ClosableIterator<T> {
    private final Collection<String> files;
    private final PriorityQueue<HeapEntryWithFileIdx<T>> minHeap;
    private final List<HeapEntryWithFileIdx<T>> allIterators;
    private T lastReturnedValue;

    private MultipleSstFileIterator(Collection<String> files) {
      this.files = files;
      this.minHeap = new PriorityQueue<>();
      this.allIterators = new ArrayList<>();
      this.lastReturnedValue = null;
      init();
      initMinHeap();
    }

    protected abstract void init();

    protected abstract ClosableIterator<T> getKeyIteratorForFile(String file) throws RocksDBException, IOException;

    private void initMinHeap() {
      try {
        int fileIndex = 0;
        for (String file : files) {
          ClosableIterator<T> iterator = getKeyIteratorForFile(file);
          HeapEntryWithFileIdx<T> entry = new HeapEntryWithFileIdx<>(iterator, fileIndex++);
          allIterators.add(entry);

          if (entry.getCurrent() != null) {
            minHeap.offer(entry);
          }
        }
      } catch (IOException | RocksDBException e) {
        // Clean up any opened iterators
        close();
        throw new RuntimeException("Failed to initialize SST file iterators", e);
      }
    }

    @Override
    public boolean hasNext() {
      // Skip duplicates, keep advancing until we find a different key or run out of entries
      while (!minHeap.isEmpty()) {
        HeapEntryWithFileIdx<T> topEntry = minHeap.peek();
        if (topEntry == null) {
          break;
        }
        T currentValue = topEntry.getCurrent();

        // If this is a new value (different from last returned), we have a next element
        if (lastReturnedValue == null || !Objects.equals(currentValue, lastReturnedValue)) {
          return true;
        }

        // Skip this duplicate entry
        HeapEntryWithFileIdx<T> entry = minHeap.poll();
        if (entry != null && entry.advance()) {
          minHeap.offer(entry);
        }
      }

      return false;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more elements found.");
      }

      assert minHeap.peek() != null;
      // Get current key from heap
      T currentKey = minHeap.peek().getCurrent();

      // Advance all entries with the same key (from different files)
      // and keep the one with the highest file index
      while (!minHeap.isEmpty() && Objects.equals(minHeap.peek().getCurrent(), currentKey)) {
        HeapEntryWithFileIdx<T> entry = minHeap.poll();
        if (entry.advance()) {
          minHeap.offer(entry);
        }
      }

      lastReturnedValue = currentKey;
      return currentKey;
    }

    @Override
    public void close() {
      minHeap.clear();
      for (HeapEntryWithFileIdx<T> entry : allIterators) {
        entry.close();
      }
      allIterators.clear();
    }
  }

}
