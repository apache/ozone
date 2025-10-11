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
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Comparator;
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
  private static class HeapEntry<T extends Comparable<T>>
      implements Comparable<HeapEntry<T>>, Closeable {
    private final ClosableIterator<T> iterator;
    private T currentKey;

    HeapEntry(ClosableIterator<T> iterator) {
      this.iterator = iterator;
      advance();
    }

    @Override
    public void close() {
      iterator.close();
    }

    boolean advance() {
      if (iterator.hasNext()) {
        currentKey = iterator.next();
        return true;
      } else {
        currentKey = null;
        return false;
      }
    }

    T getCurrentKey() {
      return currentKey;
    }

    @Override
    public int compareTo(@Nonnull HeapEntry<T> other) {
      return Comparator.comparing(HeapEntry<T>::getCurrentKey).compare(this, other);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }

      HeapEntry<T> other = (HeapEntry<T>) obj;
      return this.compareTo(other) == 0;
    }

    @Override
    public int hashCode() {
      return currentKey.hashCode();
    }
  }

  /**
   * The MultipleSstFileIterator class is an abstract base for iterating over multiple SST files.
   * It uses a PriorityQueue to merge keys from all files in sorted order.
   * Each file's iterator is wrapped in a HeapEntryWithFileIdx object,
   * which ensures stable ordering for identical keys by considering the file index.
   * @param <T>
   */
  private abstract static class MultipleSstFileIterator<T extends Comparable<T>> implements ClosableIterator<T> {
    private final PriorityQueue<HeapEntry<T>> minHeap;

    private MultipleSstFileIterator(Collection<String> sstFiles) {
      this.minHeap = new PriorityQueue<>();
      init();
      initMinHeap(sstFiles);
    }

    protected abstract void init();

    protected abstract ClosableIterator<T> getKeyIteratorForFile(String file) throws RocksDBException, IOException;

    private void initMinHeap(Collection<String> files) {
      try {
        for (String file : files) {
          ClosableIterator<T> iterator = getKeyIteratorForFile(file);
          HeapEntry<T> entry = new HeapEntry<>(iterator);

          if (entry.getCurrentKey() != null) {
            minHeap.offer(entry);
          } else {
            // No valid entries, close the iterator
            entry.close();
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
      return !minHeap.isEmpty();
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more elements found.");
      }

      assert minHeap.peek() != null;
      // Get current key from heap
      T currentKey = minHeap.peek().getCurrentKey();

      // Advance all entries with the same key (from different files)
      while (!minHeap.isEmpty() && Objects.equals(minHeap.peek().getCurrentKey(), currentKey)) {
        HeapEntry<T> entry = minHeap.poll();
        if (entry.advance()) {
          minHeap.offer(entry);
        } else {
          // Iterator is exhausted, close it to prevent resource leak
          entry.close();
        }
      }

      return currentKey;
    }

    @Override
    public void close() {
      while (!minHeap.isEmpty()) {
        minHeap.poll().close();
      }
    }
  }

}
