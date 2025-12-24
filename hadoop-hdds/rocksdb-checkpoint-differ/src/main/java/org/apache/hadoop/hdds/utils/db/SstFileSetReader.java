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

import static org.apache.hadoop.hdds.utils.db.IteratorType.KEY_ONLY;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.ManagedRawSSTFileIterator.KeyValue;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSlice;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileReader;
import org.apache.hadoop.ozone.util.ClosableIterator;
import org.rocksdb.RocksDBException;

/**
 * Provides an abstraction layer using which we can iterate over multiple
 * underlying SST files transparently.
 */
public class SstFileSetReader {

  private final Collection<Path> sstFiles;

  private volatile long estimatedTotalKeys = -1;

  public SstFileSetReader(final Collection<Path> sstFiles) {
    this.sstFiles = sstFiles;
  }

  public long getEstimatedTotalKeys() throws RocksDatabaseException {
    if (estimatedTotalKeys != -1) {
      return estimatedTotalKeys;
    }

    long estimatedSize = 0;
    synchronized (this) {
      if (estimatedTotalKeys != -1) {
        return estimatedTotalKeys;
      }

      try (ManagedOptions options = new ManagedOptions()) {
        for (Path sstFile : sstFiles) {
          try (ManagedSstFileReader fileReader = new ManagedSstFileReader(options)) {
            fileReader.open(sstFile.toAbsolutePath().toString());
            estimatedSize += fileReader.getTableProperties().getNumEntries();
          } catch (RocksDBException e) {
            throw new RocksDatabaseException("Failed to open SST file: " + sstFile, e);
          }
        }
      }
      estimatedTotalKeys = estimatedSize;
    }

    return estimatedTotalKeys;
  }

  public ClosableIterator<String> getKeyStream(String lowerBound, String upperBound) throws CodecException {
    // TODO: [SNAPSHOT] Check if default Options and ReadOptions is enough.
    final MultipleSstFileIterator<String> itr = new MultipleSstFileIterator<String>(sstFiles) {
      private ManagedOptions options;
      private ManagedReadOptions readOptions;

      private ManagedSlice lowerBoundSLice;

      private ManagedSlice upperBoundSlice;

      @Override
      protected void init() throws CodecException {
        this.options = new ManagedOptions();
        this.readOptions = new ManagedReadOptions();
        if (Objects.nonNull(lowerBound)) {
          this.lowerBoundSLice = new ManagedSlice(
              StringCodec.get().toPersistedFormat(lowerBound));
          readOptions.setIterateLowerBound(lowerBoundSLice);
        }

        if (Objects.nonNull(upperBound)) {
          this.upperBoundSlice = new ManagedSlice(
              StringCodec.get().toPersistedFormat(upperBound));
          readOptions.setIterateUpperBound(upperBoundSlice);
        }
      }

      @Override
      protected ClosableIterator<String> getKeyIteratorForFile(String file) throws RocksDatabaseException {
        return new ManagedSstFileIterator<String>(file, options, readOptions, KEY_ONLY) {

          @Override
          String getIteratorValue(CodecBuffer key, CodecBuffer value) {
            return StringCodec.get().fromCodecBuffer(key);
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
    return itr;
  }

  public ClosableIterator<String> getKeyStreamWithTombstone(String lowerBound, String upperBound)
      throws CodecException {
    final MultipleSstFileIterator<String> itr = new MultipleSstFileIterator<String>(sstFiles) {
      //TODO: [SNAPSHOT] Check if default Options is enough.
      private ManagedOptions options;
      private ManagedSlice lowerBoundSlice;
      private ManagedSlice upperBoundSlice;

      @Override
      protected void init() throws CodecException {
        this.options = new ManagedOptions();
        if (Objects.nonNull(lowerBound)) {
          this.lowerBoundSlice = new ManagedSlice(
              StringCodec.get().toPersistedFormat(lowerBound));
        }
        if (Objects.nonNull(upperBound)) {
          this.upperBoundSlice = new ManagedSlice(
              StringCodec.get().toPersistedFormat(upperBound));
        }
      }

      @Override
      protected ClosableIterator<String> getKeyIteratorForFile(String file) {
        return new ManagedRawSstFileIterator(file, options, lowerBoundSlice, upperBoundSlice,
            keyValue -> StringCodec.get().fromCodecBuffer(keyValue.getKey()), KEY_ONLY);
      }

      @Override
      public void close() throws UncheckedIOException {
        super.close();
        options.close();
        IOUtils.closeQuietly(lowerBoundSlice, upperBoundSlice);
      }
    };
    return itr;
  }

  private static class ManagedRawSstFileIterator implements ClosableIterator<String> {
    private final ManagedRawSSTFileReader fileReader;
    private final ManagedRawSSTFileIterator<String> fileReaderIterator;
    private static final int READ_AHEAD_SIZE = 2 * 1024 * 1024;

    ManagedRawSstFileIterator(String path, ManagedOptions options, ManagedSlice lowerBound, ManagedSlice upperBound,
                              Function<KeyValue, String> keyValueFunction, IteratorType type) {
      this.fileReader = new ManagedRawSSTFileReader(options, path, READ_AHEAD_SIZE);
      this.fileReaderIterator = fileReader.newIterator(keyValueFunction, lowerBound, upperBound, type);
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
   * The MultipleSstFileIterator class is an abstract base for iterating over multiple SST files.
   * It uses a PriorityQueue to merge keys from all files in sorted order.
   * Each file's iterator is wrapped in a HeapEntryWithFileIdx object,
   * which ensures stable ordering for identical keys by considering the file index.
   * @param <T>
   */
  private abstract static class MultipleSstFileIterator<T extends Comparable<T>>
      extends MinHeapMergeIterator<T, ClosableIterator<T>, T> {
    private final List<Path> sstFiles;

    private MultipleSstFileIterator(Collection<Path> sstFiles) throws CodecException {
      super(sstFiles.size(), Comparable::compareTo);
      init();
      this.sstFiles = sstFiles.stream().map(Path::toAbsolutePath).collect(Collectors.toList());
    }

    protected abstract void init() throws CodecException;

    protected abstract ClosableIterator<T> getKeyIteratorForFile(String file) throws IOException;

    @Override
    protected ClosableIterator<T> getIterator(int idx) throws IOException {
      return getKeyIteratorForFile(sstFiles.get(idx).toString());
    }

    @Override
    protected T merge(Map<Integer, T> keys) {
      return keys.values().stream().findAny()
          .orElseThrow(() -> new NoSuchElementException("All values are null from all iterators."));
    }
  }

}
