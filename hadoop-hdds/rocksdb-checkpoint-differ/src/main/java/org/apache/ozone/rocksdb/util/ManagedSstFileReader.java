/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ozone.rocksdb.util;

import org.apache.hadoop.util.ClosableIterator;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSSTDumpIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSSTDumpTool;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * ManagedSstFileReader provides an abstraction layer using which we can
 * iterate over multiple underlying SST files transparently.
 */
public class ManagedSstFileReader {

  private final Collection<String> sstFiles;

  private volatile long estimatedTotalKeys = -1;

  public ManagedSstFileReader(final Collection<String> sstFiles) {
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
          SstFileReader fileReader = new SstFileReader(options);
          fileReader.open(sstFile);
          estimatedSize += fileReader.getTableProperties().getNumEntries();
        }
      }
      estimatedTotalKeys = estimatedSize;
    }

    return estimatedTotalKeys;
  }

  public Stream<String> getKeyStream() throws RocksDBException,
      NativeLibraryNotLoadedException, IOException {
    // TODO: [SNAPSHOT] Check if default Options and ReadOptions is enough.
    final MultipleSstFileIterator<String> itr =
        new MultipleSstFileIterator<String>(sstFiles) {
          private ManagedOptions options;
          private ReadOptions readOptions;

          @Override
          protected void init() {
            this.options = new ManagedOptions();
            this.readOptions = new ManagedReadOptions();
          }

          @Override
          protected ClosableIterator<String> getKeyIteratorForFile(String file)
              throws RocksDBException {
            return new ManagedSstFileIterator(file, options, readOptions) {
              @Override
              protected String getIteratorValue(
                  SstFileReaderIterator iterator) {
                return new String(iterator.key(), UTF_8);
              }
            };
          }

          @Override
          public void close() throws UncheckedIOException {
            super.close();
            options.close();
            readOptions.close();
          }
        };
    return getStreamFromIterator(itr);
  }

  public Stream<String> getKeyStreamWithTombstone(
      ManagedSSTDumpTool sstDumpTool) throws IOException, RocksDBException,
      NativeLibraryNotLoadedException {
    final MultipleSstFileIterator<String> itr =
        new MultipleSstFileIterator<String>(sstFiles) {
          //TODO: [SNAPSHOT] Check if default Options is enough.
          private ManagedOptions options;

          @Override
          protected void init() {
            this.options = new ManagedOptions();
          }

          @Override
          protected ClosableIterator<String> getKeyIteratorForFile(String file)
              throws NativeLibraryNotLoadedException, IOException {
            return new ManagedSSTDumpIterator<String>(sstDumpTool, file,
                options) {
              @Override
              protected String getTransformedValue(KeyValue value) {
                return value.getKey();
              }
            };
          }

          @Override
          public void close() throws UncheckedIOException {
            super.close();
            options.close();
          }
        };
    return getStreamFromIterator(itr);
  }

  private abstract static class ManagedSstFileIterator implements
      ClosableIterator<String> {
    private SstFileReader fileReader;
    private SstFileReaderIterator fileReaderIterator;

    ManagedSstFileIterator(String path, ManagedOptions options,
                           ReadOptions readOptions)
        throws RocksDBException {
      this.fileReader = new SstFileReader(options);
      this.fileReader.open(path);
      this.fileReaderIterator = fileReader.newIterator(readOptions);
      fileReaderIterator.seekToFirst();
    }

    @Override
    public void close() {
      this.fileReaderIterator.close();
      this.fileReader.close();
    }

    @Override
    public boolean hasNext() {
      return fileReaderIterator.isValid();
    }

    protected abstract String getIteratorValue(SstFileReaderIterator iterator);

    @Override
    public String next() {
      String value = getIteratorValue(fileReaderIterator);
      fileReaderIterator.next();
      return value;
    }
  }

  private abstract static class MultipleSstFileIterator<T> implements
      ClosableIterator<T> {

    private final Iterator<String> fileNameIterator;

    private String currentFile;
    private ClosableIterator<T> currentFileIterator;

    private MultipleSstFileIterator(Collection<String> files)
        throws IOException, RocksDBException,
        NativeLibraryNotLoadedException {
      this.fileNameIterator = files.iterator();
      init();
      moveToNextFile();
    }

    protected abstract void init();

    protected abstract ClosableIterator<T> getKeyIteratorForFile(String file)
        throws RocksDBException, NativeLibraryNotLoadedException,
        IOException;

    @Override
    public boolean hasNext() {
      try {
        do {
          if (currentFileIterator.hasNext()) {
            return true;
          }
        } while (moveToNextFile());
      } catch (IOException | RocksDBException |
               NativeLibraryNotLoadedException e) {
        // TODO: [Snapshot] This exception has to be handled by the caller.
        //  We have to do better exception handling.
        throw new RuntimeException(e);
      }
      return false;
    }

    @Override
    public T next() {
      if (hasNext()) {
        return currentFileIterator.next();
      }
      throw new NoSuchElementException("No more elements found.");
    }

    @Override
    public void close() throws UncheckedIOException {
      try {
        closeCurrentFile();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private boolean moveToNextFile() throws IOException, RocksDBException,
        NativeLibraryNotLoadedException {
      if (fileNameIterator.hasNext()) {
        closeCurrentFile();
        currentFile = fileNameIterator.next();
        this.currentFileIterator = getKeyIteratorForFile(currentFile);
        return true;
      }
      return false;
    }

    private void closeCurrentFile() throws IOException {
      if (currentFile != null) {
        currentFileIterator.close();
        currentFile = null;
      }
    }
  }

}
