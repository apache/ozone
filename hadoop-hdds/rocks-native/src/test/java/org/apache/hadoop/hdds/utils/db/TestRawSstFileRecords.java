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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.ExpectedLatestVersionMergeOutput.SourceRecord;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;

/**
 * Test helper that reads all key versions from raw SST files.
 */
final class TestRawSstFileRecords {

  private static final int DEFAULT_READ_AHEAD_SIZE = 2 * 1024 * 1024;

  private TestRawSstFileRecords() {
  }

  static List<SourceRecord> readFile(Path sstFile) throws IOException {
    return readFile(sstFile, DEFAULT_READ_AHEAD_SIZE);
  }

  static List<SourceRecord> readFile(Path sstFile, int readAheadSize) throws IOException {
    List<SourceRecord> records = new ArrayList<>();
    try (ManagedOptions options = new ManagedOptions();
         ManagedRawSSTFileReader reader = new ManagedRawSSTFileReader(
             options, sstFile.toAbsolutePath().toString(), readAheadSize);
         ManagedRawSSTFileIterator<ManagedRawSSTFileIterator.KeyValue> iterator =
             reader.newIterator(kv -> kv, null, null, IteratorType.KEY_AND_VALUE)) {
      while (iterator.hasNext()) {
        ManagedRawSSTFileIterator.KeyValue kv = iterator.next();
        byte[] key = copyBuffer(kv.getKey());
        byte[] value = kv.getType() == LatestVersionedKWayMergeIterator.ROCKS_TYPE_VALUE
            ? copyBuffer(kv.getValue()) : null;
        records.add(new SourceRecord(key, kv.getSequence().longValue(), kv.getType(), value));
      }
    }
    return records;
  }

  static List<List<SourceRecord>> readFiles(List<Path> sstFiles) throws IOException {
    return readFiles(sstFiles, DEFAULT_READ_AHEAD_SIZE);
  }

  static List<List<SourceRecord>> readFiles(List<Path> sstFiles, int readAheadSize)
      throws IOException {
    List<List<SourceRecord>> perSource = new ArrayList<>(sstFiles.size());
    for (Path sstFile : sstFiles) {
      perSource.add(readFile(sstFile, readAheadSize));
    }
    return perSource;
  }

  private static byte[] copyBuffer(CodecBuffer buffer) {
    if (buffer == null) {
      return null;
    }
    ByteBuffer byteBuffer = buffer.asReadOnlyByteBuffer();
    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.get(bytes);
    return bytes;
  }
}
