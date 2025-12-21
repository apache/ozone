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

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;

/**
 * Test for RDBSstFileWriter.
 */
public class TestRDBSstFileWriter {

  @TempDir
  private Path path;

  @EnabledIfSystemProperty(named = ROCKS_TOOLS_NATIVE_PROPERTY, matches = "true")
  @Test
  public void testSstFileTombstoneCreationWithCodecBufferReuse() throws IOException {
    ManagedRawSSTFileReader.tryLoadLibrary();
    Path sstPath = path.resolve("test.sst").toAbsolutePath();
    try (CodecBuffer codecBuffer = CodecBuffer.allocateDirect(1024);
         RDBSstFileWriter sstFileWriter = new RDBSstFileWriter(sstPath.toFile());
         CodecBuffer emptyBuffer = CodecBuffer.getEmptyBuffer()) {
      Queue<String> keys = new LinkedList<>(ImmutableList.of("key1_renamed", "key1", "key1_renamed"));
      PutToByteBuffer<IOException> putFunc = byteBuffer -> {
        byte[] keyBytes = StringUtils.string2Bytes(keys.peek());
        byteBuffer.put(keyBytes);
        return keyBytes.length;
      };
      int len = codecBuffer.putFromSource(putFunc);
      assertEquals(codecBuffer.readableBytes(), len);
      assertEquals(keys.poll(), StringUtils.bytes2String(codecBuffer.getArray()));
      codecBuffer.clear();
      int idx = 0;
      while (!keys.isEmpty()) {
        codecBuffer.putFromSource(putFunc);
        byte[] keyBytes = new byte[codecBuffer.readableBytes()];
        assertEquals(keyBytes.length, codecBuffer.getInputStream().read(keyBytes));
        if (idx++ % 2 == 0) {
          sstFileWriter.delete(codecBuffer);
        } else {
          sstFileWriter.put(codecBuffer, emptyBuffer);
        }
        assertEquals(keys.poll(), StringUtils.bytes2String(codecBuffer.getArray()));
        codecBuffer.clear();
      }
    }
    Assertions.assertTrue(sstPath.toFile().exists());
    try (ManagedOptions options = new ManagedOptions();
         ManagedRawSSTFileReader reader = new ManagedRawSSTFileReader(options, sstPath.toString(), 1024);
         ManagedRawSSTFileIterator<ManagedRawSSTFileIterator.KeyValue> itr =
             reader.newIterator(kv -> kv, null, null, IteratorType.KEY_AND_VALUE)) {

      int idx = 0;
      List<String> keys = ImmutableList.of("key1", "key1_rename");
      while (itr.hasNext()) {
        ManagedRawSSTFileIterator.KeyValue kv = itr.next();
        assertEquals(idx, kv.getType());
        assertEquals(keys.get(idx), keys.get(idx++));
        assertEquals(0, kv.getValue().readableBytes());
      }
      assertEquals(2, idx);
    }
  }
}
