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

import com.google.common.primitives.UnsignedLong;
import java.util.Optional;
import java.util.function.Function;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedParsedEntry;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSlice;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileReaderIterator;
import org.rocksdb.EntryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ManagedRawSstFileIterator provides an implementation of {@link AbstractSstFileIterator}
 * to iterate over and transform all entries (including all non user entries like tombstone) in SST (Sorted String
 * Table) files using RocksDB. It utilizes a custom entry parser, key-value transformation logic, and resource
 * management features for efficient and safe access to the SST data.
 * This implementation makes use of TableIterator implemented by RocksDB to read the entries from the SST files.
 *
 * @param <T> The type of the transformation result for each SST record.
 */
public class ManagedRawSstFileIterator<T> extends AbstractSstFileIterator<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ManagedRawSstFileIterator.class);
  private final ManagedParsedEntry entryParser;
  private final Buffer userKeyBuffer;
  private final Function<KeyValue, T> transformer;
  private boolean closed;
  private final KeyValue currentKeyValue;

  public ManagedRawSstFileIterator(String path, ManagedOptions options,
      Optional<ManagedSlice> internalKeyLowerBound,
      Optional<ManagedSlice> internalKeyUpperBound, IteratorType type,
      Function<KeyValue, T> transformer) throws RocksDatabaseException {
    super(path, options, type, sstFileReader ->
        ManagedSstFileReaderIterator.managed(sstFileReader.newTableIterator(
            internalKeyLowerBound.orElse(null), internalKeyUpperBound.orElse(null))));
    LOG.info("Created ManagedRawSstFileIterator for path: {} with lower bound : {} and upper bound : {}",
        path, internalKeyLowerBound, internalKeyUpperBound);
    this.entryParser = new ManagedParsedEntry();
    this.userKeyBuffer = new Buffer(
        new CodecBuffer.Capacity(path + " iterator-key", 1 << 10),
        type.readKey() ? entryParser::userKey : null);
    this.currentKeyValue = new KeyValue();
    this.transformer = transformer;
  }

  @Override
  T getIteratorValue(CodecBuffer key, CodecBuffer value) {
    if (key != null) {
      entryParser.parseEntry(getOptions(), key.asReadOnlyByteBuffer());
      CodecBuffer userKey = userKeyBuffer.getFromDb();
      EntryType type = entryParser.getEntryType();
      UnsignedLong sequenceNumber = UnsignedLong.fromLongBits(entryParser.getSequenceNumber());
      return this.transformer.apply(currentKeyValue.setKeyValue(userKey, sequenceNumber, type, value));
    }
    return this.transformer.apply(currentKeyValue.setKeyValue(null, UnsignedLong.fromLongBits(0), null,
        value));
  }

  @Override
  public synchronized void close() {
    super.close();
    if (!closed) {
      userKeyBuffer.release();
      entryParser.close();
    }
    closed = true;
  }

  /**
   * Class containing Parsed KeyValue Record from RawSstReader output.
   */
  public static final class KeyValue {

    private CodecBuffer key;
    private UnsignedLong sequence;
    private EntryType type;
    private CodecBuffer value;

    private KeyValue() {
    }

    private KeyValue setKeyValue(CodecBuffer keyBuffer, UnsignedLong sequenceVal, EntryType typeVal,
        CodecBuffer valueBuffer) {
      this.key = keyBuffer;
      this.sequence = sequenceVal;
      this.type = typeVal;
      this.value = valueBuffer;
      return this;
    }

    public CodecBuffer getKey() {
      return this.key;
    }

    public UnsignedLong getSequence() {
      return sequence;
    }

    public EntryType getType() {
      return type;
    }

    public CodecBuffer getValue() {
      return value;
    }

    @Override
    public String toString() {
      return "KeyValue{" +
          "key=" + (key == null ? null : StringCodec.get().fromCodecBuffer(key)) +
          ", sequence=" + sequence +
          ", type=" + type +
          ", value=" + (value == null ? null : StringCodec.get().fromCodecBuffer(value)) +
          '}';
    }
  }
}

