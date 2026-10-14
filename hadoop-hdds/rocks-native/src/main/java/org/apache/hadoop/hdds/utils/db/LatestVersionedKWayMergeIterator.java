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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.UnsignedLong;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSlice;
import org.apache.hadoop.ozone.util.ClosableIterator;

/**
 * K-way merge over RocksDB SST files for snapshot diff.
 * <p>
 * A single min-heap orders source heads by user key. For each user key, all versions
 * are drained and the latest value and latest tombstone are tracked, then snapshot-diff
 * emit rules apply: emit the latest tombstone and/or latest value, including both when
 * a delete is followed by a newer recreate.
 * <p>
 * When constructed with an exclusive minimum sequence number {@code S}, each SST
 * source skips entries whose sequence is {@code <= S} while advancing.
 */
public final class LatestVersionedKWayMergeIterator implements
    ClosableIterator<LatestVersionedKWayMergeIterator.MergedKeyValue> {

  /** RocksDB {@code ValueType::kTypeValue}. */
  public static final int ROCKS_TYPE_VALUE = 1;
  private static final int DEFAULT_READ_AHEAD_SIZE = 2 * 1024 * 1024;

  private final List<ClosableIterator<? extends MergeHead>> iterators;
  private final Long exclusiveMinSequenceNumber;

  private final PriorityQueue<HeapEntry> sourceHeads;

  private List<MergedKeyValue> emitQueue;
  private boolean initialized;

  /**
   * Opens one iterator per SST file and merges them within an optional user-key range.
   *
   * <p>Caller contract: {@code fromUserKeyInclusive} and {@code toUserKeyExclusive} are wrapped by
   * native {@link org.apache.hadoop.hdds.utils.db.managed.ManagedSlice} objects without copying.
   * Callers must not mutate either array until the returned iterator is closed.
   *
   * @param exclusiveMinSequenceNumber when non-null, each source skips entries with
   *        sequence {@code <=} this value while advancing; when null, no entries are skipped
   * @param fromUserKeyInclusive when non-null and non-empty, each source seeks to this lower bound
   * @param toUserKeyExclusive when non-null and non-empty, each source stops before this upper bound
   */
  public static LatestVersionedKWayMergeIterator overRawSstFiles(Collection<Path> sstFiles,
      int readAheadSizePerFile, Long exclusiveMinSequenceNumber,
      byte[] fromUserKeyInclusive, byte[] toUserKeyExclusive) {
    Objects.requireNonNull(sstFiles, "sstFiles cannot be null");
    List<ClosableIterator<? extends MergeHead>> sources = new ArrayList<>(sstFiles.size());
    try (ManagedOptions options = new ManagedOptions()) {
      for (Path file : sstFiles) {
        sources.add(new RawSstIterator(options, file, readAheadSizePerFile, fromUserKeyInclusive,
            toUserKeyExclusive));
      }
    } catch (RuntimeException e) {
      IOUtils.closeQuietly(sources);
      throw e;
    }
    return new LatestVersionedKWayMergeIterator(sources, exclusiveMinSequenceNumber);
  }

  /**
   * Opens one iterator per SST file, skipping entries at or below {@code exclusiveMinSequenceNumber}.
   *
   * <p>Bound arrays must stay unchanged until the returned iterator is closed; see
   * {@link #overRawSstFiles(Collection, int, Long, byte[], byte[])}.
   */
  public static LatestVersionedKWayMergeIterator overRawSstFilesFromSequence(Collection<Path> sstFiles,
      long exclusiveMinSequenceNumber, byte[] fromUserKeyInclusive, byte[] toUserKeyExclusive) {
    return overRawSstFiles(sstFiles, DEFAULT_READ_AHEAD_SIZE, exclusiveMinSequenceNumber,
        fromUserKeyInclusive, toUserKeyExclusive);
  }

  @VisibleForTesting
  public static LatestVersionedKWayMergeIterator forTest(
      List<ClosableIterator<MergedKeyValue>> iterators, Long exclusiveMinSequenceNumber) {
    List<ClosableIterator<? extends MergeHead>> sources = new ArrayList<>(iterators.size());
    sources.addAll(iterators);
    return new LatestVersionedKWayMergeIterator(sources, exclusiveMinSequenceNumber);
  }

  private LatestVersionedKWayMergeIterator(
      List<ClosableIterator<? extends MergeHead>> iterators,
      Long exclusiveMinSequenceNumber) {
    this.iterators = new ArrayList<>(Objects.requireNonNull(iterators, "iterators cannot be null"));
    this.exclusiveMinSequenceNumber = exclusiveMinSequenceNumber;
    this.sourceHeads = new PriorityQueue<>(Math.max(this.iterators.size(), 1));
    this.emitQueue = new ArrayList<>();
  }

  @Override
  public boolean hasNext() {
    if (!emitQueue.isEmpty()) {
      return true;
    }
    return advance();
  }

  @Override
  public MergedKeyValue next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more elements found.");
    }
    return emitQueue.remove(0);
  }

  private boolean advance() {
    if (!initialized) {
      initHeap();
      initialized = true;
    }

    while (emitQueue.isEmpty() && !sourceHeads.isEmpty()) {
      processNextUserKey();
    }

    return !emitQueue.isEmpty();
  }

  private void processNextUserKey() {
    if (sourceHeads.isEmpty()) {
      return;
    }

    byte[] nextKey = sourceHeads.peek().current.getUserKey();
    MergeHead latestValue = null;
    long latestValueSeq = -1L;
    MergeHead latestTombstone = null;
    long latestTombstoneSeq = -1L;

    while (heapHasUserKey(nextKey)) {
      List<HeapEntry> polled = new ArrayList<>();
      while (heapHasUserKey(nextKey)) {
        HeapEntry entry = sourceHeads.poll();
        MergeHead head = entry.current;
        if (head.isTombstone()) {
          if (head.getSequence() > latestTombstoneSeq) {
            latestTombstone = head;
            latestTombstoneSeq = head.getSequence();
          }
        } else if (head.getSequence() > latestValueSeq) {
          latestValue = head;
          latestValueSeq = head.getSequence();
        }
        polled.add(entry);
      }
      for (HeapEntry entry : polled) {
        if (entry.current == latestValue && entry.current instanceof RawSstHeapHead) {
          ((RawSstHeapHead) entry.current).snapshotValue();
        }
        entry.advance();
        if (entry.current != null) {
          sourceHeads.offer(entry);
        }
      }
    }

    emitForUserKey(latestValue, latestTombstone);
  }

  private void emitForUserKey(MergeHead latestValue, MergeHead latestTombstone) {
    if (latestValue != null && latestTombstone != null) {
      if (latestValue.getSequence() > latestTombstone.getSequence()) {
        emitQueue.add(latestTombstone.toMergedKeyValue());
        emitQueue.add(latestValue.toMergedKeyValue());
      } else {
        emitQueue.add(latestTombstone.toMergedKeyValue());
      }
    } else if (latestValue != null) {
      emitQueue.add(latestValue.toMergedKeyValue());
    } else if (latestTombstone != null) {
      emitQueue.add(latestTombstone.toMergedKeyValue());
    }
  }

  private boolean heapHasUserKey(byte[] userKey) {
    return !sourceHeads.isEmpty()
        && compareUserKeys(sourceHeads.peek().current.getUserKey(), userKey) == 0;
  }

  private void initHeap() {
    for (int idx = 0; idx < iterators.size(); idx++) {
      ClosableIterator<? extends MergeHead> iterator = iterators.get(idx);
      HeapEntry entry = new HeapEntry(idx, iterator);
      if (entry.current != null) {
        sourceHeads.offer(entry);
      }
    }
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(iterators);
  }

  private static int compareUserKeys(byte[] left, byte[] right) {
    if (left == right) {
      return 0;
    }
    if (left == null) {
      return -1;
    }
    if (right == null) {
      return 1;
    }
    int minLength = Math.min(left.length, right.length);
    for (int i = 0; i < minLength; i++) {
      int l = left[i] & 0xff;
      int r = right[i] & 0xff;
      if (l != r) {
        return Integer.compare(l, r);
      }
    }
    return Integer.compare(left.length, right.length);
  }

  private static int compareHeapOrder(MergeHead left, int leftIndex,
      MergeHead right, int rightIndex) {
    int keyCompare = compareUserKeys(left.getUserKey(), right.getUserKey());
    if (keyCompare != 0) {
      return keyCompare;
    }
    int seqCompare = Long.compare(right.getSequence(), left.getSequence());
    if (seqCompare != 0) {
      return seqCompare;
    }
    int tombstoneCompare = Boolean.compare(right.isTombstone(), left.isTombstone());
    if (tombstoneCompare != 0) {
      return tombstoneCompare;
    }
    return Integer.compare(leftIndex, rightIndex);
  }

  private interface MergeHead {
    byte[] getUserKey();

    long getSequence();

    boolean isTombstone();

    MergedKeyValue toMergedKeyValue();
  }

  private static final class RawSstHeapHead implements MergeHead {
    private final byte[] key;
    private final long sequence;
    private final int type;
    private final CodecBuffer valueBuffer;
    private byte[] snapshottedValue;

    RawSstHeapHead(byte[] key, long sequence, int type, CodecBuffer valueBuffer) {
      this.key = Objects.requireNonNull(key, "key cannot be null");
      this.sequence = sequence;
      this.type = type;
      this.valueBuffer = valueBuffer;
    }

    void snapshotValue() {
      if (snapshottedValue != null || isTombstone() || valueBuffer == null) {
        return;
      }
      snapshottedValue = copyBuffer(valueBuffer);
    }

    @Override
    public byte[] getUserKey() {
      return key;
    }

    @Override
    public long getSequence() {
      return sequence;
    }

    @Override
    public boolean isTombstone() {
      return type != ROCKS_TYPE_VALUE;
    }

    @Override
    public MergedKeyValue toMergedKeyValue() {
      byte[] value = snapshottedValue;
      if (value == null && !isTombstone() && valueBuffer != null) {
        value = copyBuffer(valueBuffer);
      }
      return new MergedKeyValue(key, UnsignedLong.fromLongBits(sequence), type, value);
    }
  }

  private final class HeapEntry implements Comparable<HeapEntry> {
    private final int index;
    private final ClosableIterator<? extends MergeHead> iterator;
    private MergeHead current;

    private HeapEntry(int index, ClosableIterator<? extends MergeHead> iterator) {
      this.index = index;
      this.iterator = iterator;
      advance();
    }

    private void advance() {
      while (true) {
        if (!iterator.hasNext()) {
          current = null;
          iterator.close();
          return;
        }
        MergeHead next = iterator.next();
        if (exclusiveMinSequenceNumber == null
            || next.getSequence() > exclusiveMinSequenceNumber) {
          current = next;
          return;
        }
      }
    }

    @Override
    public int compareTo(HeapEntry other) {
      return compareHeapOrder(
          this.current, this.index, other.current, other.index);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof HeapEntry)) {
        return false;
      }
      HeapEntry other = (HeapEntry) o;
      return index == other.index;
    }

    @Override
    public int hashCode() {
      return index;
    }
  }

  private static final class RawSstIterator implements ClosableIterator<RawSstHeapHead> {
    private final ManagedRawSSTFileReader reader;
    private final ManagedRawSSTFileIterator<ManagedRawSSTFileIterator.KeyValue> iterator;
    private final ManagedSlice fromSlice;
    private final ManagedSlice toSlice;
    private boolean closed;

    private RawSstIterator(ManagedOptions options, Path file, int readAheadSize,
        byte[] fromUserKeyInclusive, byte[] toUserKeyExclusive) {
      ManagedRawSSTFileReader openedReader = new ManagedRawSSTFileReader(
          options, file.toAbsolutePath().toString(), readAheadSize);
      ManagedSlice openedFromSlice = toManagedSlice(fromUserKeyInclusive);
      ManagedSlice openedToSlice = toManagedSlice(toUserKeyExclusive);
      ManagedRawSSTFileIterator<ManagedRawSSTFileIterator.KeyValue> openedIterator;
      try {
        openedIterator = openedReader.newIterator(
            kv -> kv, openedFromSlice, openedToSlice, IteratorType.KEY_AND_VALUE);
      } catch (RuntimeException e) {
        closeQuietly(openedToSlice);
        closeQuietly(openedFromSlice);
        openedReader.close();
        throw e;
      }
      this.reader = openedReader;
      this.iterator = openedIterator;
      this.fromSlice = openedFromSlice;
      this.toSlice = openedToSlice;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public RawSstHeapHead next() {
      ManagedRawSSTFileIterator.KeyValue keyValue = iterator.next();
      int type = keyValue.getType();
      CodecBuffer valueBuffer = type == ROCKS_TYPE_VALUE ? keyValue.getValue() : null;
      return new RawSstHeapHead(
          copyBuffer(keyValue.getKey()),
          keyValue.getSequence().longValue(),
          type,
          valueBuffer);
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      if (iterator != null) {
        iterator.close();
      }
      if (reader != null) {
        reader.close();
      }
      closeQuietly(toSlice);
      closeQuietly(fromSlice);
    }

    private static ManagedSlice toManagedSlice(byte[] key) {
      if (key == null || key.length == 0) {
        return null;
      }
      return new ManagedSlice(key);
    }

    private static void closeQuietly(ManagedSlice slice) {
      if (slice != null) {
        slice.close();
      }
    }
  }

  /**
   * A merged RocksDB internal key after applying snapshot-diff emit rules across
   * multiple SST sources.
   * <p>
   * Each instance represents one emitted version for a user key: either a value
   * ({@link #ROCKS_TYPE_VALUE}) or a tombstone (any other RocksDB value type).
   * When the latest value has a higher sequence than the latest tombstone for
   * the same user key, both are emitted (delete followed by recreate). Otherwise
   * only the winning tombstone or value is emitted.
   * <p>
   * Key and value arrays are owned by this object. Callers must treat returned
   * {@code byte[]} references as read-only; snapshot-diff integration should
   * consume them once without retaining mutable aliases.
   */
  public static final class MergedKeyValue implements MergeHead {
    private final byte[] key;
    private final UnsignedLong sequence;
    private final int type;
    private final byte[] value;

    /** Creates a merged entry for unit tests. */
    @VisibleForTesting
    public static MergedKeyValue of(byte[] key, long sequence, int type, byte[] value) {
      return new MergedKeyValue(
          Arrays.copyOf(key, key.length),
          UnsignedLong.fromLongBits(sequence),
          type,
          value == null ? null : Arrays.copyOf(value, value.length));
    }

    private MergedKeyValue(byte[] key, UnsignedLong sequence, int type, byte[] value) {
      this.key = key;
      this.sequence = Objects.requireNonNull(sequence, "sequence cannot be null");
      this.type = type;
      this.value = value;
    }

    /** Returns the user key bytes shared by all versions merged for this emit. */
    @Override
    public byte[] getUserKey() {
      return key;
    }

    /** Returns the RocksDB sequence number as a signed {@code long}. */
    @Override
    public long getSequence() {
      return sequence.longValue();
    }

    /** Returns the RocksDB internal value type for this record. */
    public int getValueType() {
      return type;
    }

    /** Returns the RocksDB sequence number. */
    public UnsignedLong getSequenceNumber() {
      return sequence;
    }

    /** Returns the value bytes, or {@code null} for tombstones. */
    public byte[] getValue() {
      return value;
    }

    /** Returns {@code true} when this record is a tombstone rather than a value. */
    @Override
    public boolean isTombstone() {
      return type != ROCKS_TYPE_VALUE;
    }

    @Override
    public MergedKeyValue toMergedKeyValue() {
      return this;
    }
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
