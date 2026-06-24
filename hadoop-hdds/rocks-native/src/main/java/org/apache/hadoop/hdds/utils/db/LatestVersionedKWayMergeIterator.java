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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.ozone.util.ClosableIterator;

/**
 * Dual-heap k-way merge over RocksDB SST files for snapshot diff.
 * <p>
 * Two heaps track non-tombstones and tombstones separately. For each user key,
 * all versions are drained from both heaps, then snapshot-diff emit rules apply:
 * emit the latest tombstone and/or latest value, including both when a delete is
 * followed by a newer recreate.
 * <p>
 * When constructed with an exclusive minimum sequence number {@code S}, each SST
 * source skips entries whose sequence is {@code <= S} while advancing.
 */
public final class LatestVersionedKWayMergeIterator implements
    ClosableIterator<LatestVersionedKWayMergeIterator.MergedKeyValue> {

  /** RocksDB {@code ValueType::kTypeValue}. */
  public static final int ROCKS_TYPE_VALUE = 1;
  private static final int DEFAULT_READ_AHEAD_SIZE = 2 * 1024 * 1024;

  private final ManagedOptions options;
  private final List<ClosableIterator<? extends MergeHead>> iterators;
  private final Long exclusiveMinSequenceNumber;

  private final PriorityQueue<HeapEntry> valueHeap;
  private final PriorityQueue<HeapEntry> tombstoneHeap;

  private List<MergedKeyValue> emitQueue;
  private boolean initialized;

  public static LatestVersionedKWayMergeIterator overRawSstFiles(Collection<Path> sstFiles)
      throws IOException {
    return overRawSstFiles(sstFiles, DEFAULT_READ_AHEAD_SIZE, null);
  }

  public static LatestVersionedKWayMergeIterator overRawSstFiles(Collection<Path> sstFiles,
      int readAheadSizePerFile) throws IOException {
    return overRawSstFiles(sstFiles, readAheadSizePerFile, null);
  }

  /**
   * Opens one iterator per SST file and merges them.
   *
   * @param exclusiveMinSequenceNumber when non-null, each source skips entries with
   *        sequence {@code <=} this value while advancing; when null, no entries are skipped
   */
  public static LatestVersionedKWayMergeIterator overRawSstFiles(Collection<Path> sstFiles,
      int readAheadSizePerFile, Long exclusiveMinSequenceNumber) throws IOException {
    Objects.requireNonNull(sstFiles, "sstFiles cannot be null");
    ManagedOptions options = new ManagedOptions();
    List<ClosableIterator<? extends MergeHead>> sources = new ArrayList<>(sstFiles.size());
    for (Path file : sstFiles) {
      sources.add(new RawSstIterator(options, file, readAheadSizePerFile));
    }
    return new LatestVersionedKWayMergeIterator(options, sources, exclusiveMinSequenceNumber);
  }

  public static LatestVersionedKWayMergeIterator overRawSstFiles(Collection<Path> sstFiles,
      long exclusiveMinSequenceNumber) throws IOException {
    return overRawSstFiles(sstFiles, DEFAULT_READ_AHEAD_SIZE, exclusiveMinSequenceNumber);
  }

  @VisibleForTesting
  public static LatestVersionedKWayMergeIterator forTest(
      List<ClosableIterator<MergedKeyValue>> iterators) {
    return forTest(iterators, null);
  }

  @VisibleForTesting
  public static LatestVersionedKWayMergeIterator forTest(
      List<ClosableIterator<MergedKeyValue>> iterators, Long exclusiveMinSequenceNumber) {
    List<ClosableIterator<? extends MergeHead>> sources = new ArrayList<>(iterators.size());
    sources.addAll(iterators);
    return new LatestVersionedKWayMergeIterator(null, sources, exclusiveMinSequenceNumber);
  }

  private LatestVersionedKWayMergeIterator(
      ManagedOptions options,
      List<ClosableIterator<? extends MergeHead>> iterators,
      Long exclusiveMinSequenceNumber) {
    this.options = options;
    this.iterators = new ArrayList<>(Objects.requireNonNull(iterators, "iterators cannot be null"));
    this.exclusiveMinSequenceNumber = exclusiveMinSequenceNumber;
    this.valueHeap = new PriorityQueue<>(Math.max(this.iterators.size(), 1));
    this.tombstoneHeap = new PriorityQueue<>(Math.max(this.iterators.size(), 1));
    this.emitQueue = new ArrayList<>();
  }

  @Override
  public boolean hasNext() {
    if (!emitQueue.isEmpty()) {
      return true;
    }
    try {
      return advance();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public MergedKeyValue next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more elements found.");
    }
    return emitQueue.remove(0);
  }

  private boolean advance() throws IOException {
    if (!initialized) {
      initHeaps();
      initialized = true;
    }

    while (emitQueue.isEmpty() && (!valueHeap.isEmpty() || !tombstoneHeap.isEmpty())) {
      processNextUserKey();
    }

    return !emitQueue.isEmpty();
  }

  private void processNextUserKey() throws IOException {
    if (valueHeap.isEmpty() && tombstoneHeap.isEmpty()) {
      return;
    }

    byte[] nextKey = null;
    if (!valueHeap.isEmpty() && !tombstoneHeap.isEmpty()) {
      int cmp = compareUserKeys(
          valueHeap.peek().current.getUserKey(), tombstoneHeap.peek().current.getUserKey());
      nextKey = cmp <= 0
          ? valueHeap.peek().current.getUserKey()
          : tombstoneHeap.peek().current.getUserKey();
    } else if (!valueHeap.isEmpty()) {
      nextKey = valueHeap.peek().current.getUserKey();
    } else {
      nextKey = tombstoneHeap.peek().current.getUserKey();
    }

    MergeHead latestValue = null;
    long latestValueSeq = -1L;
    MergeHead latestTombstone = null;
    long latestTombstoneSeq = -1L;

    while (hasUserKey(valueHeap, nextKey) || hasUserKey(tombstoneHeap, nextKey)) {
      DrainedVersion valueRound = drainHeapForUserKey(valueHeap, nextKey, true);
      if (valueRound.entry != null && valueRound.sequence > latestValueSeq) {
        latestValue = valueRound.entry;
        latestValueSeq = valueRound.sequence;
      }
      DrainedVersion tombstoneRound = drainHeapForUserKey(tombstoneHeap, nextKey, false);
      if (tombstoneRound.entry != null && tombstoneRound.sequence > latestTombstoneSeq) {
        latestTombstone = tombstoneRound.entry;
        latestTombstoneSeq = tombstoneRound.sequence;
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

  private boolean hasUserKey(PriorityQueue<HeapEntry> heap, byte[] userKey) {
    return !heap.isEmpty()
        && compareUserKeys(heap.peek().current.getUserKey(), userKey) == 0;
  }

  private DrainedVersion drainHeapForUserKey(PriorityQueue<HeapEntry> heap, byte[] userKey,
      boolean snapshotValueWinners) throws IOException {
    MergeHead latest = null;
    long latestSeq = -1L;

    while (true) {
      List<HeapEntry> polled = new ArrayList<>();
      while (!heap.isEmpty()
          && compareUserKeys(heap.peek().current.getUserKey(), userKey) == 0) {
        HeapEntry entry = heap.poll();
        if (entry.current.getSequence() > latestSeq) {
          latest = entry.current;
          latestSeq = entry.current.getSequence();
        }
        polled.add(entry);
      }
      if (polled.isEmpty()) {
        break;
      }
      for (HeapEntry entry : polled) {
        if (snapshotValueWinners && entry.current == latest
            && entry.current instanceof RawSstHeapHead
            && !entry.nextRecordHasSameUserKey(userKey)) {
          ((RawSstHeapHead) entry.current).snapshotValue();
        }
        entry.advance();
        if (entry.current != null) {
          offerToHeap(entry);
        }
      }
    }

    return new DrainedVersion(latest, latestSeq);
  }

  private void offerToHeap(HeapEntry entry) {
    if (entry.current.isTombstone()) {
      tombstoneHeap.offer(entry);
    } else {
      valueHeap.offer(entry);
    }
  }

  private void initHeaps() throws IOException {
    for (int idx = 0; idx < iterators.size(); idx++) {
      ClosableIterator<? extends MergeHead> iterator = iterators.get(idx);
      HeapEntry entry = new HeapEntry(idx, iterator);
      if (entry.current != null) {
        offerToHeap(entry);
      }
    }
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(iterators);
    if (options != null) {
      options.close();
    }
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

  private static final class DrainedVersion {
    private final MergeHead entry;
    private final long sequence;

    private DrainedVersion(MergeHead entry, long sequence) {
      this.entry = entry;
      this.sequence = sequence;
    }
  }

  private final class HeapEntry implements Comparable<HeapEntry> {
    private final int index;
    private final ClosableIterator<? extends MergeHead> iterator;
    private MergeHead current;

    private HeapEntry(int index, ClosableIterator<? extends MergeHead> iterator)
        throws IOException {
      this.index = index;
      this.iterator = iterator;
      advance();
    }

    private void advance() throws IOException {
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

    private boolean nextRecordHasSameUserKey(byte[] userKey) {
      return iterator instanceof RawSstIterator
          && ((RawSstIterator) iterator).nextRecordHasSameUserKey(userKey);
    }

    @Override
    public int compareTo(HeapEntry other) {
      return compareHeapOrder(
          this.current, this.index, other.current, other.index);
    }
  }

  private static final class RawSstIterator implements ClosableIterator<RawSstHeapHead> {
    private final ManagedRawSSTFileReader reader;
    private final ManagedRawSSTFileIterator<ManagedRawSSTFileIterator.KeyValue> iterator;
    private boolean closed;

    private RawSstIterator(ManagedOptions options, Path file, int readAheadSize) {
      this.reader = new ManagedRawSSTFileReader(
          options, file.toAbsolutePath().toString(), readAheadSize);
      this.iterator = reader.newIterator(kv -> kv, null, null, IteratorType.KEY_AND_VALUE);
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

    private boolean nextRecordHasSameUserKey(byte[] userKey) {
      if (!iterator.hasNext()) {
        return false;
      }
      byte[] nextKey = iterator.peekKeyBytes();
      return nextKey != null && compareUserKeys(nextKey, userKey) == 0;
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      iterator.close();
      reader.close();
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
   */
  public static final class MergedKeyValue implements MergeHead {
    private final byte[] key;
    private final UnsignedLong sequence;
    private final int type;
    private final byte[] value;

    /** Creates a merged entry for unit tests. */
    @VisibleForTesting
    public static MergedKeyValue of(byte[] key, long sequence, int type, byte[] value) {
      return new MergedKeyValue(key, UnsignedLong.fromLongBits(sequence), type, value);
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
