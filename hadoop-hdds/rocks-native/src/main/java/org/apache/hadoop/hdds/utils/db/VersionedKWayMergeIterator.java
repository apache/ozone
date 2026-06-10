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
 * Generic dual-heap k-way merge over versioned key streams.
 * <p>
 * Two heaps track non-tombstones and tombstones separately. For each user key,
 * all versions are drained from both heaps, then {@link VersionedMergeEmitter}
 * decides what to output. {@link VersionedMergeComparators} controls user-key
 * grouping and heap ordering.
 * <p>
 * Snapshot diff uses {@link LatestVersionMergeComparators} and
 * {@link LatestVersionMergeEmitter} to capture delete-recreate patterns.
 */
public final class VersionedKWayMergeIterator implements ClosableIterator<VersionedKWayMergeIterator.MergedKeyValue> {

  public static final int ROCKS_TYPE_VALUE = VersionedMergeEntry.ROCKS_TYPE_VALUE;
  private static final int DEFAULT_READ_AHEAD_SIZE = 2 * 1024 * 1024;

  private final ManagedOptions options;
  private final List<ClosableIterator<? extends VersionedMergeEntry>> iterators;
  private final VersionedMergeComparators comparators;
  private final VersionedMergeEmitter emitter;

  private final PriorityQueue<HeapEntry> valueHeap;
  private final PriorityQueue<HeapEntry> tombstoneHeap;

  private List<MergedKeyValue> emitQueue;
  private boolean initialized;

  public static VersionedKWayMergeIterator overRawSstFiles(Collection<Path> sstFiles)
      throws IOException {
    return overRawSstFiles(sstFiles, DEFAULT_READ_AHEAD_SIZE);
  }

  public static VersionedKWayMergeIterator overRawSstFiles(Collection<Path> sstFiles,
      int readAheadSizePerFile) throws IOException {
    return overRawSstFiles(sstFiles, readAheadSizePerFile,
        LatestVersionMergeComparators.INSTANCE, LatestVersionMergeEmitter.INSTANCE);
  }

  public static VersionedKWayMergeIterator overRawSstFiles(Collection<Path> sstFiles,
      int readAheadSizePerFile, VersionedMergeComparators comparators,
      VersionedMergeEmitter emitter) throws IOException {
    Objects.requireNonNull(sstFiles, "sstFiles cannot be null");
    Objects.requireNonNull(comparators, "comparators cannot be null");
    Objects.requireNonNull(emitter, "emitter cannot be null");
    ManagedOptions options = new ManagedOptions();
    List<ClosableIterator<? extends VersionedMergeEntry>> sources = new ArrayList<>(sstFiles.size());
    for (Path file : sstFiles) {
      sources.add(new RawSstIterator(options, file, readAheadSizePerFile));
    }
    return new VersionedKWayMergeIterator(options, sources, comparators, emitter);
  }

  @VisibleForTesting
  VersionedKWayMergeIterator(List<ClosableIterator<? extends VersionedMergeEntry>> iterators) {
    this(null, iterators, LatestVersionMergeComparators.INSTANCE,
        LatestVersionMergeEmitter.INSTANCE);
  }

  @VisibleForTesting
  public static VersionedKWayMergeIterator forTest(
      List<ClosableIterator<MergedKeyValue>> iterators) {
    return forTest(iterators, LatestVersionMergeComparators.INSTANCE,
        LatestVersionMergeEmitter.INSTANCE);
  }

  @VisibleForTesting
  public static VersionedKWayMergeIterator forTest(
      List<ClosableIterator<MergedKeyValue>> iterators,
      VersionedMergeComparators comparators, VersionedMergeEmitter emitter) {
    List<ClosableIterator<? extends VersionedMergeEntry>> sources = new ArrayList<>(iterators.size());
    sources.addAll(iterators);
    return new VersionedKWayMergeIterator(null, sources, comparators, emitter);
  }

  private VersionedKWayMergeIterator(
      ManagedOptions options,
      List<ClosableIterator<? extends VersionedMergeEntry>> iterators,
      VersionedMergeComparators comparators,
      VersionedMergeEmitter emitter) {
    this.options = options;
    this.iterators = new ArrayList<>(Objects.requireNonNull(iterators, "iterators cannot be null"));
    this.comparators = Objects.requireNonNull(comparators, "comparators cannot be null");
    this.emitter = Objects.requireNonNull(emitter, "emitter cannot be null");
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
      int cmp = comparators.compareUserKeys(
          valueHeap.peek().current.getUserKey(), tombstoneHeap.peek().current.getUserKey());
      nextKey = cmp <= 0
          ? valueHeap.peek().current.getUserKey()
          : tombstoneHeap.peek().current.getUserKey();
    } else if (!valueHeap.isEmpty()) {
      nextKey = valueHeap.peek().current.getUserKey();
    } else {
      nextKey = tombstoneHeap.peek().current.getUserKey();
    }

    VersionedMergeEntry latestValue = null;
    long latestValueSeq = -1L;
    VersionedMergeEntry latestTombstone = null;
    long latestTombstoneSeq = -1L;

    while (hasUserKey(valueHeap, nextKey) || hasUserKey(tombstoneHeap, nextKey)) {
      DrainedVersion valueRound = drainHeapForUserKey(valueHeap, nextKey);
      if (valueRound.entry != null && valueRound.sequence > latestValueSeq) {
        latestValue = valueRound.entry;
        latestValueSeq = valueRound.sequence;
      }
      DrainedVersion tombstoneRound = drainHeapForUserKey(tombstoneHeap, nextKey);
      if (tombstoneRound.entry != null && tombstoneRound.sequence > latestTombstoneSeq) {
        latestTombstone = tombstoneRound.entry;
        latestTombstoneSeq = tombstoneRound.sequence;
      }
    }

    emitter.emit(latestValue, latestTombstone, emitQueue);
  }

  private boolean hasUserKey(PriorityQueue<HeapEntry> heap, byte[] userKey) {
    return !heap.isEmpty()
        && comparators.compareUserKeys(heap.peek().current.getUserKey(), userKey) == 0;
  }

  private DrainedVersion drainHeapForUserKey(PriorityQueue<HeapEntry> heap, byte[] userKey)
      throws IOException {
    VersionedMergeEntry latest = null;
    long latestSeq = -1L;

    while (true) {
      List<HeapEntry> polled = new ArrayList<>();
      while (!heap.isEmpty()
          && comparators.compareUserKeys(heap.peek().current.getUserKey(), userKey) == 0) {
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
      ClosableIterator<? extends VersionedMergeEntry> iterator = iterators.get(idx);
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

  static MergedKeyValue toMergedKeyValue(VersionedMergeEntry entry) {
    if (entry instanceof MergedKeyValue) {
      return (MergedKeyValue) entry;
    }
    if (entry instanceof RawSstHeapHead) {
      return ((RawSstHeapHead) entry).toMergedKeyValue();
    }
    throw new IllegalStateException("Unexpected entry type: " + entry.getClass());
  }

  private static final class RawSstHeapHead implements VersionedMergeEntry {
    private final byte[] key;
    private final long sequence;
    private final int type;
    private final CodecBuffer valueBuffer;

    RawSstHeapHead(byte[] key, long sequence, int type, CodecBuffer valueBuffer) {
      this.key = Objects.requireNonNull(key, "key cannot be null");
      this.sequence = sequence;
      this.type = type;
      this.valueBuffer = valueBuffer;
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
    public int getValueType() {
      return type;
    }

    MergedKeyValue toMergedKeyValue() {
      byte[] value = null;
      if (!isTombstone() && valueBuffer != null) {
        value = copyBuffer(valueBuffer);
      }
      return new MergedKeyValue(key, UnsignedLong.fromLongBits(sequence), type, value);
    }
  }

  private static final class DrainedVersion {
    private final VersionedMergeEntry entry;
    private final long sequence;

    private DrainedVersion(VersionedMergeEntry entry, long sequence) {
      this.entry = entry;
      this.sequence = sequence;
    }
  }

  private final class HeapEntry implements Comparable<HeapEntry> {
    private final int index;
    private final ClosableIterator<? extends VersionedMergeEntry> iterator;
    private VersionedMergeEntry current;

    private HeapEntry(int index, ClosableIterator<? extends VersionedMergeEntry> iterator)
        throws IOException {
      this.index = index;
      this.iterator = iterator;
      advance();
    }

    private void advance() throws IOException {
      if (iterator.hasNext()) {
        current = iterator.next();
      } else {
        current = null;
        iterator.close();
      }
    }

    @Override
    public int compareTo(HeapEntry other) {
      return comparators.compareHeapOrder(
          this.current, this.index, other.current, other.index);
    }
  }

  private static final class RawSstIterator implements ClosableIterator<RawSstHeapHead> {
    private final ManagedRawSSTFileReader reader;
    private final ManagedRawSSTFileIterator<ManagedRawSSTFileIterator.KeyValue> iterator;

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

    @Override
    public void close() {
      iterator.close();
      reader.close();
    }
  }

  public static final class MergedKeyValue implements VersionedMergeEntry {
    private final byte[] key;
    private final UnsignedLong sequence;
    private final int type;
    private final byte[] value;

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

    @Override
    public byte[] getUserKey() {
      return key;
    }

    @Override
    public long getSequence() {
      return sequence.longValue();
    }

    @Override
    public int getValueType() {
      return type;
    }

    public UnsignedLong getSequenceNumber() {
      return sequence;
    }

    public byte[] getValue() {
      return value;
    }

    @Override
    public boolean isTombstone() {
      return type != ROCKS_TYPE_VALUE;
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
