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

package org.apache.ozone.compaction.log;

import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.FlushLogEntryProto;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;

/**
 * Flush log entry Dao to write to the flush log table.
 * Similar to CompactionLogEntry but used for tracking flush operations.
 * Each entry represents a single L0 SST file created by a flush.
 */
public final class FlushLogEntry implements CopyObject<FlushLogEntry> {
  private static final Codec<FlushLogEntry> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(FlushLogEntryProto.getDefaultInstance()),
      FlushLogEntry::getFromProtobuf,
      FlushLogEntry::getProtobuf,
      FlushLogEntry.class);

  private final long dbSequenceNumber;
  private final long flushTime;
  private final FlushFileInfo fileInfo;
  private final String flushReason;

  @VisibleForTesting
  public FlushLogEntry(long dbSequenceNumber,
                       long flushTime,
                       FlushFileInfo fileInfo,
                       String flushReason) {
    this.dbSequenceNumber = dbSequenceNumber;
    this.flushTime = flushTime;
    this.fileInfo = Objects.requireNonNull(fileInfo, "fileInfo == null");
    this.flushReason = flushReason;
  }

  public static Codec<FlushLogEntry> getCodec() {
    return CODEC;
  }

  public long getDbSequenceNumber() {
    return dbSequenceNumber;
  }

  public long getFlushTime() {
    return flushTime;
  }

  public FlushFileInfo getFileInfo() {
    return fileInfo;
  }

  public String getFlushReason() {
    return flushReason;
  }

  public FlushLogEntryProto getProtobuf() {
    FlushLogEntryProto.Builder builder = FlushLogEntryProto
        .newBuilder()
        .setDbSequenceNumber(dbSequenceNumber)
        .setFlushTime(flushTime)
        .setFileInfo(fileInfo.getProtobuf());

    if (flushReason != null) {
      builder.setFlushReason(flushReason);
    }

    return builder.build();
  }

  public static FlushLogEntry getFromProtobuf(FlushLogEntryProto proto) {
    FlushFileInfo fileInfo = FlushFileInfo.getFromProtobuf(proto.getFileInfo());
    Builder builder = new Builder(proto.getDbSequenceNumber(),
        proto.getFlushTime(), fileInfo);

    if (proto.hasFlushReason()) {
      builder.setFlushReason(proto.getFlushReason());
    }
    return builder.build();
  }

  @Override
  public String toString() {
    return String.format("FlushLogEntry{dbSequenceNumber: %d, flushTime: %d, " +
            "fileInfo: %s, flushReason: '%s'}",
        dbSequenceNumber, flushTime, fileInfo, flushReason);
  }

  public Builder toBuilder() {
    Builder builder = new Builder(this.getDbSequenceNumber(),
        this.getFlushTime(), this.getFileInfo());
    if (this.getFlushReason() != null) {
      builder.setFlushReason(this.getFlushReason());
    }
    return builder;
  }

  /**
   * Builder of FlushLogEntry.
   */
  public static class Builder {
    private final long dbSequenceNumber;
    private final long flushTime;
    private final FlushFileInfo fileInfo;
    private String flushReason;

    public Builder(long dbSequenceNumber, long flushTime,
                   FlushFileInfo fileInfo) {
      this.dbSequenceNumber = dbSequenceNumber;
      this.flushTime = flushTime;
      this.fileInfo = Objects.requireNonNull(fileInfo, "fileInfo == null");
    }

    public Builder setFlushReason(String flushReason) {
      this.flushReason = flushReason;
      return this;
    }

    public FlushLogEntry build() {
      return new FlushLogEntry(dbSequenceNumber, flushTime,
          fileInfo, flushReason);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FlushLogEntry)) {
      return false;
    }

    FlushLogEntry that = (FlushLogEntry) o;
    return dbSequenceNumber == that.dbSequenceNumber &&
        flushTime == that.flushTime &&
        Objects.equals(fileInfo, that.fileInfo) &&
        Objects.equals(flushReason, that.flushReason);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dbSequenceNumber, flushTime, fileInfo, flushReason);
  }

  @Override
  public FlushLogEntry copyObject() {
    return new FlushLogEntry(dbSequenceNumber, flushTime, fileInfo, flushReason);
  }
}
