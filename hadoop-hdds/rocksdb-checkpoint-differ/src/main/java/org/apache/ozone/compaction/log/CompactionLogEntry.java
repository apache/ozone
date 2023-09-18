/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ozone.compaction.log;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.CompactionLogEntryProto;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.util.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Compaction log entry Dao to write to the compaction log file.
 */
public final class CompactionLogEntry implements
    CopyObject<CompactionLogEntry> {
  private static final Codec<CompactionLogEntry> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(CompactionLogEntryProto.class),
      CompactionLogEntry::getFromProtobuf,
      CompactionLogEntry::getProtobuf);

  public static Codec<CompactionLogEntry> getCodec() {
    return CODEC;
  }

  private final long dbSequenceNumber;
  private final long compactionTime;
  private final List<CompactionFileInfo> inputFileInfoList;
  private final List<CompactionFileInfo> outputFileInfoList;
  private final String compactionReason;

  private CompactionLogEntry(long dbSequenceNumber,
                            long compactionTime,
                            List<CompactionFileInfo> inputFileInfoList,
                            List<CompactionFileInfo> outputFileInfoList,
                            String compactionReason) {
    this.dbSequenceNumber = dbSequenceNumber;
    this.compactionTime = compactionTime;
    this.inputFileInfoList = inputFileInfoList;
    this.outputFileInfoList = outputFileInfoList;
    this.compactionReason = compactionReason;
  }

  public List<CompactionFileInfo> getInputFileInfoList() {
    return inputFileInfoList;
  }

  public List<CompactionFileInfo> getOutputFileInfoList() {
    return outputFileInfoList;
  }

  public long getDbSequenceNumber() {
    return dbSequenceNumber;
  }

  public long getCompactionTime() {
    return compactionTime;
  }

  public String getCompactionReason() {
    return compactionReason;
  }

  public CompactionLogEntryProto getProtobuf() {
    CompactionLogEntryProto.Builder builder = CompactionLogEntryProto
        .newBuilder()
        .setDbSequenceNumber(dbSequenceNumber)
        .setCompactionTime(compactionTime);

    if (compactionReason != null) {
      builder.setCompactionReason(compactionReason);
    }

    inputFileInfoList.forEach(fileInfo ->
        builder.addInputFileIntoList(fileInfo.getProtobuf()));

    outputFileInfoList.forEach(fileInfo ->
        builder.addOutputFileIntoList(fileInfo.getProtobuf()));

    return builder.build();
  }

  public static CompactionLogEntry getFromProtobuf(
      CompactionLogEntryProto proto) {
    List<CompactionFileInfo> inputFileInfo = proto.getInputFileIntoListList()
        .stream()
        .map(CompactionFileInfo::getFromProtobuf)
        .collect(Collectors.toList());

    List<CompactionFileInfo> outputFileInfo = proto.getOutputFileIntoListList()
        .stream()
        .map(CompactionFileInfo::getFromProtobuf)
        .collect(Collectors.toList());
    Builder builder = new Builder(proto.getDbSequenceNumber(),
        proto.getCompactionTime(), inputFileInfo, outputFileInfo);

    if (proto.hasCompactionReason()) {
      builder.setCompactionReason(proto.getCompactionReason());
    }
    return builder.build();
  }

  @Override
  public String toString() {
    return String.format("dbSequenceNumber: '%s', compactionTime: '%s', " +
            "inputFileInfoList: '%s', outputFileInfoList: '%s', " +
            "compactionReason: '%s'.", dbSequenceNumber, compactionTime,
        inputFileInfoList, outputFileInfoList, compactionReason);
  }

  /**
   * Builder of CompactionLogEntry.
   */
  public static class Builder {
    private final long dbSequenceNumber;
    private final long compactionTime;
    private final List<CompactionFileInfo> inputFileInfoList;
    private final List<CompactionFileInfo> outputFileInfoList;
    private String compactionReason;

    public Builder(long dbSequenceNumber, long compactionTime,
                   List<CompactionFileInfo> inputFileInfoList,
                   List<CompactionFileInfo> outputFileInfoList) {
      Preconditions.checkNotNull(inputFileInfoList,
          "inputFileInfoList is required parameter.");
      Preconditions.checkNotNull(outputFileInfoList,
          "outputFileInfoList is required parameter.");
      this.dbSequenceNumber = dbSequenceNumber;
      this.compactionTime = compactionTime;
      this.inputFileInfoList = inputFileInfoList;
      this.outputFileInfoList = outputFileInfoList;
    }

    public Builder setCompactionReason(String compactionReason) {
      this.compactionReason = compactionReason;
      return this;
    }

    public CompactionLogEntry build() {
      return new CompactionLogEntry(dbSequenceNumber, compactionTime,
          inputFileInfoList, outputFileInfoList, compactionReason);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CompactionLogEntry)) {
      return false;
    }

    CompactionLogEntry that = (CompactionLogEntry) o;
    return dbSequenceNumber == that.dbSequenceNumber &&
        compactionTime == that.compactionTime &&
        Objects.equals(inputFileInfoList, that.inputFileInfoList) &&
        Objects.equals(outputFileInfoList, that.outputFileInfoList) &&
        Objects.equals(compactionReason, that.compactionReason);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dbSequenceNumber, compactionTime, inputFileInfoList,
        outputFileInfoList, compactionReason);
  }

  @Override
  public CompactionLogEntry copyObject() {
    return new CompactionLogEntry(dbSequenceNumber, compactionTime,
        inputFileInfoList, outputFileInfoList, compactionReason);
  }
}
