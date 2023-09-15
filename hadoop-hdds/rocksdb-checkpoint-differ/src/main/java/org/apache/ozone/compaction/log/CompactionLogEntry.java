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

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.CompactionLogEntryProto;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.SST_FILE_EXTENSION_LENGTH;

/**
 * Compaction log entry Dao to write to the compaction log file.
 */
public class CompactionLogEntry implements CopyObject<CompactionLogEntry> {
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

  public CompactionLogEntry(long dbSequenceNumber,
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

  public CompactionLogEntryProto getProtobuf() {
    CompactionLogEntryProto.Builder builder = CompactionLogEntryProto
        .newBuilder()
        .setDbSequenceNumber(dbSequenceNumber)
        .setCompactionTime(compactionTime);

    if (compactionReason != null) {
      builder.setCompactionReason(compactionReason);
    }

    if (inputFileInfoList != null) {
      inputFileInfoList.forEach(fileInfo ->
          builder.addInputFileIntoList(fileInfo.getProtobuf()));
    }

    if (outputFileInfoList != null) {
      outputFileInfoList.forEach(fileInfo ->
          builder.addOutputFileIntoList(fileInfo.getProtobuf()));
    }

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

    return new CompactionLogEntry(proto.getDbSequenceNumber(),
        proto.getCompactionTime(), inputFileInfo, outputFileInfo,
        proto.getCompactionReason());
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
    private long dbSequenceNumber;
    private long compactionTime;
    private List<String> inputFiles;
    private List<String> outputFiles;
    private String compactionReason;

    public Builder() {
    }

    public Builder setDbSequenceNumber(long dbSequenceNumber) {
      this.dbSequenceNumber = dbSequenceNumber;
      return this;
    }

    public Builder setCompactionTime(long compactionTime) {
      this.compactionTime = compactionTime;
      return this;
    }
    public Builder setInputFiles(List<String> inputFiles) {
      this.inputFiles = inputFiles;
      return this;
    }

    public Builder setOutputFiles(List<String> outputFiles) {
      this.outputFiles = outputFiles;
      return this;
    }

    public Builder setCompactionReason(String compactionReason) {
      this.compactionReason = compactionReason;
      return this;
    }

    public CompactionLogEntry build() {
      try (ManagedOptions options = new ManagedOptions();
           ManagedReadOptions readOptions = new ManagedReadOptions()) {
        return new CompactionLogEntry(dbSequenceNumber, compactionTime,
            toFileInfoList(inputFiles, options, readOptions),
            toFileInfoList(outputFiles, options, readOptions),
            compactionReason);
      }
    }

    private List<CompactionFileInfo> toFileInfoList(
        List<String> sstFiles,
        ManagedOptions options,
        ManagedReadOptions readOptions
    ) {
      if (CollectionUtils.isEmpty(sstFiles)) {
        return Collections.emptyList();
      }

      final int fileNameOffset = sstFiles.get(0).lastIndexOf("/") + 1;
      List<CompactionFileInfo> response = new ArrayList<>();

      for (String sstFile : sstFiles) {
        String fileName = sstFile.substring(fileNameOffset,
            sstFile.length() - SST_FILE_EXTENSION_LENGTH);
        SstFileReader fileReader = new SstFileReader(options);
        try {
          fileReader.open(sstFile);
          String columnFamily = StringUtils.bytes2String(
              fileReader.getTableProperties().getColumnFamilyName());
          SstFileReaderIterator iterator = fileReader.newIterator(readOptions);
          iterator.seekToFirst();
          String startKey = StringUtils.bytes2String(iterator.key());
          iterator.seekToLast();
          String endKey = StringUtils.bytes2String(iterator.key());

          CompactionFileInfo fileInfo =
              new CompactionFileInfo(fileName, startKey, endKey, columnFamily);
          response.add(fileInfo);
        } catch (RocksDBException rocksDBException) {
          throw new RuntimeException("Failed to read SST file: " + sstFile,
              rocksDBException);
        }
      }
      return response;
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
