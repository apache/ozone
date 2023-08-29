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

package org.apache.ozone.rocksdiff;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.CompactionLogEntryProto;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.SST_FILE_EXTENSION_LENGTH;

/**
 * Compaction log entry Dao to write to the compaction log file.
 */
public class CompactionLogEntry {
  private final long dbSequenceNumber;
  private final List<FileInfo> inputFileInfoList;
  private final List<FileInfo> outputFileInfoList;

  public CompactionLogEntry(long dbSequenceNumber,
                            List<FileInfo> inputFileInfoList,
                            List<FileInfo> outputFileInfoList) {
    this.dbSequenceNumber = dbSequenceNumber;
    this.inputFileInfoList = inputFileInfoList;
    this.outputFileInfoList = outputFileInfoList;
  }

  public List<FileInfo> getInputFileInfoList() {
    return inputFileInfoList;
  }

  public List<FileInfo> getOutputFileInfoList() {
    return outputFileInfoList;
  }

  public long getDbSequenceNumber() {
    return dbSequenceNumber;
  }

  public CompactionLogEntryProto getProtobuf() {
    CompactionLogEntryProto.Builder builder = CompactionLogEntryProto
        .newBuilder()
        .setDbSequenceNumber(dbSequenceNumber);

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
    List<FileInfo> inputFileInfo = proto.getInputFileIntoListList().stream()
        .map(FileInfo::getFromProtobuf)
        .collect(Collectors.toList());

    List<FileInfo> outputFileInfo = proto.getOutputFileIntoListList().stream()
        .map(FileInfo::getFromProtobuf)
        .collect(Collectors.toList());


    return new CompactionLogEntry(proto.getDbSequenceNumber(),
        inputFileInfo, outputFileInfo);
  }

  public String toEncodedString() {
    // Encoding is used to deal with \n. Protobuf appends \n after each
    // parameter. If ByteArray is simply converted to a string or
    // protobuf.toString(), it will contain \n and will be added to the log.
    // Which creates a problem while reading compaction logs.
    // Compaction log lines are read sequentially assuming each line is one
    // compaction log entry.
    return Base64.getEncoder().encodeToString(getProtobuf().toByteArray());
  }

  public static CompactionLogEntry fromEncodedString(String string) {
    try {
      byte[] decode = Base64.getDecoder().decode(string);
      return getFromProtobuf(CompactionLogEntryProto.parseFrom(decode));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return String.format("dbSequenceNumber: '%s', inputFileInfoList: '%s'," +
            " outputFileInfoList: '%s',", dbSequenceNumber, inputFileInfoList,
        outputFileInfoList);
  }

  public static CompactionLogEntry fromCompactionFiles(
      long dbSequenceNumber,
      List<String> inputFiles,
      List<String> outputFiles
  ) {

    try (ManagedOptions options = new ManagedOptions();
         ManagedReadOptions readOptions = new ManagedReadOptions()) {
      List<FileInfo> inputFileInfos = convertFileInfo(inputFiles, options,
          readOptions);
      List<FileInfo> outputFileInfos = convertFileInfo(outputFiles, options,
          readOptions);
      return new CompactionLogEntry(dbSequenceNumber, inputFileInfos,
          outputFileInfos);
    }
  }

  private static List<FileInfo> convertFileInfo(
      List<String> sstFiles,
      ManagedOptions options,
      ManagedReadOptions readOptions
  ) {
    if (CollectionUtils.isEmpty(sstFiles)) {
      return Collections.emptyList();
    }

    final int fileNameOffset = sstFiles.get(0).lastIndexOf("/") + 1;
    List<FileInfo> response = new ArrayList<>();

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

        FileInfo fileInfo = new FileInfo(fileName, startKey, endKey,
            columnFamily);
        response.add(fileInfo);
      } catch (RocksDBException rocksDBException) {
        throw new RuntimeException("Failed to read SST file: " + sstFile,
            rocksDBException);
      }
    }
    return response;
  }
}
