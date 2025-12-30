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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.CompactionLogEntryProto;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test class for CompactionLogEntry.
 */
public class TestCompactionLogEntry {

  private static Stream<Arguments> compactionLogEntryValidScenarios() {
    List<CompactionFileInfo> inputFiles = Arrays.asList(
        new CompactionFileInfo.Builder("inputFileName1").setStartRange("key1")
            .setEndRange("key5").setColumnFamily("columnFamily").build(),
        new CompactionFileInfo.Builder("inputFileName2").setStartRange("key6")
            .setEndRange("key11").setColumnFamily("columnFamily").build(),
        new CompactionFileInfo.Builder("inputFileName3").setStartRange("key12")
            .setEndRange("key19").setColumnFamily("columnFamily").build());
    List<CompactionFileInfo> outputFiles = Arrays.asList(
        new CompactionFileInfo.Builder("outputFileName1").setStartRange("key1")
            .setEndRange("key8").setColumnFamily("columnFamily").build(),
        new CompactionFileInfo.Builder("outputFileName2").setStartRange("key9")
            .setEndRange("key19").setColumnFamily("columnFamily").build());

    return Stream.of(
        Arguments.of("With compaction reason.",
            1000,
            Time.now(),
            inputFiles,
            outputFiles,
            "compactionReason"
        ),
        Arguments.of("Without compaction reason.",
            2000,
            Time.now(),
            inputFiles,
            outputFiles,
            null
        )
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("compactionLogEntryValidScenarios")
  public void testGetProtobuf(
      String description,
      long dbSequenceNumber,
      long compactionTime,
      List<CompactionFileInfo> inputFiles,
      List<CompactionFileInfo> outputFiles,
      String compactionReason) {
    CompactionLogEntry.Builder builder = new CompactionLogEntry
        .Builder(dbSequenceNumber, compactionTime, inputFiles, outputFiles);

    if (compactionReason != null) {
      builder.setCompactionReason(compactionReason);
    }

    CompactionLogEntry compactionLogEntry = builder.build();
    assertNotNull(compactionLogEntry);
    CompactionLogEntryProto protobuf =
        compactionLogEntry.getProtobuf();
    assertEquals(dbSequenceNumber, protobuf.getDbSequenceNumber());
    assertEquals(compactionTime, protobuf.getCompactionTime());
    assertEquals(inputFiles, protobuf.getInputFileIntoListList().stream()
        .map(CompactionFileInfo::getFromProtobuf)
        .collect(Collectors.toList()));
    assertEquals(outputFiles, protobuf.getOutputFileIntoListList().stream()
        .map(CompactionFileInfo::getFromProtobuf)
        .collect(Collectors.toList()));
    if (compactionReason != null) {
      assertEquals(compactionReason, protobuf.getCompactionReason());
    } else {
      assertFalse(protobuf.hasCompactionReason());
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("compactionLogEntryValidScenarios")
  public void testFromProtobuf(
      String description,
      long dbSequenceNumber,
      long compactionTime,
      List<CompactionFileInfo> inputFiles,
      List<CompactionFileInfo> outputFiles,
      String compactionReason) {

    CompactionLogEntryProto.Builder builder = CompactionLogEntryProto
        .newBuilder()
        .setDbSequenceNumber(dbSequenceNumber)
        .setCompactionTime(compactionTime);

    if (compactionReason != null) {
      builder.setCompactionReason(compactionReason);
    }

    inputFiles.forEach(fileInfo ->
        builder.addInputFileIntoList(fileInfo.getProtobuf()));

    outputFiles.forEach(fileInfo ->
        builder.addOutputFileIntoList(fileInfo.getProtobuf()));

    CompactionLogEntryProto protobuf1 = builder.build();

    CompactionLogEntry compactionLogEntry =
        CompactionLogEntry.getFromProtobuf(protobuf1);

    assertNotNull(compactionLogEntry);
    assertEquals(dbSequenceNumber, compactionLogEntry.getDbSequenceNumber());
    assertEquals(compactionTime, compactionLogEntry.getCompactionTime());
    assertEquals(inputFiles, compactionLogEntry.getInputFileInfoList());
    assertEquals(outputFiles, compactionLogEntry.getOutputFileInfoList());
    assertEquals(compactionReason, compactionLogEntry.getCompactionReason());
  }
}
