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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Stream;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.CompactionFileInfoProto;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test class for CompactionFileInfo.
 */
public class TestCompactionFileInfo {

  private static Stream<Arguments> compactionFileInfoValidScenarios() {
    return Stream.of(
        Arguments.of("All parameters are present.",
            "fileName",
            "startRange",
            "endRange",
            "columnFamily"
        ),
        Arguments.of("Only fileName is present.",
            "fileName",
            null,
            null,
            null
        ),
        Arguments.of("Only fileName is present.",
            "fileName",
            null,
            null,
            null
        )
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("compactionFileInfoValidScenarios")
  public void testCompactionFileInfoValidScenario(String description,
                                                  String fileName,
                                                  String startRange,
                                                  String endRange,
                                                  String columnFamily) {

    CompactionFileInfo.Builder builder = new CompactionFileInfo.Builder(fileName).setStartRange(startRange)
        .setEndRange(endRange).setColumnFamily(columnFamily);
    CompactionFileInfo compactionFileInfo = builder.build();
    assertNotNull(compactionFileInfo);
    CompactionFileInfo prunedCompactionFileInfo = builder.setPruned().build();
    assertFalse(compactionFileInfo.isPruned());
    compactionFileInfo.setPruned();
    assertTrue(compactionFileInfo.isPruned());
    assertTrue(prunedCompactionFileInfo.isPruned());
  }

  private static Stream<Arguments> compactionFileInfoInvalidScenarios() {
    return Stream.of(
        Arguments.of("All parameters are null.",
            null,
            null,
            null,
            null,
            "FileName is required parameter."
        ),
        Arguments.of("fileName is null.",
            null,
            "startRange",
            "endRange",
            "columnFamily",
            "FileName is required parameter."
        ),
        Arguments.of("startRange is not present.",
            "fileName",
            null,
            "endRange",
            "columnFamily",
            "Either all of startRange, endRange and columnFamily" +
                " should be non-null or null. startRange: 'null', " +
                "endRange: 'endRange', columnFamily: 'columnFamily'."
        ),
        Arguments.of("endRange is not present.",
            "fileName",
            "startRange",
            null,
            "columnFamily",
            "Either all of startRange, endRange and columnFamily" +
                " should be non-null or null. startRange: 'startRange', " +
                "endRange: 'null', columnFamily: 'columnFamily'."
        ),
        Arguments.of("columnFamily is not present.",
            "fileName",
            "startRange",
            "endRange",
            null,
            "Either all of startRange, endRange and columnFamily" +
                " should be non-null or null. startRange: 'startRange', " +
                "endRange: 'endRange', columnFamily: 'null'."
        ),
        Arguments.of("startRange and endRange are not present.",
            "fileName",
            null,
            null,
            "columnFamily",
            "Either all of startRange, endRange and columnFamily" +
                " should be non-null or null. startRange: 'null', " +
                "endRange: 'null', columnFamily: 'columnFamily'."
        ),
        Arguments.of("endRange and columnFamily are not present.",
            "fileName",
            "startRange",
            null,
            null,
            "Either all of startRange, endRange and columnFamily " +
                "should be non-null or null. startRange: 'startRange', " +
                "endRange: 'null', columnFamily: 'null'."
        ),
        Arguments.of("startRange and columnFamily are not present.",
            "fileName",
            null,
            "endRange",
            null,
            "Either all of startRange, endRange and columnFamily" +
                " should be non-null or null. startRange: 'null', " +
                "endRange: 'endRange', columnFamily: 'null'."
        )
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("compactionFileInfoInvalidScenarios")
  public void testCompactionFileInfoInvalidScenario(String description,
                                                    String fileName,
                                                    String startRange,
                                                    String endRange,
                                                    String columnFamily,
                                                    String expectedMessage) {
    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> new CompactionFileInfo.Builder(fileName).setStartRange(startRange)
            .setEndRange(endRange).setColumnFamily(columnFamily).build());
    assertEquals(expectedMessage, exception.getMessage());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("compactionFileInfoValidScenarios")
  public void testGetProtobuf(String description,
                              String fileName,
                              String startRange,
                              String endRange,
                              String columnFamily) {
    CompactionFileInfo compactionFileInfo = new CompactionFileInfo
        .Builder(fileName)
        .setStartRange(startRange)
        .setEndRange(endRange)
        .setColumnFamily(columnFamily)
        .build();

    CompactionFileInfoProto protobuf = compactionFileInfo.getProtobuf();
    assertEquals(fileName, protobuf.getFileName());

    if (startRange != null) {
      assertEquals(startRange, protobuf.getStartKey());
    } else {
      assertFalse(protobuf.hasStartKey());
    }
    if (endRange != null) {
      assertEquals(endRange, protobuf.getEndKey());
    } else {
      assertFalse(protobuf.hasEndKey());
    }
    if (columnFamily != null) {
      assertEquals(columnFamily, protobuf.getColumnFamily());
    } else {
      assertFalse(protobuf.hasColumnFamily());
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("compactionFileInfoValidScenarios")
  public void testFromProtobuf(String description,
                               String fileName,
                               String startRange,
                               String endRange,
                               String columnFamily) {
    CompactionFileInfoProto.Builder builder = CompactionFileInfoProto
        .newBuilder()
        .setFileName(fileName);

    if (startRange != null) {
      builder = builder.setStartKey(startRange);
    }
    if (endRange != null) {
      builder = builder.setEndKey(endRange);
    }
    if (columnFamily != null) {
      builder = builder.setColumnFamily(columnFamily);
    }

    CompactionFileInfoProto protobuf = builder.build();

    CompactionFileInfo compactionFileInfo =
        CompactionFileInfo.getFromProtobuf(protobuf);

    assertEquals(fileName, compactionFileInfo.getFileName());
    assertEquals(startRange, compactionFileInfo.getStartKey());
    assertEquals(endRange, compactionFileInfo.getEndKey());
    assertEquals(columnFamily, compactionFileInfo.getColumnFamily());
    assertFalse(compactionFileInfo.isPruned());

    CompactionFileInfoProto unPrunedProtobuf = builder.setPruned(false).build();
    CompactionFileInfo unPrunedCompactionFileInfo =
        CompactionFileInfo.getFromProtobuf(unPrunedProtobuf);
    assertFalse(unPrunedCompactionFileInfo.isPruned());

    CompactionFileInfoProto prunedProtobuf = builder.setPruned(true).build();
    CompactionFileInfo prunedCompactionFileInfo =
        CompactionFileInfo.getFromProtobuf(prunedProtobuf);
    assertTrue(prunedCompactionFileInfo.isPruned());
  }
}
