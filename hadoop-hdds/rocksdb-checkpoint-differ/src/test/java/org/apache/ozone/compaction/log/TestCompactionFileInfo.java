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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

    CompactionFileInfo compactionFileInfo =
        new CompactionFileInfo.Builder(fileName).setStartRange(startRange)
            .setEndRange(endRange).setColumnFamily(columnFamily).build();
    Assertions.assertNotNull(compactionFileInfo);
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
    RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
        () -> new CompactionFileInfo.Builder(fileName).setStartRange(startRange)
            .setEndRange(endRange).setColumnFamily(columnFamily).build());
    assertEquals(expectedMessage, exception.getMessage());
  }
}
