/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.file;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS_IN_GIVENPATH;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.NONE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for static helpers in {@link OMFileRequest}.
 */
public class TestOMFileRequest {

  private static Stream<Arguments> directoryResults() {
    return Stream.of(
        Arguments.of(DIRECTORY_EXISTS, false, NOT_A_FILE),
        Arguments.of(DIRECTORY_EXISTS, true, NOT_A_FILE),
        Arguments.of(FILE_EXISTS_IN_GIVENPATH, false, NOT_A_FILE),
        Arguments.of(FILE_EXISTS_IN_GIVENPATH, true, NOT_A_FILE),
        Arguments.of(FILE_EXISTS, false, FILE_ALREADY_EXISTS),
        Arguments.of(FILE_EXISTS, true, null),
        Arguments.of(DIRECTORY_EXISTS_IN_GIVENPATH, false, null),
        Arguments.of(DIRECTORY_EXISTS_IN_GIVENPATH, true, null),
        Arguments.of(NONE, false, null),
        Arguments.of(NONE, true, null));
  }

  @ParameterizedTest
  @MethodSource("directoryResults")
  public void testCheckDirectoryResult(OMDirectoryResult result, boolean isOverWrite,
      OMException.ResultCodes expected) {
    if (expected == null) {
      assertDoesNotThrow(() -> OMFileRequest.checkDirectoryResult("a/b", isOverWrite, result));
    } else {
      OMException ex = assertThrows(OMException.class,
          () -> OMFileRequest.checkDirectoryResult("a/b", isOverWrite, result));
      assertEquals(expected, ex.getResult());
    }
  }
}
