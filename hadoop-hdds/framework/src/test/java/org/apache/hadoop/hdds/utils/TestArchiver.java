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

package org.apache.hadoop.hdds.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Test {@link Archiver}. */
class TestArchiver {

  @ParameterizedTest
  @ValueSource(longs = { 0, 1, Archiver.MIN_BUFFER_SIZE - 1 })
  void bufferSizeBelowMinimum(long fileSize) {
    assertThat(Archiver.getBufferSize(fileSize))
        .isEqualTo(Archiver.MIN_BUFFER_SIZE);
  }

  @ParameterizedTest
  @ValueSource(longs = { Archiver.MIN_BUFFER_SIZE, Archiver.MAX_BUFFER_SIZE })
  void bufferSizeBetweenLimits(long fileSize) {
    assertThat(Archiver.getBufferSize(fileSize))
        .isEqualTo(fileSize);
  }

  @ParameterizedTest
  @ValueSource(longs = { Archiver.MAX_BUFFER_SIZE + 1, Integer.MAX_VALUE, Long.MAX_VALUE })
  void bufferSizeAboveMaximum(long fileSize) {
    assertThat(Archiver.getBufferSize(fileSize))
        .isEqualTo(Archiver.MAX_BUFFER_SIZE);
  }

}
