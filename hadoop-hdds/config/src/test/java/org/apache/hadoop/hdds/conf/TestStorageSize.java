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

package org.apache.hadoop.hdds.conf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link StorageSize}.
 */
class TestStorageSize {

  @Test
  void parsesValueAndUnit() {
    StorageSize size = StorageSize.parse("10MB");

    assertEquals(StorageUnit.MB, size.getUnit());
    assertEquals(10.0, size.getValue());
  }

  /**
   * XML config values are commonly indented, so parse() trims its input.
   * The cases below cover whitespace shorter than, equal to, and longer than
   * the unit suffix being stripped.
   */
  @ParameterizedTest
  @ValueSource(strings = {" 10MB", "10MB ", " 10MB ", "\n    10MB\n  "})
  void parsesValueWithSurroundingWhitespace(String value) {
    StorageSize size = StorageSize.parse(value);

    assertEquals(StorageUnit.MB, size.getUnit());
    assertEquals(10.0, size.getValue());
  }

  /**
   * The default-unit overload only falls back on IllegalArgumentException,
   * so whitespace long enough to push the offset past the end of the value
   * used to escape it as a StringIndexOutOfBoundsException.
   */
  @Test
  void parsesWhitespaceWithDefaultUnit() {
    StorageSize size = StorageSize.parse("\n    5GB\n  ", StorageUnit.BYTES);

    assertEquals(StorageUnit.GB, size.getUnit());
    assertEquals(5.0, size.getValue());
  }
}
