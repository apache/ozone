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

package org.apache.hadoop.hdds.utils.db;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

/**
 * Tests for RocksDatabaseException.
 */
public class TestRocksDatabaseException {

  /**
   * When there's no space left on disk, RocksDB throws the following exception.
   * `org.rocksdb.RocksDBException: While appending to file: /test-data/testdb/000008.log: No space left on device`
   */
  @Test
  void testNoSpaceDetectedFromMessage() {
    IOException ioException = new IOException("Put Key failed",
        new RocksDatabaseException("IOError(NoSpace): " +
            "RocksDatabase[/data/metadata/db/CID-5544c724-d702-4c2b-b8c5-948d6c36bde0/" +
            "DS-58c1b346-6d46-4b1f-a641-2a149f3a6292/container.db]: Failed to batchWrite"));

    assertTrue(RocksDatabaseException.isNoSpaceError(ioException));
  }

  @Test
  void testNoSpaceDetectedFromRocksCause() {
    RocksDBException rocksCause = new RocksDBException("No space left on device");
    RocksDatabaseException dbException =
        new RocksDatabaseException("Failed to open DB", rocksCause);

    assertTrue(RocksDatabaseException.isNoSpaceError(dbException));
  }

  @Test
  void testNoSpaceNotDetectedForOtherErrors() {
    IOException ioException = new IOException("Permission denied");

    assertFalse(RocksDatabaseException.isNoSpaceError(ioException));
  }
}
