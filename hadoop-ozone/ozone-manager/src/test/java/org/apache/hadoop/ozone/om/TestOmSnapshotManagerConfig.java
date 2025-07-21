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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Unit test for OmSnapshotManager configuration validation.
 */
class TestOmSnapshotManagerConfig {

  @ParameterizedTest
  @CsvSource({
      "-2, true,  'Invalid value: -2 should throw IllegalArgumentException'",
      "-1, false, 'Valid value: -1 should not throw exception'",
      "0,  false, 'Valid value: 0 should not throw exception'",
      "1,  false, 'Valid value: 1 should not throw exception'",
  })
  @Execution(ExecutionMode.CONCURRENT)
  void testMaxOpenFilesConfig(int maxOpenFiles, boolean shouldThrowException,
      String description, @TempDir File tempDir) {
    OzoneConfiguration conf = new OzoneConfiguration();

    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempDir.getAbsolutePath());
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
    conf.setInt(OMConfigKeys.OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES, maxOpenFiles);

    // Configure dynamic ports to avoid conflicts during parallel execution
    conf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(OMConfigKeys.OZONE_OM_RATIS_PORT_KEY, "0");
    conf.set(OMConfigKeys.OZONE_OM_GRPC_PORT_KEY, "0");

    if (shouldThrowException) {
      assertThrows(IllegalArgumentException.class, () -> new OmTestManagers(conf), description);
    } else {
      OmTestManagers testManagers = assertDoesNotThrow(() -> new OmTestManagers(conf), description);
      testManagers.stop();
    }
  }
}
