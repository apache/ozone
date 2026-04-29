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

package org.apache.hadoop.ozone.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import org.apache.hadoop.ozone.local.LocalOzoneClusterConfig.FormatMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link LocalOzoneClusterConfig}.
 */
class TestLocalOzoneClusterConfig {

  @TempDir
  private Path tempDir;

  @Test
  void builderSetsDefaults() {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(tempDir)
        .build();

    assertEquals(tempDir.toAbsolutePath().normalize(), config.getDataDir());
    assertEquals(FormatMode.IF_NEEDED, config.getFormatMode());
    assertEquals(LocalOzoneClusterConfig.DEFAULT_DATANODES,
        config.getDatanodes());
    assertEquals(LocalOzoneClusterConfig.DEFAULT_HOST, config.getHost());
    assertEquals(LocalOzoneClusterConfig.DEFAULT_BIND_HOST,
        config.getBindHost());
    assertEquals(LocalOzoneClusterConfig.DEFAULT_STARTUP_TIMEOUT,
        config.getStartupTimeout());
    assertFalse(config.isEphemeral());
  }

  @Test
  void builderAcceptsCustomValues() {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(tempDir)
        .setFormatMode(FormatMode.ALWAYS)
        .setDatanodes(3)
        .setHost("192.168.1.100")
        .setBindHost("192.168.1.100")
        .setStartupTimeout(Duration.ofMinutes(5))
        .setEphemeral(true)
        .build();

    assertEquals(FormatMode.ALWAYS, config.getFormatMode());
    assertEquals(3, config.getDatanodes());
    assertEquals("192.168.1.100", config.getHost());
    assertEquals("192.168.1.100", config.getBindHost());
    assertEquals(Duration.ofMinutes(5), config.getStartupTimeout());
    assertTrue(config.isEphemeral());
  }

  @Test
  void builderRejectsInvalidDatanodeCount() {
    LocalOzoneClusterConfig.Builder builder =
        LocalOzoneClusterConfig.builder(tempDir);

    assertThrows(IllegalArgumentException.class,
        () -> builder.setDatanodes(0));
    assertThrows(IllegalArgumentException.class,
        () -> builder.setDatanodes(-1));
  }

  @Test
  void dataDirIsNormalized() {
    Path unnormalized = Paths.get(tempDir.toString(), "subdir", "..", "data");
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(unnormalized)
        .build();

    Path expected = tempDir.resolve("data").toAbsolutePath().normalize();
    assertEquals(expected, config.getDataDir());
  }

  @Test
  void formatModeFromStringParsesValidValues() {
    assertEquals(FormatMode.IF_NEEDED, FormatMode.fromString("if-needed"));
    assertEquals(FormatMode.IF_NEEDED, FormatMode.fromString("IF_NEEDED"));
    assertEquals(FormatMode.IF_NEEDED, FormatMode.fromString("If-Needed"));
    assertEquals(FormatMode.ALWAYS, FormatMode.fromString("always"));
    assertEquals(FormatMode.NEVER, FormatMode.fromString("never"));
  }

  @Test
  void formatModeFromStringRejectsInvalidValues() {
    assertThrows(IllegalArgumentException.class,
        () -> FormatMode.fromString("invalid"));
    assertThrows(IllegalArgumentException.class,
        () -> FormatMode.fromString(""));
  }
}
