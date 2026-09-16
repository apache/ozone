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

import java.nio.file.Paths;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LocalOzoneClusterConfig}.
 */
class TestLocalOzoneClusterConfig {

  @Test
  void builderProvidesLocalClusterDefaults() {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder().build();

    assertEquals(LocalOzoneClusterConfig.DEFAULT_DATA_DIR,
        config.getDataDir());
    assertEquals(LocalOzoneClusterConfig.FormatMode.IF_NEEDED,
        config.getFormatMode());
    assertEquals(1, config.getDatanodes());
    assertEquals("127.0.0.1", config.getHost());
    assertEquals("0.0.0.0", config.getBindHost());
    assertEquals(0, config.getScmPort());
    assertEquals(0, config.getOmPort());
    assertEquals(0, config.getS3gPort());
    assertTrue(config.isS3gEnabled());
    assertFalse(config.isEphemeral());
    assertEquals(Duration.ofMinutes(2), config.getStartupTimeout());
    assertEquals("admin", config.getS3AccessKey());
    assertEquals("admin123", config.getS3SecretKey());
    assertEquals("us-east-1", config.getS3Region());
  }

  @Test
  void typedDefaultsMatchSharedFallbackValues() {
    assertEquals(LocalOzoneClusterConfig.FormatMode.fromString(
        LocalOzoneClusterConfig.DEFAULT_FORMAT_MODE_VALUE),
        LocalOzoneClusterConfig.DEFAULT_FORMAT_MODE);
    assertEquals(Integer.parseInt(
        LocalOzoneClusterConfig.DEFAULT_DATANODES_VALUE),
        LocalOzoneClusterConfig.DEFAULT_DATANODES);
    assertEquals(Integer.parseInt(LocalOzoneClusterConfig.DEFAULT_PORT_VALUE),
        LocalOzoneClusterConfig.DEFAULT_PORT);
    assertEquals(Boolean.parseBoolean(
        LocalOzoneClusterConfig.DEFAULT_S3G_ENABLED_VALUE),
        LocalOzoneClusterConfig.DEFAULT_S3G_ENABLED);
    assertEquals(Boolean.parseBoolean(
        LocalOzoneClusterConfig.DEFAULT_EPHEMERAL_VALUE),
        LocalOzoneClusterConfig.DEFAULT_EPHEMERAL);
    assertEquals(Duration.parse(
        LocalOzoneClusterConfig.DEFAULT_STARTUP_TIMEOUT_VALUE),
        LocalOzoneClusterConfig.DEFAULT_STARTUP_TIMEOUT);
  }

  @Test
  void builderAcceptsExplicitOverrides() {
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(
            Paths.get("target", "custom-local-ozone"))
        .setFormatMode(LocalOzoneClusterConfig.FormatMode.ALWAYS)
        .setDatanodes(3)
        .setHost("localhost")
        .setBindHost("127.0.0.1")
        .setScmPort(9860)
        .setOmPort(9862)
        .setS3gPort(9878)
        .setS3gEnabled(false)
        .setEphemeral(true)
        .setStartupTimeout(Duration.ofSeconds(45))
        .setS3AccessKey("dev")
        .setS3SecretKey("secret")
        .setS3Region("ap-south-1")
        .build();

    assertEquals(Paths.get("target", "custom-local-ozone")
        .toAbsolutePath().normalize(), config.getDataDir());
    assertEquals(LocalOzoneClusterConfig.FormatMode.ALWAYS,
        config.getFormatMode());
    assertEquals(3, config.getDatanodes());
    assertEquals("localhost", config.getHost());
    assertEquals("127.0.0.1", config.getBindHost());
    assertEquals(9860, config.getScmPort());
    assertEquals(9862, config.getOmPort());
    assertEquals(9878, config.getS3gPort());
    assertFalse(config.isS3gEnabled());
    assertTrue(config.isEphemeral());
    assertEquals(Duration.ofSeconds(45), config.getStartupTimeout());
    assertEquals("dev", config.getS3AccessKey());
    assertEquals("secret", config.getS3SecretKey());
    assertEquals("ap-south-1", config.getS3Region());
  }

  @Test
  void formatModeParsesUserFacingValues() {
    assertEquals(LocalOzoneClusterConfig.FormatMode.IF_NEEDED,
        LocalOzoneClusterConfig.FormatMode.fromString("if-needed"));
    assertEquals(LocalOzoneClusterConfig.FormatMode.ALWAYS,
        LocalOzoneClusterConfig.FormatMode.fromString(" always "));
    assertEquals(LocalOzoneClusterConfig.FormatMode.NEVER,
        LocalOzoneClusterConfig.FormatMode.fromString("NEVER"));
  }

  @Test
  void formatModeRejectsUnknownValues() {
    assertThrows(IllegalArgumentException.class,
        () -> LocalOzoneClusterConfig.FormatMode.fromString("sometimes"));
  }

  @Test
  void formatModeRejectsNullValues() {
    assertThrows(IllegalArgumentException.class,
        () -> LocalOzoneClusterConfig.FormatMode.fromString(null));
  }
}
