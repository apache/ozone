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

package org.apache.hadoop.ozone.insight;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OmConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test insight report which prints out configs.
 */
public class TestConfigurationSubCommand {

  private static final PrintStream OLD_OUT = System.out;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();

  @BeforeEach
  public void setup() throws Exception {
    System.setOut(new PrintStream(out, false, StandardCharsets.UTF_8.name()));
  }

  @AfterEach
  public void reset() {
    System.setOut(OLD_OUT);
  }

  @Test
  public void testPrintConfig() throws UnsupportedEncodingException {
    OzoneConfiguration conf = new OzoneConfiguration();
    long customValue = OmConfig.Defaults.SERVER_LIST_MAX_SIZE + 50;
    conf.setLong(OmConfig.Keys.SERVER_LIST_MAX_SIZE, customValue);
    ConfigurationSubCommand subCommand = new ConfigurationSubCommand();

    subCommand.printConfig(OmConfig.class, conf);

    final String output = out.toString(StandardCharsets.UTF_8.name());
    assertThat(output).contains(">>> " + OmConfig.Keys.SERVER_LIST_MAX_SIZE);
    assertThat(output).contains("default: " + OmConfig.Defaults.SERVER_LIST_MAX_SIZE);
    assertThat(output).contains("current: " + customValue);
    assertThat(output).contains(">>> " + OmConfig.Keys.ENABLE_FILESYSTEM_PATHS);
    assertThat(output).contains("default: " + OmConfig.Defaults.ENABLE_FILESYSTEM_PATHS);
    assertThat(output).contains("current: " + OmConfig.Defaults.ENABLE_FILESYSTEM_PATHS);
  }
}
