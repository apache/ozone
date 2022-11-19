/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.conf;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * In HDDS-5035, we met the case that ozone-default-generated.xml got
 * overwritten in an assemble jar, here we are trying to simulate this case
 * by rename the generated config file.
 */
public class TestGeneratedConfigurationOverwrite {

  private final Path generatedConfigurationPath =
      Paths.get("target/test-classes/ozone-default-generated.xml");
  private final Path generatedConfigurationPathBak =
      Paths.get("target/test-classes/ozone-default-generated.xml.bak");

  private OzoneConfiguration conf;

  @BeforeEach
  public void overwriteConfigFile() throws Exception {
    Files.move(generatedConfigurationPath, generatedConfigurationPathBak);
    conf = new OzoneConfiguration();
  }

  @AfterEach
  public void recoverConfigFile() throws Exception {
    Files.move(generatedConfigurationPathBak, generatedConfigurationPath);
  }

  @Test
  public void getConfigurationObject() {
    // Check Config Type of String
    Assertions.assertNotNull(
        conf.getObject(SimpleConfiguration.class).getBindHost());
    // Check Config Type of Int
    Assertions.assertNotEquals(
        conf.getObject(SimpleConfiguration.class).getPort(), 0);
    // Check Config Type of Time
    Assertions.assertNotEquals(
        conf.getObject(SimpleConfiguration.class).getWaitTime(), 0);
  }
}