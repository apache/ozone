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

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * In HDDS-5035, we met the case that ozone-default-generated.xml got
 * overwritten in an assemble jar, here we are trying to simulate this case
 * by rename the generated config file.
 */
public class TestGeneratedConfigurationOverwrite {

  private final Path generatedConfigurationPath =
      Paths.get("target/test-classes/hdds-common-default.xml");
  private final Path generatedConfigurationPathBak =
      Paths.get("target/test-classes/hdds-common-default.xml.bak");

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
    assertNotNull(conf.getObject(SimpleConfiguration.class).getBindHost());
    // Check Config Type of Int
    assertNotEquals(0, conf.getObject(SimpleConfiguration.class).getPort());
    // Check Config Type of Time
    assertNotEquals(0, conf.getObject(SimpleConfiguration.class).getWaitTime());
  }
}
