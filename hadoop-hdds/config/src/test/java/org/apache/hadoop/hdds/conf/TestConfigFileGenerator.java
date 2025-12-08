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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import org.junit.jupiter.api.Test;

/**
 * Test the ConfigFileGenerator.
 * <p>
 * ConfigFileGenerator is an annotation processor and activated for the
 * testCompile. Therefore, in the unit test we can check the content of the
 * generated ozone-default-generated.xml
 */
public class TestConfigFileGenerator {

  @Test
  public void testGeneratedXml() throws FileNotFoundException {
    String generatedXml =
        new Scanner(new File("target/test-classes/ozone-default-generated.xml"),
            StandardCharsets.UTF_8.name())
            .useDelimiter("//Z")
            .next();

    assertThat(generatedXml)
        .as("annotation in ConfigurationExample")
        .contains("<name>ozone.test.config.bind.host</name>");

    assertThat(generatedXml)
        .contains("<tag>MANAGEMENT</tag>");
  }
}
