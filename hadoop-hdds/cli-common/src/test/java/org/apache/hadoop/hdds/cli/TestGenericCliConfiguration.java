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

package org.apache.hadoop.hdds.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Tests for {@link GenericCli} configuration option handling.
 */
public class TestGenericCliConfiguration {

  private static Path deprecatedConf;
  private static Path preferredConf;

  private static final class TestGenericCli extends GenericCli {
  }

  @BeforeAll
  static void setup() throws IOException {
    deprecatedConf = writeConf("deprecated");
    preferredConf = writeConf("preferred");
  }

  @Test
  void confOptionsAreExclusive() {
    CommandLine cmd = new TestGenericCli().getCmd();
    assertThrows(CommandLine.OverwrittenOptionException.class,
        () -> cmd.parseArgs("-conf", deprecatedConf.toString(), "--conf", preferredConf.toString()));
    assertThrows(CommandLine.OverwrittenOptionException.class,
        () -> cmd.parseArgs("--conf", deprecatedConf.toString(), "-conf", preferredConf.toString()));
  }

  @Test
  void deprecatedConfIsUsedWhenNonDeprecatedIsAbsent() {
    TestGenericCli cli = new TestGenericCli();
    cli.getCmd().parseArgs("-conf", deprecatedConf.toString());

    assertThat(cli.getOzoneConf().get("test.key")).isEqualTo("deprecated");
  }

  private static Path writeConf(String value) throws IOException {
    Path conf = Files.createTempFile("ozone-conf-", ".xml");
    Files.write(conf,
        ("<configuration><property><name>test.key</name><value>" + value
            + "</value></property></configuration>").getBytes(StandardCharsets.UTF_8));
    conf.toFile().deleteOnExit();
    return conf;
  }
}
