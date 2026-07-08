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

import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

/**
 * Tests for deprecated CLI option warnings of @{code ozone admin}.
 */
class TestOzoneAdminDeprecatedOptions {

  private CommandLine cli;
  private StringWriter err;

  @BeforeEach
  public void setup() {
    err = new StringWriter();
    cli = createCommandLine();
  }

  private CommandLine createCommandLine() {
    OzoneAdmin command = new OzoneAdmin();
    CommandLine cmd = command.getCmd();
    cmd.setErr(new PrintWriter(err, true));
    cmd.setExecutionStrategy(parseResult -> CommandLine.ExitCode.OK);
    return cmd;
  }

  @ParameterizedTest
  @ValueSource(strings = {"-ffc THREE", "-ffc=ONE"})
  public void warnsForDeprecatedOption(String arg) {
    execute("pipeline list " + arg);

    assertThat(err.toString())
        .contains("WARNING: Option '-ffc' is deprecated")
        .contains("--filter-by-factor");
  }

  @Test
  public void warnsForMultipleDeprecatedOptions() {
    execute("pipeline list -ffc THREE -fst OPEN");

    assertThat(err.toString())
        .contains("WARNING: Option '-ffc' is deprecated")
        .contains("WARNING: Option '-fst' is deprecated");
  }

  @ParameterizedTest
  @ValueSource(strings = {"--filter-by-factor=THREE", "--filter-by-factor ONE"})
  public void doesNotWarnForLongOption(String arg) {
    execute("pipeline list " + arg);

    assertThat(err.toString()).isEmpty();
  }

  private void execute(String cmd) {
    cli.execute(cmd.split(" "));
  }
}
