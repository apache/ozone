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
import org.apache.hadoop.hdds.scm.cli.pipeline.ListPipelinesSubcommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Tests for deprecated CLI option warnings.
 */
public class TestDeprecatedCliOption {
  private StringWriter err;

  @BeforeEach
  public void setup() {
    err = new StringWriter();
  }

  private CommandLine createCommandLine(Object command) {
    CommandLine cli = new CommandLine(command);
    cli.setErr(new PrintWriter(err, true));
    cli.setExecutionStrategy(parseResult -> {
      DeprecatedCliOption.warnIfMatched(parseResult);
      return CommandLine.ExitCode.OK;
    });
    return cli;
  }

  @Test
  public void warnsForDeprecatedOption() {
    createCommandLine(new ListPipelinesSubcommand())
        .execute("-ffc", "THREE");

    assertThat(err.toString())
        .contains("WARNING: Option '-ffc' is deprecated")
        .contains("--filter-by-factor");
  }

  @Test
  public void warnsForMultipleDeprecatedOptions() {
    createCommandLine(new ListPipelinesSubcommand())
        .execute("-ffc", "THREE", "-fst", "OPEN");

    assertThat(err.toString())
        .contains("WARNING: Option '-ffc' is deprecated")
        .contains("WARNING: Option '-fst' is deprecated");
  }

  @Test
  public void doesNotWarnForLongOption() {
    createCommandLine(new ListPipelinesSubcommand())
        .execute("--filter-by-factor", "THREE");

    assertThat(err.toString()).isEmpty();
  }
}
