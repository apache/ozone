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

package org.apache.hadoop.ozone.repair;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine;

/**
 * Tests the ozone repair command.
 */
public class TestOzoneRepair {

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;
  private static final String DEFAULT_ENCODING = UTF_8.name();

  private static final String OZONE_USER = "ozone";
  private static final String OLD_USER = System.getProperty("user.name");

  @BeforeEach
  public void setup() throws Exception {
    System.setOut(new PrintStream(out, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(err, false, DEFAULT_ENCODING));
    System.setProperty("user.name", OZONE_USER);
  }

  @AfterEach
  public void reset() {
    // reset stream after each unit test
    out.reset();
    err.reset();

    // restore system streams
    System.setOut(OLD_OUT);
    System.setErr(OLD_ERR);
    System.setProperty("user.name", OLD_USER);
  }

  /** All leaf subcommands should support {@code --dry-run},
   * except if marked as {@link ReadOnlyCommand}. */
  @Test
  void subcommandsSupportDryRun() {
    assertSubcommandOptionRecursively(new OzoneRepair().getCmd());
  }

  private static void assertSubcommandOptionRecursively(CommandLine cmd) {
    Map<String, CommandLine> subcommands = cmd.getSubcommands();
    if (subcommands.isEmpty()) {
      // leaf command
      CommandLine.Model.CommandSpec spec = cmd.getCommandSpec();
      Object userObject = spec.userObject();
      if (!(userObject instanceof ReadOnlyCommand)) {
        assertThat(spec.optionsMap().keySet())
            .as(() -> "'" + spec.qualifiedName() + "' defined by " + userObject.getClass()
                + " should support --dry-run or implement " + ReadOnlyCommand.class)
            .contains("--dry-run");
      }
    } else {
      // parent command
      for (CommandLine sub : subcommands.values()) {
        assertSubcommandOptionRecursively(sub);
      }
    }
  }

  @Test
  void testOzoneRepairWhenUserIsRemindedSystemUserAndDeclinesToProceed() throws Exception {
    OzoneRepair ozoneRepair = new OzoneRepair();
    System.setIn(new ByteArrayInputStream("N".getBytes(DEFAULT_ENCODING)));

    int res = ozoneRepair.execute(new String[]{"om", "fso-tree", "--db", "/dev/null"});
    assertThat(res).isNotEqualTo(CommandLine.ExitCode.OK);
    assertThat(err.toString(DEFAULT_ENCODING)).contains("Aborting command.");
    // prompt should contain the current user name as well
    assertThat(err.toString(DEFAULT_ENCODING)).contains("ATTENTION: Running as user " + OZONE_USER);
  }

  @Test
  void testOzoneRepairWhenUserIsRemindedSystemUserAndAgreesToProceed() throws Exception {
    OzoneRepair ozoneRepair = new OzoneRepair();
    System.setIn(new ByteArrayInputStream("y".getBytes(DEFAULT_ENCODING)));

    ozoneRepair.execute(new String[]{"om", "fso-tree", "--db", "/dev/null"});
    assertThat(out.toString(DEFAULT_ENCODING)).contains("Run as user: " + OZONE_USER);
    // prompt should contain the current user name as well
    assertThat(err.toString(DEFAULT_ENCODING)).contains("ATTENTION: Running as user " + OZONE_USER);
  }

  /** Arguments for which confirmation prompt should not be displayed. */
  static List<List<String>> skipPromptParams() {
    return asList(
        emptyList(),
        singletonList("om"),
        asList("om", "fso-tree"),
        asList("om", "fso-tree", "-h"),
        asList("om", "fso-tree", "--help")
    );
  }

  @ParameterizedTest
  @MethodSource("skipPromptParams")
  void testSkipsPrompt(List<String> args) throws Exception {
    OzoneRepair ozoneRepair = new OzoneRepair();

    ozoneRepair.execute(args.toArray(new String[0]));

    assertThat(err.toString(DEFAULT_ENCODING)).doesNotContain("ATTENTION: Running as user " + OZONE_USER);
  }

}
