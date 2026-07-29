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

package org.apache.hadoop.ozone.shell;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.cli.DeprecatedCliOption;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.apache.hadoop.ozone.conf.OzoneGetConf;
import org.apache.hadoop.ozone.debug.OzoneDebug;
import org.apache.hadoop.ozone.freon.Freon;
import org.apache.hadoop.ozone.genconf.GenerateOzoneRequiredConfigurations;
import org.apache.hadoop.ozone.local.OzoneLocal;
import org.apache.hadoop.ozone.repair.OzoneRepair;
import org.apache.hadoop.ozone.shell.s3.S3Shell;
import org.apache.hadoop.ozone.shell.tenant.TenantShell;
import org.apache.hadoop.ozone.utils.AutoCompletion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine;
import picocli.CommandLine.Model.OptionSpec;

/**
 * Tests that newly added CLI options follow the preferred naming style.
 */
class TestCliOptionStyle {

  private static final Pattern CAMEL_CASE = Pattern.compile("--.*[A-Z].*");
  private static final Pattern UNDER_SCORE = Pattern.compile("--.*_.*");

  @ParameterizedTest
  @MethodSource("commands")
  void onlyKnownOptionsUseDeprecatedStyles(CommandLine command) {
    List<String> unknownDeprecatedStyleOptions = new ArrayList<>();

    collectUnknownDeprecatedStyleOptions(command, command.getCommandName(),
        unknownDeprecatedStyleOptions);

    assertThat(unknownDeprecatedStyleOptions)
        .as("Options with deprecated styles should be listed in DeprecatedCliOption")
        .isEmpty();
  }

  private static Stream<CommandLine> commands() {
    return Stream.of(
        new OzoneAdmin().getCmd(),
        new OzoneDebug().getCmd(),
        new OzoneRepair().getCmd(),
        new OzoneShell().getCmd(),
        new S3Shell().getCmd(),
        new TenantShell().getCmd(),
        new Freon().getCmd(),
        new OzoneGetConf().getCmd(),
        new OzoneLocal().getCmd(),
        new OzoneRatis().getCmd(),
        new GenerateOzoneRequiredConfigurations().getCmd(),
        new AutoCompletion().getCmd()
    );
  }

  private static void collectUnknownDeprecatedStyleOptions(CommandLine command,
      String path, List<String> collector) {
    for (OptionSpec option : command.getCommandSpec().options()) {
      for (String name : option.names()) {
        if (hasDeprecatedStyle(name) && !isKnownDeprecatedOption(name)) {
          collector.add(path + ": " + name);
        }
      }
    }

    for (Map.Entry<String, CommandLine> subcommand :
        command.getSubcommands().entrySet()) {
      collectUnknownDeprecatedStyleOptions(subcommand.getValue(),
          path + " " + subcommand.getKey(), collector);
    }
  }

  private static boolean hasDeprecatedStyle(String option) {
    if (option.startsWith("--")) {
      return CAMEL_CASE.matcher(option).matches()
          || UNDER_SCORE.matcher(option).matches();
    }
    return option.startsWith("-") && option.length() > 2;
  }

  private static boolean isKnownDeprecatedOption(String option) {
    StringWriter err = new StringWriter();
    String replacement = DeprecatedCliOption.toNonDeprecated(option,
        new PrintWriter(err, true));
    return !replacement.equals(option);
  }
}
