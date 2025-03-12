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

package org.apache.hadoop.ozone.utils;

import java.util.Objects;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.ratis.util.ReflectionUtils;
import org.reflections.Reflections;
import picocli.AutoComplete;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * Tool to generate bash/zsh auto-complete scripts for Ozone CLI.
 */
public final class AutoCompletion {

  private static final String OZONE_COMMAND = "ozone ";
  private static final int PREFIX_LENGTH = OZONE_COMMAND.length();
  private static final String[] PACKAGES_TO_SCAN = {
      "org.apache.hadoop.hdds",
      "org.apache.hadoop.ozone",
      "org.apache.ozone",
  };

  private AutoCompletion() { }

  public static void main(String[] args) {
    final CommandLine ozone = new CommandLine(new Ozone());
    for (String pkg : PACKAGES_TO_SCAN) {
      new Reflections(pkg).getSubTypesOf(GenericCli.class).stream()
          .map(AutoCompletion::getCommand).filter(Objects::nonNull)
          .filter(cmd -> commandFilter(cmd.getCommandSpec()))
          .forEach(cmd -> ozone.addSubcommand(getCommandName(cmd), cmd));
    }
    System.out.println(AutoComplete.bash("ozone", ozone));
  }

  private static CommandLine getCommand(Class<? extends GenericCli> clazz) {
    CommandLine command = null;
    try {
      command = ReflectionUtils.newInstance(clazz).getCmd();
    } catch (UnsupportedOperationException ignored) {
      // Skip the GenericCli subclasses that do not have no-args constructor.
    }
    return command;
  }

  private static boolean commandFilter(CommandLine.Model.CommandSpec spec) {
    return !spec.usageMessage().hidden() &&
      !CommandLine.Model.CommandSpec.DEFAULT_COMMAND_NAME.equals(spec.name());
  }

  private static String getCommandName(CommandLine cmd) {
    final CommandLine.Model.CommandSpec spec = cmd.getCommandSpec();
    final String qualifiedName = spec.qualifiedName();
    return qualifiedName.startsWith(OZONE_COMMAND) ? qualifiedName.substring(PREFIX_LENGTH) : qualifiedName;
  }

  /**
   * Ozone top level command, used only to generate auto-complete.
   */
  @CommandLine.Command(name = "ozone",
      description = "Ozone top level command")
  private static class Ozone {

    @Option(names = {"--buildpaths"},
        description = "attempt to add class files from build tree")
    private String buildpaths;

    @Option(names = {"--config"},
        description = "Ozone config directory")
    private String config;

    @Option(names = {"--debug"},
        description = "turn on shell script debug mode")
    private String debug;

    @Option(names = {"--daemon"},
        description = "attempt to add class files from build tree")
    private String daemon;

    @Option(names = {"--help"},
        description = "usage information")
    private String help;

    @Option(names = {"--hostnames"},
        description = "hosts to use in worker mode")
    private String hostnames;

    @Option(names = {"--hosts"},
        description = "list of hosts to use in worker mode")
    private String hosts;

    @Option(names = {"--loglevel"},
        description = "set the log4j level for this command")
    private String loglevel;

    @Option(names = {"--workers"},
        description = "turn on worker mode")
    private String workers;

    @Option(names = {"--jvmargs"},
        description = "append JVM options to any existing options defined in the OZONE_OPTS environment variable. " +
            "Any defined in OZONE_CLIENT_OPTS will be appended after these jvmargs")
    private String jvmargs;

    @Option(names = {"--validate"},
        description = "validates if all jars as indicated in the corresponding OZONE_RUN_ARTIFACT_NAME classpath " +
            "file are present, command execution shall continue post validation failure if 'continue' is passed")
    private String validate;
  }
}
