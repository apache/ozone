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

import java.util.ServiceLoader;
import java.util.SortedMap;
import java.util.TreeMap;
import picocli.CommandLine;

/**
 * Interface for parent commands that accept subcommands to be dynamically registered.
 * Subcommands should:
 * <li>implement the interface returned by {@link #subcommandType()}</li>
 * <li>be annotated with {@code MetaInfServices} parameterized with the same type</li>
 */
public interface ExtensibleParentCommand {

  /** @return The class of the marker interface for subcommands. */
  Class<?> subcommandType();

  /** Recursively find and add subcommands to {@code cli}. */
  static void addSubcommands(CommandLine cli) {
    Object command = cli.getCommand();

    // find and add subcommands
    if (command instanceof ExtensibleParentCommand) {
      ExtensibleParentCommand parentCommand = (ExtensibleParentCommand) command;
      ServiceLoader<?> subcommands = ServiceLoader.load(parentCommand.subcommandType());
      SortedMap<String, CommandLine> sorted = new TreeMap<>();
      for (Object subcommand : subcommands) {
        final CommandLine.Command commandAnnotation = subcommand.getClass().getAnnotation(CommandLine.Command.class);
        CommandLine subcommandCommandLine = new CommandLine(subcommand, cli.getFactory());
        sorted.put(commandAnnotation.name(), subcommandCommandLine);
      }
      sorted.forEach(cli::addSubcommand);
    }

    // process subcommands recursively
    for (CommandLine subcommand : cli.getSubcommands().values()) {
      addSubcommands(subcommand);
    }
  }

}
