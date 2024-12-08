/**
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
package org.apache.hadoop.hdds.cli;

import picocli.CommandLine;

import java.util.ServiceLoader;

/**
 * Defines parent command for SPI based subcommand registration.
 * @deprecated use more specific interfaces
 * @see ExtensibleParentCommand
 */
@Deprecated
public interface SubcommandWithParent {

  static void addSubcommands(CommandLine cli, Class<?> type) {
    ServiceLoader<SubcommandWithParent> registeredSubcommands =
        ServiceLoader.load(SubcommandWithParent.class);
    for (SubcommandWithParent subcommand : registeredSubcommands) {
      if (subcommand.getParentType().equals(type)) {
        final CommandLine.Command commandAnnotation =
            subcommand.getClass().getAnnotation(CommandLine.Command.class);
        CommandLine subcommandCommandLine = new CommandLine(subcommand);
        addSubcommands(subcommandCommandLine, subcommand.getClass());
        cli.addSubcommand(commandAnnotation.name(), subcommandCommandLine);
      }
    }
  }

  /**
   * Java type of the parent command.
   */
  Class<?> getParentType();

}
