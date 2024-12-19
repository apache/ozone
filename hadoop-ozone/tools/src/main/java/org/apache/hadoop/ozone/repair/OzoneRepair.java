/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.repair;

import org.apache.hadoop.hdds.cli.ExtensibleParentCommand;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.RepairSubcommand;
import picocli.CommandLine;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * Ozone Repair Command line tool.
 */
@CommandLine.Command(name = "ozone repair",
    description = "Operational tool to repair Ozone",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class OzoneRepair extends GenericCli implements ExtensibleParentCommand {

  public static final String WARNING_SYS_USER_MESSAGE =
      "ATTENTION: Running as user %s. Make sure this is the same user used to run the Ozone process." +
          " Are you sure you want to continue (y/N)? ";

  public static void main(String[] argv) {
    new OzoneRepair().run(argv);
  }

  @Override
  public int execute(String[] argv) {
    String currentUser = getSystemUserName();
    if (!("y".equalsIgnoreCase(getConsoleReadLineWithFormat(currentUser)))) {
      System.out.println("Aborting command.");
      return 1;
    }
    System.out.println("Run as user: " + currentUser);

    return super.execute(argv);
  }

  public  String getSystemUserName() {
    return System.getProperty("user.name");
  }

  public  String getConsoleReadLineWithFormat(String currentUser) {
    System.err.printf(WARNING_SYS_USER_MESSAGE, currentUser);
    return (new Scanner(System.in, StandardCharsets.UTF_8.name())).nextLine().trim();
  }

  @Override
  public Class<?> subcommandType() {
    return RepairSubcommand.class;
  }
}
