/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.conf;

import java.io.PrintStream;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import picocli.CommandLine;

/**
 * CLI utility to print out ozone related configuration.
 */
@CommandLine.Command(
    name = "ozone getconf",
    description = "ozone getconf is utility for"
        + " getting configuration information from the config file.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class,
    subcommands = {
        PrintConfKeyCommandHandler.class,
        StorageContainerManagersCommandHandler.class,
        OzoneManagersCommandHandler.class
    })
public class OzoneGetConf extends GenericCli {
  private final PrintStream out; // Stream for printing command output
  private final PrintStream err; // Stream for printing error
  private OzoneConfiguration conf;

  protected OzoneGetConf(OzoneConfiguration conf) {
    this(conf, System.out, System.err);
  }

  protected OzoneGetConf(OzoneConfiguration conf, PrintStream out,
      PrintStream err) {
    this.conf = conf;
    this.out = out;
    this.err = err;
  }

  void printError(String message) {
    err.println(message);
  }

  void printOut(String message) {
    out.println(message);
  }

  OzoneConfiguration getConf() {
    return this.conf;
  }

  public static void main(String[] argv) {
    LogManager.resetConfiguration();
    Logger.getRootLogger().setLevel(Level.INFO);
    Logger.getRootLogger()
        .addAppender(new ConsoleAppender(new PatternLayout("%m%n")));
    Logger.getLogger(NativeCodeLoader.class).setLevel(Level.ERROR);

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.addResource(new OzoneConfiguration());
    new OzoneGetConf(conf).run(argv);
  }
}
