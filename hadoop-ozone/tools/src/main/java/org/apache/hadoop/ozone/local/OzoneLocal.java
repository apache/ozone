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

package org.apache.hadoop.ozone.local;

import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import picocli.CommandLine.Command;

/**
 * Internal CLI entry point for local single-node Ozone commands.
 */
@Command(name = "ozone local",
    hidden = true,
    description = "Internal commands for local single-node Ozone",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    subcommands = {
        OzoneLocal.RunCommand.class
    })
public class OzoneLocal extends GenericCli {

  public static void main(String[] args) {
    new OzoneLocal().run(args);
  }

  @Command(name = "run",
      hidden = true,
      description = "Internal placeholder for a local Ozone runtime")
  static class RunCommand implements Callable<Void> {

    @Override
    public Void call() {
      return null;
    }
  }
}
