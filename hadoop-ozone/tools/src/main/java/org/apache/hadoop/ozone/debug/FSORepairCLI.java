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

package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * Parser for scm.db file.
 */
@CommandLine.Command(
    name = "fso-repair",
    description = "Identify a disconnected FSO tree, and optionally mark " +
        "unreachable entries for deletion. OM should be " +
        "stopped while this tool is run. Information will be logged at " +
        "INFO and DEBUG levels."
)
@MetaInfServices(SubcommandWithParent.class)
public class FSORepairCLI implements Callable<Void>, SubcommandWithParent {

  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "Path to OM RocksDB")
  private String dbPath;

  @CommandLine.Option(names = {"--read-mode-only", "-r"},
      required = true,
      description =
          "Mode to run the tool in. Read-mode will just log information about unreachable files or directories;" +
              "otherwise the tool will move those files and directories to the deleted tables.",
      defaultValue = "true")
  private boolean readModeOnly;

  @CommandLine.ParentCommand
  private OzoneDebug parent;

  @Override
  public Void call() throws Exception {

    try {
      // TODO case insensitive enum options.
      FSORepairTool repairTool = new FSORepairTool(dbPath, readModeOnly);
      repairTool.run();
    } catch (Exception ex) {
      throw new IllegalArgumentException("FSO repair failed: " + ex.getMessage());
    }

    System.out.printf("FSO %s finished. See client logs for results.%n",
        readModeOnly ? "read-mode" : "repair-mode");

    return null;
  }

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }
}

