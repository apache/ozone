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

package org.apache.hadoop.ozone.repair.om;

import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.ozone.repair.OzoneRepair;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * Parser for scm.db file.
 */
@CommandLine.Command(
    name = "fso-tree-repair",
    description = "Identify and repair a disconnected FSO tree, and mark " +
        "unreachable entries for deletion. OM should be " +
        "stopped while this tool is run. Information will be logged at " +
        "INFO and DEBUG levels."
)
@MetaInfServices(SubcommandWithParent.class)
public class FSORepairCLI implements Callable<Void>, SubcommandWithParent {

  @CommandLine.ParentCommand
  private OzoneRepair parent;

  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "Path to OM RocksDB")
  private String dbPath;

  @CommandLine.Option(names = {"--dry-run"},
        description = "Mode to run the tool in. Read-mode will just log information about unreachable files or " +
        "directories; otherwise the tool will move those files and directories to the deleted tables.")
  private boolean dryRun;

  @CommandLine.Option(names = {"--volume"},
      description = "Filter by volume name")
  private String volume;

  @CommandLine.Option(names = {"--bucket"},
      description = "Filter by bucket name")
  private String bucket;

  @CommandLine.Option(names = {"--verbose"},
      description = "More verbose output. ")
  private boolean verbose;


  @Override
  public Void call() throws Exception {
    try {
      FSORepairTool
          repairTool = new FSORepairTool(dbPath, dryRun, volume, bucket);
      repairTool.run();
    } catch (Exception ex) {
      throw new IllegalArgumentException("FSO repair failed: " + ex.getMessage());
    }

    if (verbose) {
      System.out.println("FSO repair finished. See client logs for results.");
    }

    return null;
  }

  @Override
  public Class<?> getParentType() {
    return OzoneRepair.class;
  }
}

