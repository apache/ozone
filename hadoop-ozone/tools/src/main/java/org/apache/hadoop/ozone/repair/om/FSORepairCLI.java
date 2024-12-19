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

import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * Parser for scm.db file.
 */
@CommandLine.Command(
    name = "fso-tree",
    description = "Identify and repair a disconnected FSO tree by marking unreferenced entries for deletion. " +
        "OM should be stopped while this tool is run."
)
public class FSORepairCLI implements Callable<Void> {

  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "Path to OM RocksDB")
  private String dbPath;

  @CommandLine.Option(names = {"-r", "--repair"},
        defaultValue = "false",
        description = "Run in repair mode to move unreferenced files and directories to deleted tables.")
  private boolean repair;

  @CommandLine.Option(names = {"-v", "--volume"},
      description = "Filter by volume name. Add '/' before the volume name.")
  private String volume;

  @CommandLine.Option(names = {"-b", "--bucket"},
      description = "Filter by bucket name")
  private String bucket;

  @CommandLine.Option(names = {"--verbose"},
      description = "Verbose output. Show all intermediate steps and deleted keys info.")
  private boolean verbose;

  @Override
  public Void call() throws Exception {
    if (repair) {
      System.out.println("FSO Repair Tool is running in repair mode");
    } else {
      System.out.println("FSO Repair Tool is running in debug mode");
    }
    try {
      FSORepairTool
          repairTool = new FSORepairTool(dbPath, repair, volume, bucket, verbose);
      repairTool.run();
    } catch (Exception ex) {
      throw new IllegalArgumentException("FSO repair failed: " + ex.getMessage());
    }

    if (verbose) {
      System.out.println("FSO repair finished.");
    }

    return null;
  }
}
