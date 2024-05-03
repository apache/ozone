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
import org.apache.hadoop.ozone.common.FSOBaseCLI;
import org.apache.hadoop.ozone.repair.OzoneRepair;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

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
public class FSORepairCLI extends FSOBaseCLI {

  @CommandLine.ParentCommand
  private OzoneRepair parent;

  @Override
  public Void call() throws Exception {

    try {
      // TODO case insensitive enum options.
      FSORepairTool
          repairTool = new FSORepairTool(getDbPath(), false);
      repairTool.run();
    } catch (Exception ex) {
      throw new IllegalArgumentException("FSO repair failed: " + ex.getMessage());
    }

    if (getVerbose()) {
      System.out.println("FSO repair finished. See client logs for results.");
    }

    return null;
  }

  @Override
  public Class<?> getParentType() {
    return OzoneRepair.class;
  }
}

