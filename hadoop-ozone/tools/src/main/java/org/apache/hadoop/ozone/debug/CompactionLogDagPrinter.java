/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.io.IOException;

/**
 * Handler to generate image for current compaction DAG in the OM.
 * ozone sh snapshot print-log-dag.
 */
@CommandLine.Command(
    name = "print-log-dag",
    aliases = "pld",
    description = "List snapshots for the buckets.")
@MetaInfServices(SubcommandWithParent.class)
public class CompactionLogDagPrinter extends Handler
    implements SubcommandWithParent {

  @CommandLine.Option(names = {"-f", "--file-name"},
      description = "Name of the image file. (optional)")
  private String fileName;

  @CommandLine.Option(names = {"-t", "--graph-type"},
      description = "Type of node name to use in the graph image.\n" +
          "Accepted values are: \n" +
          "  file_name: to use file name as node name in DAG,\n" +
          "  key_size: to show the no. of keys in the file along with file " +
          "name in the DAG node name,\n" +
          "  cumulative_size: to show the cumulative size along with file " +
          "name in the DAG node name.",
      defaultValue = "file_name")
  private String graphType;

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException, OzoneClientException {
    String imagePath = client.getObjectStore()
        .printCompactionLogDag(fileName, graphType);
    System.out.println("DAG image is created on path: " + imagePath);
  }
}
