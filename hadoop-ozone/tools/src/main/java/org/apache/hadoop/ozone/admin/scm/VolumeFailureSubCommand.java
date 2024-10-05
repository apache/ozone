/*
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
package org.apache.hadoop.ozone.admin.scm;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.datanode.VolumeFailureInfo;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.utils.FormattingCLIUtils;
import org.apache.hadoop.util.StringUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

@Command(
    name = "volumes failure",
    description = "Display the list of failure volumes on the DataNode.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class VolumeFailureSubCommand extends ScmSubcommand {

  // Display it in JSON format.
  @Option(names = { "--json" },
       defaultValue = "false",
       description = "Format output as JSON")
  private boolean json;

  // Display it in TABLE format.
  @Option(names = { "--table" },
       defaultValue = "false",
       description = "Format output as Table")
  private boolean table;

  private static final SimpleDateFormat sdf = new SimpleDateFormat(
      "EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);

  private static final String DATANODE_VOLUME_FAILURES_TITLE = "Datanode Volume Failures";

  private static final List<String>  DATANODE_VOLUME_FAILURES_HEADER = Arrays.asList(
      "Node", "Volume Name", "Capacity Lost", "Failure Date");

  @Override
  public void execute(ScmClient client) throws IOException {
    List<VolumeFailureInfo> volumeFailureInfos = client.getVolumeFailureInfos();

    if (json) {
      System.out.print(
          JsonUtils.toJsonStringWithDefaultPrettyPrinter(volumeFailureInfos));
      return;
    }

    if(table) {
      FormattingCLIUtils formattingCLIUtils = new FormattingCLIUtils(DATANODE_VOLUME_FAILURES_TITLE)
          .addHeaders(DATANODE_VOLUME_FAILURES_HEADER);
      for (VolumeFailureInfo info : volumeFailureInfos) {
        String capacityLost = StringUtils.byteDesc(info.getCapacityLost());
        String failureDate = sdf.format(info.getFailureDate());
        String[] values = new String[]{info.getNode(), info.getVolumeName(),
            capacityLost, failureDate};
        formattingCLIUtils.addLine(values);
      }
      System.out.println(formattingCLIUtils.render());
      return;
    }

    System.out.printf("Datanode Volume Failures (%d Volumes)%n%n", volumeFailureInfos.size());
    volumeFailureInfos.forEach(this::printInfo);
  }

  private void printInfo(VolumeFailureInfo info) {
    System.out.printf("%-13s: %s %n", "Node", info.getNode());
    System.out.printf("%-13s: %s %n", "Failed Volume", info.getVolumeName());
    System.out.printf("%-13s: %s (%s) %n", "Capacity Lost", info.getCapacityLost()
        + " B", StringUtils.byteDesc(info.getCapacityLost()));
    System.out.printf("%-13s: %s %n%n", "Failure Date", sdf.format(info.getFailureDate()));
  }
}
