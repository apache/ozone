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

package org.apache.hadoop.hdds.scm.cli.datanode;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.VolumeInfo;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.VolumeInfoProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetVolumeInfosResponseProto;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.shell.ListPaginationOptions;
import org.apache.hadoop.ozone.utils.FormattingCLIUtils;
import org.apache.hadoop.util.StringUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * We provide a set of volume commands to display the disk information of the DataNode.
 */
@Command(
    name = "volumes",
    description = "Display the list of volumes on the DataNode.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class VolumeSubCommand extends ScmSubcommand {

  private static final String DATANODE_VOLUME_FAILURES_TITLE = "Datanode Volume";
  private static final List<String> DATANODE_VOLUME_FAILURES_HEADER = Arrays.asList(
      "UUID", "Host Name", "Volume Name", "Volume Status", "Capacity / Capacity Lost", "Failure Time");

  private SimpleDateFormat sdf = new SimpleDateFormat(
      "EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);

  /**
   * We have designed a new option called 'show',
   * which includes two selectable configurations:
   * 'failed' is used to display failed disks,
   * and 'healthy' is used to display normal disks.
   */
  @Option(names = { "--state" },
      defaultValue = "ALL",
      description = "Filter disks by state: 'failed' shows failed disks, " +
      "'healthy' shows healthy disks, 'all' shows all disks.")
  private State state;

  // The UUID identifier of the DataNode.
  @Option(names = { "--uuid" },
      defaultValue = "",
      description = "Filter disks by the UUID of the DataNode.")
  private String uuid;

  // The hostname identifier of the DataNode.
  @Option(names = { "--hostname" },
      defaultValue = "",
      description = "Filter disks by the host name of the DataNode.")
  private String hostName;

  // Display it in JSON format.
  @Option(names = { "--json" },
       defaultValue = "false",
       description = "Format output as JSON.")
  private boolean json;

  // Display it in TABLE format.
  @Option(names = { "--table" },
       defaultValue = "false",
       description = "Format output as Table.")
  private boolean table;

  // Mixin command-line pagination options to support paginated query functionality
  @CommandLine.Mixin
  private ListPaginationOptions listOptions;

  enum State { ALL, HEALTHY, FAILED }

  @Override
  public void execute(ScmClient client) throws IOException {

    // Retrieve the volume data based on the conditions.
    // Normally, pageSize is derived from listOptions.getLimit().
    // However, if the user requests to display all results,
    // pageSize is set to -1 to indicate no pagination.
    int pageSize = listOptions.getLimit();
    if (listOptions.isAll()) {
      pageSize = -1;
    }
    GetVolumeInfosResponseProto response =
        client.getVolumeInfos(state.name(), uuid, hostName, pageSize, listOptions.getStartItem());

    List<VolumeInfoProto> volumeInfosList = response.getVolumeInfosList();
    List<VolumeInfo> volumeInfos = convertToVolumeInfos(volumeInfosList);

    // If displayed in JSON format.
    if (json) {
      System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(volumeInfos));
      return;
    }

    // If displayed in TABLE format.
    if (table) {
      FormattingCLIUtils formattingCLIUtils = new FormattingCLIUtils(DATANODE_VOLUME_FAILURES_TITLE)
          .addHeaders(DATANODE_VOLUME_FAILURES_HEADER);
      for (VolumeInfo info : volumeInfos) {
        String capacity = StringUtils.byteDesc(info.getCapacity());
        String failureTime = sdf.format(info.getFailureTime());
        String volumeStatus = getVolumeStatus(info.isFailed());
        String[] values = new String[]{info.getDatanodeID().getID(), info.getHostName(), info.getVolumeName(),
            volumeStatus, capacity, failureTime};
        formattingCLIUtils.addLine(values);
      }
      System.out.println(formattingCLIUtils.render());
      return;
    }

    System.out.printf("Datanode Volume (%d Volumes)%n%n", volumeInfos.size());
    volumeInfos.forEach(this::printInfo);
  }

  /**
   * Print the information of the volume.
   *
   * @param info volume information.
   */
  private void printInfo(VolumeInfo info) {
    System.out.printf("%-13s: %s %n", "Uuid", info.getDatanodeID().getID());
    System.out.printf("%-13s: %s %n", "HostName", info.getHostName());
    System.out.printf("%-13s: %s %n", "Volume Status", getVolumeStatus(info.isFailed()));
    System.out.printf("%-13s: %s %n", "Volume Name", info.getVolumeName());
    System.out.printf("%-13s: %s (%s) %n", "Capacity / Capacity Lost", info.getCapacity()
        + " B", StringUtils.byteDesc(info.getCapacity()));
    System.out.printf("%-13s: %s %n%n", "Failure Time", sdf.format(info.getFailureTime()));
  }

  /**
   * Convert the Protobuf object to a displayable object.
   *
   * @param volumeInfosList The list of volume information.
   * @return volume list.
   */
  private List<VolumeInfo> convertToVolumeInfos(List<VolumeInfoProto> volumeInfosList) {
    List<VolumeInfo> volumeInfoList = new ArrayList<>();
    for (VolumeInfoProto volumeInfoProto : volumeInfosList) {
      VolumeInfo volumeInfo = VolumeInfo.fromProtobuf(volumeInfoProto);
      volumeInfoList.add(volumeInfo);
    }
    return volumeInfoList;
  }

  /**
   * Retrieve the display information of the disk.
   *
   * @param failed If it is true, it will display as FAILED;
   * if it is false, it will display as Normal.
   *
   * @return Volume Status.
   */
  private String getVolumeStatus(boolean failed) {
    return !failed ? "NORMAL" : "FAILED";
  }
}
