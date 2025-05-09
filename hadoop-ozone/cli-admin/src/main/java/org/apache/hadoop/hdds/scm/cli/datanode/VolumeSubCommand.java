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
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.VolumeInfoProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetVolumeInfosResponseProto;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.datanode.VolumeInfo;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.shell.ListOptions;
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

  @CommandLine.ArgGroup(multiplicity = "1")
  private OutputFormatOptions formatOptions = new OutputFormatOptions();

  // 用于分页
  @CommandLine.Mixin
  private ListOptions listOptions;

  enum DisplayMode {
    all,
    normal,
    failed
  }

  /**
   * We have designed a new option called 'show',
   * which includes two selectable configurations:
   * 'failed' is used to display failed disks,
   * and 'normal' is used to display normal disks.
   */
  @Option(names = { "--displayMode", "-d"},
      defaultValue = "all",
      description = "The `displayMode` currently offers two display options: " +
      "`normal` for showing healthy disks, and `fail` for displaying failed disks.")
  private DisplayMode displayMode;

  // The UUID identifier of the DataNode.
  @Option(names = { "--uuid" },
      defaultValue = "",
      description = "The `uuid` allows specifying the UUID of a DataNode to " +
      "filter and display only the specified DataNode.")
  private String uuid;

  // The HostName identifier of the DataNode.
  @Option(names = { "--hostname" },
      defaultValue = "",
      description = "The `hostname` allows specifying the hostname of a DataNode to " +
      "filter and display only the specified DataNode.")
  private String hostName;

  static class OutputFormatOptions {

    // Display it in JSON format.
    @Option(names = {"--json"},
        description = "Format output as JSON",
        defaultValue = "false")
    private boolean json;

    // Display it in TABLE format.
    @Option(names = {"--table"},
        description = "Format output as Table",
        defaultValue = "false")
    private boolean table;

    // Custom validation to ensure only one of --json or --table is selected
    public void validate() {
      if (json && table) {
        throw new CommandLine.ParameterException(
            new CommandLine(this), "--json and --table are mutually exclusive.");
      }
    }
  }

  private SimpleDateFormat sdf = new SimpleDateFormat(
      "EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);

  private static final String DATANODE_VOLUME_FAILURES_TITLE = "Datanode Volume";

  private static final List<String>  DATANODE_VOLUME_FAILURES_HEADER = Arrays.asList(
      "UUID", "Host Name", "Volume Name", "Volume Status", "Capacity / Capacity Lost",
      "Failure Time");

  @Override
  public void execute(ScmClient client) throws IOException {

    // Retrieve the volume data based on the conditions.
    GetVolumeInfosResponseProto response =
        client.getVolumeInfos(displayMode.name(), uuid, hostName,
        listOptions.getLimit(), listOptions.getCurrentPage());

    // Print the relevant information if the return value is empty.
    if (response == null || CollectionUtils.isEmpty(response.getVolumeInfosList())) {
      out().println("No volume data was retrieved.");
      out().flush();
      return;
    }

    List<VolumeInfoProto> volumeInfosList = response.getVolumeInfosList();
    List<VolumeInfo> volumeInfos = convertToVolumeInfos(volumeInfosList);

    // If displayed in JSON format.
    if (formatOptions.json) {
      out().print(JsonUtils.toJsonStringWithDefaultPrettyPrinter(volumeInfos));
      out().flush();
      return;
    }

    // If displayed in TABLE format.
    if (formatOptions.table) {
      FormattingCLIUtils formattingCLIUtils = new FormattingCLIUtils(DATANODE_VOLUME_FAILURES_TITLE)
          .addHeaders(DATANODE_VOLUME_FAILURES_HEADER);
      for (VolumeInfo info : volumeInfos) {
        String capacity = StringUtils.byteDesc(info.getCapacity());
        String failureTime = sdf.format(info.getFailureTime());
        String volumeStatus = getVolumeStatus(info.isFailed());
        String[] values = new String[]{info.getUuid(), info.getHostName(), info.getVolumeName(),
            volumeStatus, capacity, failureTime};
        formattingCLIUtils.addLine(values);
      }
      out().println(formattingCLIUtils.render());
      out().flush();
      return;
    }

    out().printf("Datanode Volume (%d Volumes)%n%n", volumeInfos.size());
    volumeInfos.forEach(this::printInfo);
    out().flush();
  }

  /**
   * Print the information of the volume.
   *
   * @param info volume information.
   */
  private void printInfo(VolumeInfo info) {
    out().printf("%-13s: %s %n", "Uuid", info.getUuid());
    out().printf("%-13s: %s %n", "HostName", info.getHostName());
    out().printf("%-13s: %s %n", "Volume Status", getVolumeStatus(info.isFailed()));
    out().printf("%-13s: %s %n", "Volume Name", info.getVolumeName());
    out().printf("%-13s: %s (%s) %n", "Capacity / Capacity Lost", info.getCapacity()
        + " B", StringUtils.byteDesc(info.getCapacity()));
    out().printf("%-13s: %s %n%n", "Failure Time", sdf.format(info.getFailureTime()));
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
