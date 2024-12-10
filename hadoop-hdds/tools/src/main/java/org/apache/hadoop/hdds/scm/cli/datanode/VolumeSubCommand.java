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
package org.apache.hadoop.hdds.scm.cli.datanode;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.VolumeInfoProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetVolumeInfosResponseProto;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.datanode.VolumeInfo;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.hdds.utils.FormattingCLIUtils;
import org.apache.hadoop.util.StringUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * We provide a set of volume commands to display the disk information of the DataNode.
 */
@Command(
    name = "volumes",
    description = "Display the list of volumes on the DataNode.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class VolumeSubCommand extends ScmSubcommand {

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

  // PageSize refers to the number of items displayed per page
  // in a paginated view.
  @Option(names = { "--pageSize" },
       defaultValue = "20",
       description = "The number of volume information items displayed per page.")
  private int pageSize;

  // The current page.
  @Option(names = { "--currentPage" },
       defaultValue = "1",
       description = "The current page.")
  private int currentPage;

  /**
   * We have designed a new option called 'show',
   * which includes two selectable configurations:
   * 'failed' is used to display failed disks,
   * and 'normal' is used to display normal disks.
   */
  @Option(names = { "--displayMode" },
      defaultValue = "all",
      description = "failed is used to display failed disks, " +
      "normal is used to display normal disks.")
  private String displayMode;

  // The UUID identifier of the DataNode.
  @Option(names = { "--uuid" },
      defaultValue = "",
      description = "failed is used to display failed disks, " +
      "normal is used to display normal disks.")
  private String uuid;

  // The HostName identifier of the DataNode.
  @Option(names = { "--hostName" },
      defaultValue = "",
      description = "failed is used to display failed disks, " +
      "normal is used to display normal disks.")
  private String hostName;

  private SimpleDateFormat sdf = new SimpleDateFormat(
      "EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);

  private static final String DATANODE_VOLUME_FAILURES_TITLE = "Datanode Volume";

  private static final List<String>  DATANODE_VOLUME_FAILURES_HEADER = Arrays.asList(
      "UUID", "Host Name", "Volume Name", "Volume Status", "Capacity / Capacity Lost",
      "Failure Time");

  private final String[] validModes = {"all", "normal", "failed"};

  @Override
  public void execute(ScmClient client) throws IOException {

    validateDisplayMode(displayMode);

    // Retrieve the volume data based on the conditions.
    GetVolumeInfosResponseProto response =
        client.getVolumeInfos(displayMode, uuid, hostName, pageSize, currentPage);

    // Print the relevant information if the return value is empty.
    if (response == null || CollectionUtils.isEmpty(response.getVolumeInfosList())) {
      System.out.println("No volume data was retrieved.");
      return;
    }

    List<VolumeInfoProto> volumeInfosList = response.getVolumeInfosList();
    List<VolumeInfo> volumeInfos = convertToVolumeInfos(volumeInfosList);

    // If displayed in JSON format.
    if (json) {
      System.out.print(JsonUtils.toJsonStringWithDefaultPrettyPrinter(volumeInfos));
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
        String[] values = new String[]{info.getUuid(), info.getHostName(), info.getVolumeName(),
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
    System.out.printf("%-13s: %s %n", "Uuid", info.getUuid());
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
   * Validate whether the displayMode meets the expected values,
   * where displayMode must be one of the following: all, normal, or failed.
   *
   * @param pDisplayMode
   * The displayMode parameter determines how the volume is displayed.
   *
   * @throws IOException
   * If the parameter is invalid, we will throw an exception.
   */
  private void validateDisplayMode(String pDisplayMode) throws IOException {

    boolean isValid = false;
    for (String validMode : validModes) {
      if (validMode.equals(pDisplayMode)) {
        isValid = true;
        break;
      }
    }

    if (!isValid) {
      throw new IOException("Invalid displayMode. " +
          "It must be one of the following: 'all', 'normal', or 'failed'.");
    }
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
