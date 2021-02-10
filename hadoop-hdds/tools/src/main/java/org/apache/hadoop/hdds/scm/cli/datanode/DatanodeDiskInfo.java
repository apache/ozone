package org.apache.hadoop.hdds.scm.cli.datanode;

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.IOException;
import java.text.NumberFormat;

/**
 * Command to list the disk usage related information of datanodes.
 */
@Command(
    name = "datanodediskinfo",
    description = "List disk usage " +
        "information (such as Capacity, SCMUsed, Remaining) of a datanode",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DatanodeDiskInfo extends ScmSubcommand {

  @CommandLine.Option(names = {"-ip"}, paramLabel = "IP", description =
      "Show info by datanode ip address")
  private String ipaddress;

  @CommandLine.Option(names = {"-uuid"}, paramLabel = "UUID", description =
      "Show info by datanode UUID")
  private String uuid;

  public String getIpaddress() {
    return ipaddress;
  }

  public void setIpaddress(String ipaddress) {
    this.ipaddress = ipaddress;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    if (Strings.isNullOrEmpty(ipaddress)) {
      ipaddress = "";
    }
    if (Strings.isNullOrEmpty(uuid)) {
      uuid = "";
    }
    if (Strings.isNullOrEmpty(ipaddress) && Strings.isNullOrEmpty(uuid)) {
      throw new IOException("ipaddress or uuid of the datanode must be " +
          "specified.");
    }

    HddsProtos.DatanodeDiskInfo info =
        scmClient.getDatanodeDiskInfo(ipaddress, uuid);
    Double capacity = Double.parseDouble(info.getCapacity());
    Double used = Double.parseDouble(info.getUsed());
    Double usedPercentage = used / capacity;
    NumberFormat percentFormat = NumberFormat.getPercentInstance();
    percentFormat.setMinimumFractionDigits(5);

    System.out.printf("%-10s: %20sB %n", "Capacity", info.getCapacity());
    System.out.printf("%-10s: %20sB (%s) %n", "SCMUsed", info.getUsed(),
        percentFormat.format(usedPercentage));
    System.out.printf("%-10s: %20sB (%s) %n",
        "Remaining", info.getRemaining(),
        percentFormat.format(1d - usedPercentage));
  }
}
