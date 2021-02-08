package org.apache.hadoop.hdds.scm.cli.datanode;

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.IOException;

/**
 * Command to list the disk usage related information of datanodes.
 */
@Command(
    name = "datanodediskinfo",
    description = "List disk usage info of a datanode",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DatanodeDiskInfo extends ScmSubcommand {

  @CommandLine.Option(names = {"-ip"}, paramLabel = "IP", description =
      "show info by datanode ip address")
  private String ipaddress;

  @CommandLine.Option(names = {"-uuid"}, paramLabel = "UUID", description =
      "show info by datanode UUID")
  private String uuid;


  @Override
  public void execute(ScmClient scmClient) throws IOException {
    if (Strings.isNullOrEmpty(ipaddress)) {
      ipaddress = null;
    }
    if (Strings.isNullOrEmpty(uuid)) {
      uuid = null;
    }
    if (Strings.isNullOrEmpty(ipaddress) && Strings.isNullOrEmpty(uuid)) {
      throw new IOException("ipaddress or uuid of the datanode must be " +
          "specified.");
    }
    HddsProtos.DatanodeDiskInfo info =
        scmClient.getDatanodeDiskInfo(ipaddress, uuid);
  }

}
/*
get datanodedetails of the id or uuid provided
SCMClientProtocolServer.queryNodeState(opState, state) requires these params
only
ScmNodeMetric.getNodeStat
SCMNodeStat
 */