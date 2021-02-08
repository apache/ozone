package org.apache.hadoop.hdds.scm.cli.datanode;

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
    name = "datanodeinfo",
    description = "List disk usage info of a datanode",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DatanodeInfo extends ScmSubcommand {

  @CommandLine.Option(names = {"-ip"}, paramLabel = "IP", description =
      "show info by datanode ip address")
  private String ipaddress;

  @CommandLine.Option(names = {"-uuid"}, paramLabel = "UUID", description =
      "show info by datanode UUID")
  private String uuid;


  @Override
  public void execute(ScmClient scmClient) throws IOException {
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