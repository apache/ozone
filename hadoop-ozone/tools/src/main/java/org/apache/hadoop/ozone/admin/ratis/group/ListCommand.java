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

package org.apache.hadoop.ozone.admin.ratis.group;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.admin.ratis.BaseRatisCommand;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.GroupListReply;
import org.apache.ratis.protocol.RaftPeerId;
import picocli.CommandLine;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

import static org.apache.ratis.shell.cli.RaftUtils.getPeerId;
import static org.apache.ratis.shell.cli.RaftUtils.parseInetSocketAddress;
import static org.apache.ratis.shell.cli.RaftUtils.processReply;


/**
 * Display the group information of a specific raft server
 */
@CommandLine.Command(
    name = "list",
    description = "Display the group information of a specific raft server",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ListCommand extends BaseRatisCommand implements Callable<Void> {

  public static final String SERVER_ADDRESS_OPTION_NAME = "serverAddress";
  public static final String PEER_ID_OPTION_NAME = "peerId";


  @picocli.CommandLine.ParentCommand
  public GroupCommand parent;

  @picocli.CommandLine.Option(names = {"-peers"},
      description = "list of peers",
      required = true)
  private String peers;

  @picocli.CommandLine.Option(names = {"-" + PEER_ID_OPTION_NAME},
      description = "Id of peer")
  private String peerIdStr;

  @picocli.CommandLine.Option(names = { "-groupid" },
      description = "groupid")
  private String groupid;

  @picocli.CommandLine.Option(names = { "-" + SERVER_ADDRESS_OPTION_NAME},
      description = "serverAddress")
  private String serverAddress;

  @CommandLine.Option(names = {"-id", "--service-id"},
      description = "OM Service ID",
      required = true)
  private String omServiceId;


  @Override
  public Void call() throws Exception {
    super.run(peers, groupid, omServiceId);
    final RaftPeerId peerId;
    final String address;

    if (StringUtils.isNotEmpty(peerIdStr)) {
      peerId = RaftPeerId.getRaftPeerId(peerIdStr);
      address = getRaftGroup().getPeer(peerId).getAddress();
    } else if (StringUtils.isNotEmpty(serverAddress)) {
      address = serverAddress;
      final InetSocketAddress serverAddress = parseInetSocketAddress(address);
      peerId = RaftUtils.getPeerId(serverAddress);
    } else {
      throw new IllegalArgumentException(
          "Both " + PEER_ID_OPTION_NAME + " and " + SERVER_ADDRESS_OPTION_NAME
              + " options are missing.");
    }

    try(final RaftClient raftClient = buildRaftClient(getRaftGroup())) {
      GroupListReply reply = raftClient.getGroupManagementApi(peerId).list();
      processReply(reply,
          System.out, String.format("Failed to get group information of peerId %s (server %s)", peerId, address));
      System.out.println(String.format("The peerId %s (server %s) is in %d groups, and the groupIds is: %s",
          peerId, address, reply.getGroupIds().size(), reply.getGroupIds()));
    }
    return null;
  }


}
