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

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.admin.ratis.BaseRatisCommand;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.GroupInfoReply;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * Display the information of a specific raft group
 */
@CommandLine.Command(
    name = "info",
    description = "Display the information of a specific raft group",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class InfoCommand extends BaseRatisCommand implements Callable<Void> {

  @CommandLine.ParentCommand
  public GroupCommand parent;

  @CommandLine.Option(names = {"-peers"},
      description = "list of peers",
      required = true)
  private String peers;

  @CommandLine.Option(names = { "-groupid" },
      description = "groupid")
  private String groupid;

  @CommandLine.Option(names = {"-id", "--service-id"},
      description = "OM Service ID",
      required = true)
  private String omServiceId;


  @Override
  public Void call() throws Exception {
    super.run(peers, groupid, omServiceId);
    System.out.println("group id: " + getRaftGroup().getGroupId().getUuid());
    final GroupInfoReply reply = getGroupInfoReply();
    RaftProtos.RaftPeerProto leader = getLeader(reply.getRoleInfoProto());
    if (leader == null) {
      System.out.println("leader not found");
    } else {
      System.out.printf("leader info: %s(%s)%n%n", leader.getId().toStringUtf8(), leader.getAddress());
    }
    System.out.println(reply.getCommitInfos());
    return null;
  }

}
