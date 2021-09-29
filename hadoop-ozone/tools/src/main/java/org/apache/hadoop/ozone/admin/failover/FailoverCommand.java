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
package org.apache.hadoop.ozone.admin.failover;

import org.apache.commons.lang3.EnumUtils;
import org.apache.hadoop.hdds.NodeDetails;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.ha.OMHANodeDetails;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.api.GroupManagementApi;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.*;
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.util.TimeDuration;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.*;

/**
 * Subcommand for admin operations related to failover actions.
 */
@CommandLine.Command(
    name = "failover",
    description = "manually transfer leadership of raft group to target node",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
@MetaInfServices(SubcommandWithParent.class)
public class FailoverCommand extends GenericCli
    implements SubcommandWithParent {

  public static final RaftGroupId PSEUDO_RAFT_GROUP_ID = RaftGroupId.randomId();
  public static final int TRANSFER_LEADER_WAIT_MS = 120_000;
  public static final String RANDOM = "RANDOM";

  enum Domain {
    OM,
    SCM,
  }

  @CommandLine.ParentCommand
  private OzoneAdmin parent;

  @CommandLine.Parameters(
      description = "['om'/'scm']"
  )
  private String domain;

  @CommandLine.Parameters(
      description = "[host/host:port/'random'] ratis rpc address of " +
      "target domain if 'random' then choose arbitrary follower node "
  )
  private String tgtAddress;

  @CommandLine.Option(
      names = {"-id", "--service-id"},
      description = "Ozone Manager Service ID, if domain is om, " +
          "this option is prerequisite"
  )
  private String omServiceId;

  @CommandLine.Option(
          names = {"--hostPortList"},
          description = "if need to test on given nodes, " +
                  "pattern like this 'host:port,host:port,host:port'"
  )
  private String ratisAddresses;

  public OzoneAdmin getParent() {
    return parent;
  }

  @Override
  public Void call() throws Exception {
    OzoneConfiguration configuration;
    if (parent.getOzoneConf() == null) {
      configuration = new OzoneConfiguration();
    } else{
      configuration = parent.getOzoneConf();
    }
    try {
      transferLeadership(configuration);
    } catch (Exception ex) {
      ex.printStackTrace();
      throw ex;
    }
    return null;
  }

  @Override
  public Class<?> getParentType() {
    return OzoneAdmin.class;
  }

  /**
   * Create ratis client raft client.
   *
   * @param raftGroupId the raft group id
   * @param peers       the peers
   * @return the raft client
   */
  public static RaftClient createRatisClient(RaftGroupId raftGroupId,
                                             Collection<RaftPeer> peers) {
    RaftProperties properties = new RaftProperties();
    Parameters parameters = new Parameters();
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties,
        TimeDuration.valueOf(15, TimeUnit.SECONDS));
    ExponentialBackoffRetry retryPolicy = ExponentialBackoffRetry.newBuilder()
        .setBaseSleepTime(
                TimeDuration.valueOf(100, TimeUnit.MILLISECONDS))
        .setMaxAttempts(10)
        .setMaxSleepTime(
                TimeDuration.valueOf(100000, TimeUnit.MILLISECONDS))
        .build();
    return RaftClient.newBuilder()
        .setClientId(ClientId.randomId())
        .setLeaderId(null)
        .setProperties(properties)
        .setParameters(parameters)
        .setRetryPolicy(retryPolicy)
        .setRaftGroup(RaftGroup.valueOf(raftGroupId, peers))
        .build();
  }

  /**
   * The whole procedure is as following.
   *
   *   Pseudo raft client -> get real raft group info -> check address
   *   whether in raft peers -> real raft client -> set priority -> trigger
   *   transferleadership -> new raft leader takes office
   *
   * @throws IOException IOException
   */
  private void transferLeadership(OzoneConfiguration conf) throws IOException {
    List<String> ratisAddressList;
    if (ratisAddresses != null) {
      ratisAddressList = Arrays.asList(ratisAddresses.split(","));
    } else if (domain.equalsIgnoreCase(Domain.SCM.toString())) {
      ratisAddressList = getSCMRatisAddressList(conf);
    } else if (domain.equalsIgnoreCase(Domain.OM.toString())) {
      ratisAddressList = getOMRatisAddressList(conf);
    } else {
      throw new IllegalArgumentException("Invalid domain, should be one of " +
              Arrays.toString(Domain.values()));
    }
    assert ratisAddressList.size() > 0;
    List<RaftPeer> peerList = ratisAddressList.stream().map(addr-> RaftPeer
            .newBuilder()
            .setId(RaftPeerId.valueOf(addr))
            .setAddress(addr)
            .build())
            .collect(Collectors.toList());

    // Pseudo client for inquiry
    RaftClient raftClient = createRatisClient(PSEUDO_RAFT_GROUP_ID, peerList);
    RaftGroupId remoteGroupId;
    GroupManagementApi groupManagementApi = raftClient.getGroupManagementApi(
            peerList.get(0).getId());
    List<RaftGroupId> groupIds = groupManagementApi.list().getGroupIds();
    if (groupIds.size() == 1) {
      remoteGroupId = groupIds.get(0);
    } else {
      throw new IOException("There are more than one raft group.");
    }
    GroupInfoReply groupInfoReply = groupManagementApi.info(remoteGroupId);
    RaftGroup raftGroup = groupInfoReply.getGroup();
    raftClient = createRatisClient(raftGroup.getGroupId(),
            raftGroup.getPeers());
    System.out.println("RaftGroup from raft server: " + raftGroup);

    if (tgtAddress.equalsIgnoreCase(RANDOM)) {
      tgtAddress = getRandomAddress(groupInfoReply);
    }
    getRatisPortHost(conf);
    // check address passed whether belongs to the peers
    if (raftGroup.getPeers().stream().noneMatch(raftPeer ->
        raftPeer.getAddress().contains(tgtAddress))) {
      throw new IOException(String.format("%s is not part of the " +
          "quorum %s.", tgtAddress, raftGroup.getPeers().stream().
              map(RaftPeer::getAddress).collect(Collectors.toList())));
    }
    System.out.printf("Trying to transfer to new leader %s", tgtAddress);

    List<RaftPeer> peersWithNewPriorities = new ArrayList<>();
    for (RaftPeer peer : raftGroup.getPeers()) {
      peersWithNewPriorities.add(
          RaftPeer.newBuilder(peer)
                  .setPriority(peer.getAddress()
                          .equalsIgnoreCase(tgtAddress) ? 2 : 1)
                  .build()
      );
    }
    RaftClientReply reply;
    reply = raftClient.admin().setConfiguration(peersWithNewPriorities);
    if (reply.isSuccess()) {
      System.out.printf("Successfully set new priority for division: %s.%n",
          peersWithNewPriorities);
    } else {
      System.out.printf("Failed to set new priority for division: %s." +
          " Ratis reply: %s%n", peersWithNewPriorities, reply);
      throw new IOException(reply.getException());
    }

    RaftPeerId newLeaderPeerId = raftGroup.getPeers().stream().
        filter(peer -> peer.getAddress().equalsIgnoreCase(tgtAddress))
            .findAny().get().getId();
    reply = raftClient.admin().transferLeadership(
        newLeaderPeerId, TRANSFER_LEADER_WAIT_MS);
    if (reply.isSuccess()) {
      System.out.printf("Successfully transferred leadership: %s.%n",
              tgtAddress);
    } else {
      System.out.printf("Failed to transferring leadership: %s." +
          " Ratis reply: %s%n", tgtAddress, reply);
      throw new IOException(reply.getException());
    }
  }

  private List<String> getOMRatisAddressList(OzoneConfiguration conf)
          throws IOException {
    if (OmUtils.isOmHAServiceId(conf, omServiceId)) {
      OMHANodeDetails omhaNodeDetails = OMHANodeDetails.loadOMHAConfig(conf);
      assert omhaNodeDetails != null;
      return omhaNodeDetails.getPeerNodesMap().values().stream()
              .map(NodeDetails::getRatisHostPortStr)
              .collect(Collectors.toList());
    } else {
      throw new IOException("OM HA not enabled or" +
              " OM ServiceId not set in option.");
    }
  }

  /**
   * Check SCM HA and get ratis address list.
   *
   * @param conf ozoneConfiguration
   * @return SCMRatisAddressList
   * @throws IOException IOException
   */
  List<String> getSCMRatisAddressList(OzoneConfiguration conf)
          throws IOException {
    List<String> addressList = new ArrayList<>();
    String scmServiceId = SCMHAUtils.getScmServiceId(conf);
    String scmRatisPort = conf.get(OZONE_SCM_RATIS_PORT_KEY,
            String.valueOf(OZONE_SCM_RATIS_PORT_DEFAULT));

    if(!SCMHAUtils.isSCMHAEnabled(conf) || scmServiceId == null) {
      throw new IOException("SCM HA not enabled or cannot find SCM ServiceId.");
    }
    ArrayList< String > scmNodeIds = new ArrayList<>(
            SCMHAUtils.getSCMNodeIds(conf, scmServiceId));
    if (scmNodeIds.size() == 0) {
      throw new ConfigurationException(
              String.format("Configuration does not have any value set for " +
                      "%s for the SCM serviceId %s. List of SCM Node ID's " +
                      "should be specified for an SCM HA service",
                      OZONE_SCM_NODES_KEY, scmServiceId));
    }

    for (String scmNodeId : scmNodeIds) {
      String addressKey = ConfUtils.addKeySuffixes(
              OZONE_SCM_ADDRESS_KEY, scmServiceId, scmNodeId);
      String scmAddress = conf.get(addressKey);
      if (scmAddress == null) {
        throw new ConfigurationException(addressKey + "is not defined");
      }
      addressList.add(scmAddress.concat(":").concat(scmRatisPort));
    }
    return addressList;
  }

  /**
   * adjust the address to Host:Port pattern.
   *
   * @param conf the conf
   * @return the ratis port host
   */
  public String getRatisPortHost(OzoneConfiguration conf) {
    assert EnumUtils.isValidEnum(Domain.class, domain.toUpperCase());
    int ratisPort;
    if (domain.equalsIgnoreCase(Domain.SCM.name())) {
      ratisPort = conf.getInt(
          ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY,
          ScmConfigKeys.OZONE_SCM_RATIS_PORT_DEFAULT);
    } else {
      ratisPort = conf.getInt(
          OMConfigKeys.OZONE_OM_RATIS_PORT_KEY,
          OMConfigKeys.OZONE_OM_RATIS_PORT_DEFAULT);
    }

    if (!tgtAddress.contains(":")) {
      tgtAddress = tgtAddress.concat(":").concat(
          String.valueOf(ratisPort));
    }
    return tgtAddress;
  }

  /**
   * Gets random address.
   *
   * @param groupInfoReply the group info reply
   * @return the random address
   * @throws IOException the io exception
   */
  public static String getRandomAddress(GroupInfoReply groupInfoReply)
          throws IOException {
    String targetAddress;
    String leaderAddr;
    if (groupInfoReply.getRoleInfoProto().getRole()
            .equals(RaftProtos.RaftPeerRole.LEADER)) {
      leaderAddr = groupInfoReply.getRoleInfoProto().getSelf().getAddress();
    } else {
      leaderAddr = groupInfoReply.getGroup().getPeers().stream()
              .filter(raftPeer -> groupInfoReply.getRoleInfoProto()
                      .getFollowerInfo().getLeaderInfo().getId()
                      .getId().toStringUtf8()
                      .equalsIgnoreCase(raftPeer.getId().toString()))
              .findAny().get().getAddress();
    }
    List<String> candidateAddressList = groupInfoReply.getGroup()
            .getPeers().stream().filter(raftPeer -> !raftPeer.getAddress()
            .equalsIgnoreCase(leaderAddr)).map(RaftPeer::getAddress)
            .collect(Collectors.toList());
    if (candidateAddressList.size() > 0) {
      Collections.shuffle(candidateAddressList);
      targetAddress = candidateAddressList.get(0);
      System.out.println("candidate list: " + candidateAddressList);
      System.out.println("randomly chosen address: " + targetAddress);
      return targetAddress;
    } else {
      throw new IOException("Not enough Peers for transferring leadership");
    }
  }
}
