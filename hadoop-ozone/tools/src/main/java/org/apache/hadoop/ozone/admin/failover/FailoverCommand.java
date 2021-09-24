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
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.QuorumInfo;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.util.TimeDuration;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

/**
 * Subcommand for admin operations related to SCM.
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

  private final ClientId clientId = ClientId.randomId();

  private static final int TRANSFER_LEADER_WAIT_MS = 120_000;
  private static final String RANDOM = "RANDOM";

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
  private String address;

  @CommandLine.Option(
      names = {"-id", "--service-id"},
      description = "Ozone Manager Service ID, if domain is om, " +
          "this option is prerequisite"
  )
  private String omServiceId;

  public OzoneAdmin getParent() {
    return parent;
  }

  @Override
  public Void call() throws Exception {
    transferLeadership();
    return null;
  }

  @Override
  public Class<?> getParentType() {
    return OzoneAdmin.class;
  }

  public RaftClient createRatisClient(QuorumInfo quorumInfo) {
    Objects.requireNonNull(quorumInfo);
    RaftProperties properties = new RaftProperties();
    Parameters parameters = new Parameters();
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties,
        TimeDuration.valueOf(15, TimeUnit.SECONDS));
    ExponentialBackoffRetry retryPolicy = ExponentialBackoffRetry.newBuilder()
        .setBaseSleepTime(TimeDuration.valueOf(100, TimeUnit.MILLISECONDS))
        .setMaxAttempts(10)
        .setMaxSleepTime(
            TimeDuration.valueOf(100000, TimeUnit.MILLISECONDS))
        .build();
    return RaftClient.newBuilder()
        .setClientId(clientId)
        .setLeaderId(null)
        .setProperties(properties)
        .setParameters(parameters)
        .setRetryPolicy(retryPolicy)
        .setRaftGroup(RaftGroup.valueOf(quorumInfo.getRaftGroupId(),
            quorumInfo.getPeers()))
        .build();
  }

  /**
   * the whole procedure is as following.
   * Phase1:
   *   server client -> get quorumInfo from server -> check address whether
   *   in quorum
   *
   * Phase2:
   *   raft client -> set priority -> trigger transferLeadership ->
   *   old leader step down -> new leader take office
   *
   *
   * @throws IOException IOException
   * @throws OzoneClientException OzoneClientException
   */
  private void transferLeadership() throws IOException, OzoneClientException {
    OzoneConfiguration conf = parent.createOzoneConfiguration();

    QuorumInfo quorumInfo;
    // get quorumInfo through different clients
    if (domain.equalsIgnoreCase(Domain.SCM.toString())) {
      if (SCMHAUtils.isSCMHAEnabled(conf) &&
          SCMHAUtils.getSCMNodeIds(conf).toArray().length > 0){
        ContainerOperationClient scmClient = new ContainerOperationClient(conf);
        quorumInfo = scmClient.getQuorumInfo();
      } else {
        throw new OzoneClientException("This command works only on " +
            "SCM HA cluster. " + OZONE_SCM_HA_ENABLE_KEY + " not true or SCM " +
            "nodes not found");
      }
    } else if (domain.equalsIgnoreCase(Domain.OM.toString())) {
      if (OmUtils.isOmHAServiceId(conf, omServiceId)) {
        ClientProtocol omClient =  OzoneClientFactory.
            getRpcClient(omServiceId, conf).getObjectStore().getClientProxy();
        quorumInfo = omClient.getQuorumInfo();
      } else {
        throw new OzoneClientException("This command works only on " +
            "OzoneManager HA cluster. Service ID specified: " + omServiceId +
            " does not match with " + OZONE_OM_SERVICE_IDS_KEY + " defined " +
            "in the configuration. Configured " + OZONE_OM_SERVICE_IDS_KEY +
            " are " + conf.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY)
            + "\n");
      }
    } else {
      throw new IllegalArgumentException("Invalid domain, should be one of " +
          Arrays.toString(Domain.values()));
    }
    Objects.requireNonNull(quorumInfo);
    System.out.println("QuorumInfo: " + quorumInfo.toString());

    if (address.equalsIgnoreCase(RANDOM)) {
      List<String> candidateAddressList = quorumInfo.getPeers().stream().filter(
          raftPeer -> !quorumInfo.getLeaderRaftPeerId()
              .equals(raftPeer.getId().toString())).map(RaftPeer::getAddress)
          .collect(Collectors.toList());
      if (candidateAddressList.size() > 0) {
        Collections.shuffle(candidateAddressList);
        address = candidateAddressList.get(0);
        System.out.println("candidate list :" + candidateAddressList +
            "randomly chosen address: " + address);
      } else {
        throw new IOException("Not enough Peers for transferring leadership");
      }
    }

    getRatisPortHost(conf);
    // check address passed whether belongs to the peers
    if (quorumInfo.getPeers().stream().noneMatch(raftPeer ->
        raftPeer.getAddress().contains(address))) {
      throw new IOException(String.format("%s is not part of the " +
          "quorum %s.", address, quorumInfo.getPeers().stream().
              map(RaftPeer::getAddress).collect(Collectors.toList())));
    }
    System.out.printf("Trying to transfer to new leader %s", address);

    RaftClient raftClient = createRatisClient(quorumInfo);
    List<RaftPeer> peersWithNewPriorities = new ArrayList<>();
    for (RaftPeer peer : quorumInfo.getPeers()) {
      peersWithNewPriorities.add(
          RaftPeer.newBuilder(peer)
              .setPriority(peer.getAddress().equalsIgnoreCase(address) ? 2 : 1)
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

    RaftPeerId newLeaderPeerId = quorumInfo.getPeers().stream().
        filter(peer -> peer.getAddress().equalsIgnoreCase(address)).findAny().
        get().getId();

    reply = raftClient.admin().transferLeadership(
        newLeaderPeerId, TRANSFER_LEADER_WAIT_MS);
    if (reply.isSuccess()) {
      System.out.printf("Successfully transferred leadership: %s.%n",
          address);
    } else {
      System.out.printf("Failed to transferring leadership: %s." +
          " Ratis reply: %s%n", address, reply);
      throw new IOException(reply.getException());
    }
  }


  /**
   * adjust the address to Host:Port pattern.
   *
   * @param conf the conf
   * @return the ratis port host
   */
  public String getRatisPortHost(OzoneConfiguration conf) {
    assert EnumUtils.isValidEnum(Domain.class, domain);
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

    if (!address.contains(":")) {
      address = address.concat(":").concat(
          String.valueOf(ratisPort));
    }
    return address;
  }
}
