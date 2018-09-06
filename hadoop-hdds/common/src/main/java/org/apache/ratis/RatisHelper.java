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

package org.apache.ratis;

import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.proto.RaftProtos;
import org.apache.ratis.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Ratis helper methods.
 */
public interface RatisHelper {
  Logger LOG = LoggerFactory.getLogger(RatisHelper.class);

  static String toRaftPeerIdString(DatanodeDetails id) {
    return id.getUuidString();
  }

  static UUID toDatanodeId(String peerIdString) {
    return UUID.fromString(peerIdString);
  }

  static UUID toDatanodeId(RaftPeerId peerId) {
    return toDatanodeId(peerId.toString());
  }

  static UUID toDatanodeId(RaftProtos.RaftPeerProto peerId) {
    return toDatanodeId(RaftPeerId.valueOf(peerId.getId()));
  }

  static String toRaftPeerAddressString(DatanodeDetails id) {
    return id.getIpAddress() + ":" +
        id.getPort(DatanodeDetails.Port.Name.RATIS).getValue();
  }

  static RaftPeerId toRaftPeerId(DatanodeDetails id) {
    return RaftPeerId.valueOf(toRaftPeerIdString(id));
  }

  static RaftPeer toRaftPeer(DatanodeDetails id) {
    return new RaftPeer(toRaftPeerId(id), toRaftPeerAddressString(id));
  }

  static List<RaftPeer> toRaftPeers(Pipeline pipeline) {
    return toRaftPeers(pipeline.getMachines());
  }

  static <E extends DatanodeDetails> List<RaftPeer> toRaftPeers(
      List<E> datanodes) {
    return datanodes.stream().map(RatisHelper::toRaftPeer)
        .collect(Collectors.toList());
  }

  /* TODO: use a dummy id for all groups for the moment.
   *       It should be changed to a unique id for each group.
   */
  RaftGroupId DUMMY_GROUP_ID =
      RaftGroupId.valueOf(ByteString.copyFromUtf8("AOzoneRatisGroup"));

  RaftGroup EMPTY_GROUP = new RaftGroup(DUMMY_GROUP_ID,
      Collections.emptyList());

  static RaftGroup emptyRaftGroup() {
    return EMPTY_GROUP;
  }

  static RaftGroup newRaftGroup(Collection<RaftPeer> peers) {
    return peers.isEmpty()? emptyRaftGroup()
        : new RaftGroup(DUMMY_GROUP_ID, peers);
  }

  static RaftGroup newRaftGroup(RaftGroupId groupId,
      Collection<DatanodeDetails> peers) {
    final List<RaftPeer> newPeers = peers.stream()
        .map(RatisHelper::toRaftPeer)
        .collect(Collectors.toList());
    return peers.isEmpty() ? new RaftGroup(groupId, Collections.emptyList())
        : new RaftGroup(groupId, newPeers);
  }

  static RaftGroup newRaftGroup(Pipeline pipeline) {
    return newRaftGroup(toRaftPeers(pipeline));
  }

  static RaftClient newRaftClient(RpcType rpcType, Pipeline pipeline) {
    return newRaftClient(rpcType, toRaftPeerId(pipeline.getLeader()),
        newRaftGroup(pipeline.getId().getRaftGroupID(),
            pipeline.getMachines()));
  }

  static RaftClient newRaftClient(RpcType rpcType, RaftPeer leader) {
    return newRaftClient(rpcType, leader.getId(),
        newRaftGroup(new ArrayList<>(Arrays.asList(leader))));
  }

  static RaftClient newRaftClient(RpcType rpcType, RaftPeer leader,
      RaftGroup group) {
    return newRaftClient(rpcType, leader.getId(), group);
  }

  static RaftClient newRaftClient(
      RpcType rpcType, RaftPeerId leader, RaftGroup group) {
    LOG.trace("newRaftClient: {}, leader={}, group={}", rpcType, leader, group);
    final RaftProperties properties = new RaftProperties();
    RaftConfigKeys.Rpc.setType(properties, rpcType);
    GrpcConfigKeys.setMessageSizeMax(properties,
        SizeInBytes.valueOf(OzoneConfigKeys.DFS_CONTAINER_CHUNK_MAX_SIZE));

    return RaftClient.newBuilder()
        .setRaftGroup(group)
        .setLeaderId(leader)
        .setProperties(properties)
        .build();
  }
}
