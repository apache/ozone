/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.ratis;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

import java.util.Collection;
import java.util.Objects;

/**
 *  Wrapper class for raft group info.
 */
public class QuorumInfo {
  private RaftGroupId raftGroupId;
  private Collection<RaftPeer> peers;
  private String leaderRaftPeerId;

  public QuorumInfo(RaftGroupId raftGroupId, Collection<RaftPeer> peers,
                    String leaderRaftId) {
    this.raftGroupId = raftGroupId;
    this.peers = peers;
    this.leaderRaftPeerId = leaderRaftId;
  }

  public RaftGroupId getRaftGroupId() {
    return raftGroupId;
  }

  public void setRaftGroupId(RaftGroupId raftGroupId) {
    this.raftGroupId = raftGroupId;
  }

  public Collection<RaftPeer> getPeers() {
    return peers;
  }

  public void setPeers(Collection<RaftPeer> peers) {
    this.peers = peers;
  }

  public String getLeaderRaftPeerId() {
    return leaderRaftPeerId;
  }

  public void setLeaderRaftPeerId(String leaderRaftPeerId) {
    this.leaderRaftPeerId = leaderRaftPeerId;
  }

  @Override
  public String toString() {
    return "raftGroupId: " + raftGroupId.toString() +
          " peers: " + peers.toString() +
          " leaderAddress: " + leaderRaftPeerId;
  }


  /**
   * The type Builder.
   */
  public static class Builder {
    private RaftGroupId groupId;
    private Collection<RaftPeer> peersCollection;
    private String leaderRaftId;

    public Builder setRaftGroupId(RaftGroupId raftGroupId) {
      groupId = raftGroupId;
      return this;
    }

    public Builder setPeers(Collection<RaftPeer> peers) {
      peersCollection = peers;
      return this;
    }

    public Builder setLeaderRaftId(String id) {
      leaderRaftId = id;
      return this;
    }

    public QuorumInfo build() {
      return new QuorumInfo(groupId, peersCollection, leaderRaftId);
    }
  }


  /**
   * Tool function to Get proto from RaftPeer.
   *
   * @param raftPeer the raft peer
   * @return the protobuf
   */
  public static HddsProtos.RaftPeerProto getProtobuf(RaftPeer raftPeer) {
    Objects.requireNonNull(raftPeer.getId());
    Objects.requireNonNull(raftPeer.getAddress());
    return HddsProtos.RaftPeerProto.newBuilder()
        .setId(raftPeer.getId().toString())
        .setAddress(raftPeer.getAddress())
        .setPriority(raftPeer.getPriority())
        .build();
  }

}
