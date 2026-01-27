package org.apache.hadoop.ozone.om.ratis;

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Result of creating a list of peers.
 */
public class CreateRaftPeerListResult {
  private final RaftPeerId raftPeerId;
  private final InetSocketAddress inetSocketAddress;
  private final List<RaftPeer> peers;

  public CreateRaftPeerListResult(RaftPeerId raftPeerId, InetSocketAddress inetSocketAddress, List<RaftPeer> peers) {
    this.raftPeerId = raftPeerId;
    this.inetSocketAddress = inetSocketAddress;
    this.peers = peers;
  }

  public RaftPeerId getRaftPeerId() {
    return raftPeerId;
  }

  public InetSocketAddress getInetSocketAddress() {
    return inetSocketAddress;
  }

  public List<RaftPeer> getPeers() {
    return peers;
  }
}
