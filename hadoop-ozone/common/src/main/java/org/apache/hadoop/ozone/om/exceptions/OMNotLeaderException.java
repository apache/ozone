/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.exceptions;

import java.io.IOException;
import java.net.InetAddress;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.NotLeaderException;

/**
 * Exception thrown by
 * {@link org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB} when
 * a read request is received by a non leader OM node.
 */
public class OMNotLeaderException extends IOException {
  private final String leaderPeerId;
  private final String leaderAddress;
  private final String leaderIpAddress;

  /**
   * Do NOT change the format of the exception message without changing the parsing logic.
   * <p>
   * The exception message pattern with the suggested leader has been updated for efficient parsing.
   * One of the considerations is IPv6 addresses. They may or may not be enclosed in square brackets "[fe80::1]:9862"
   * when used in URIs/URLs. As such, square brackets should be avoided for other purposes in the
   * same exception message to avoid parsing conflicts.
   */
  private static final String NO_LEADER_FORMAT =
      "OM:%s is not the leader. Could not determine the leader node.";
  private static final String SUGGESTED_LEADER_FORMAT =
      "OM:%s is not the leader. Suggested leader is OM:%s at %s (ip=%s).";
  private static final String UNRESOLVED_IP = "unresolved";

  /**
   * Pattern for parsing the suggested-leader fields from the exception message of the form
   * <pre>
   * OM:om2 is not the leader. Suggested leader is OM:om1 at om1.cluster.local:9862 (ip=10.0.0.5).
   * OM:om2 is not the leader. Suggested leader is OM:om1 at [fe80::1]:9862 (ip=fe80::1).
   * OM:om2 is not the leader. Suggested leader is OM:om1 at om1.cluster.local:9862 (ip=unresolved).
   * </pre>
   */
  private static final Pattern SUGGESTED_LEADER_PATTERN =
      Pattern.compile(".*Suggested leader is OM:(\\S+) at (\\S+) \\(ip=(\\S+?)\\)\\..*", Pattern.DOTALL);

  public OMNotLeaderException(RaftPeerId currentPeerId) {
    super(String.format(NO_LEADER_FORMAT, currentPeerId));
    this.leaderPeerId = null;
    this.leaderAddress = null;
    this.leaderIpAddress = null;
  }

  /**
   * Note that the port here is the RPC port of the suggested leader and not the Ratis port.
   * @param suggestedLeaderHostPort host:rpcPort
   */
  public OMNotLeaderException(RaftPeerId currentPeerId,
      RaftPeerId suggestedLeaderPeerId,
      String suggestedLeaderHostPort,
      String suggestedLeaderIp) {
    super(String.format(SUGGESTED_LEADER_FORMAT,
        currentPeerId, suggestedLeaderPeerId, suggestedLeaderHostPort,
        suggestedLeaderIp != null ? suggestedLeaderIp : UNRESOLVED_IP));
    this.leaderPeerId = suggestedLeaderPeerId.toString();
    this.leaderAddress = suggestedLeaderHostPort;
    this.leaderIpAddress = suggestedLeaderIp;
  }

  public OMNotLeaderException(String msg) {
    super(msg);
    Matcher matcher = SUGGESTED_LEADER_PATTERN.matcher(msg);
    String parsedLeaderPeerId = null;
    String parsedLeaderAddress = null;
    String parsedLeaderIpAddress = null;
    if (matcher.matches()) {
      parsedLeaderPeerId = matcher.group(1);
      parsedLeaderAddress = matcher.group(2);
      parsedLeaderIpAddress = matcher.group(3);
      parsedLeaderIpAddress = parsedLeaderIpAddress.equals(UNRESOLVED_IP) ? null : parsedLeaderIpAddress;
    }
    this.leaderPeerId = parsedLeaderPeerId;
    this.leaderAddress = parsedLeaderAddress;
    this.leaderIpAddress = parsedLeaderIpAddress;
  }

  public String getSuggestedLeaderNodeId() {
    return leaderPeerId;
  }

  public String getSuggestedLeaderAddress() {
    return leaderAddress;
  }

  public String getSuggestedLeaderIpAddress() {
    return leaderIpAddress;
  }

  /**
   * Convert {@link NotLeaderException} to {@link OMNotLeaderException}.
   * @param notLeaderException
   * @param currentPeer
   * @return OMNotLeaderException
   */
  public static OMNotLeaderException convertToOMNotLeaderException(
      NotLeaderException notLeaderException,
      RaftPeerId currentPeer,
      Function<RaftPeerId, OMNodeDetails> peerResolver) {
    RaftPeer suggestedLeader = notLeaderException.getSuggestedLeader();
    if (suggestedLeader == null) {
      return new OMNotLeaderException(currentPeer);
    }
    OMNodeDetails leaderDetails = peerResolver.apply(suggestedLeader.getId());
    if (leaderDetails == null) {
      return new OMNotLeaderException(currentPeer);
    }
    String rpcHostPort = leaderDetails.getRpcAddressString();
    InetAddress inet = leaderDetails.getInetAddress();
    String ip = inet == null ? null : inet.getHostAddress();
    return new OMNotLeaderException(currentPeer, suggestedLeader.getId(), rpcHostPort, ip);
  }
}
