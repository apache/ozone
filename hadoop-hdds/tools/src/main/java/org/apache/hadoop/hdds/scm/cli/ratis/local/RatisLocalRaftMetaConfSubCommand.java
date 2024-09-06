/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.cli.ratis.local;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import picocli.CommandLine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Handler to generate a new raft-meta.conf.
 */
@CommandLine.Command(
    name = "raftMetaConf",
    description = "Generates a new raft-meta.conf",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)

public class RatisLocalRaftMetaConfSubCommand extends ScmSubcommand {

  @CommandLine.Option(names = { "-peers" },
      description = "Provide list of peers in format" +
        " <[P0_ID|]P0_HOST:P0_PORT,[P1_ID|]P1_HOST:P1_PORT,[P2_ID|]P2_HOST:P2_PORT>",
      required = true)
  private String peers;

  @CommandLine.Option(names = { "-path" },
      description = "Parent path of raft-meta.conf",
      required = true)
  private String path;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    if (peers == null || path == null || peers.isEmpty() || path.isEmpty()) {
      System.err.println("peers or path cannot be empty.");
      return;
    }
    Set<String> addresses = new HashSet<>();
    Set<String> ids = new HashSet<>();
    List<RaftProtos.RaftPeerProto> raftPeerProtos = new ArrayList<>();
    String[] peerArray = peers.split(",");

    for (String idWithAddress : peerArray) {
      String[] peerIdWithAddressArray = idWithAddress.split("\\|");

      if (peerIdWithAddressArray.length < 1 || peerIdWithAddressArray.length > 2) {
        System.err.println(
            "Failed to parse peer's ID and address for: " + idWithAddress + ", from option: -peers " + peers +
            ". Please provide list of peers in format " +
            "<[P0_ID|]P0_HOST:P0_PORT,[P1_ID|]P1_HOST:P1_PORT,[P2_ID|]P2_HOST:P2_PORT>");
        return;
      }

      InetSocketAddress inetSocketAddress = parseInetSocketAddress(
          peerIdWithAddressArray[peerIdWithAddressArray.length - 1]);
      String addressString = inetSocketAddress.getHostString() + ":" + inetSocketAddress.getPort();

      if (addresses.contains(addressString)) {
        System.err.println("Found duplicated address: " + addressString +
            ". Please ensure the address of peers have no duplicated value.");
        return;
      }
      addresses.add(addressString);

      String peerId;
      if (peerIdWithAddressArray.length == 2) {
        peerId = RaftPeerId.getRaftPeerId(peerIdWithAddressArray[0]).toString();

        if (ids.contains(peerId)) {
          System.err.println("Found duplicated ID: " + peerId +
              ". Please ensure the ID of peers have no duplicated value.");
          return;
        }
        ids.add(peerId);
      } else {
        peerId = getPeerId(inetSocketAddress).toString();
      }

      raftPeerProtos.add(RaftProtos.RaftPeerProto.newBuilder()
          .setId(ByteString.copyFrom(peerId.getBytes(StandardCharsets.UTF_8)))
          .setAddress(addressString)
          .setStartupRole(RaftProtos.RaftPeerRole.FOLLOWER)
          .build());
    }

    try (InputStream in = Files.newInputStream(Paths.get(path, "raft-meta.conf"));
        OutputStream out = Files.newOutputStream(Paths.get(path, "new-raft-meta.conf"))) {

      long index = RaftProtos.LogEntryProto.newBuilder().mergeFrom(in).build().getIndex();
      System.out.println("Index in the original file is: " + index);

      RaftProtos.LogEntryProto newLogEntryProto = RaftProtos.LogEntryProto.newBuilder()
          .setConfigurationEntry(RaftProtos.RaftConfigurationProto.newBuilder()
            .addAllPeers(raftPeerProtos).build())
          .setIndex(index + 1)
          .build();

      System.out.println("Generated new LogEntryProto info:\n" + newLogEntryProto);
      newLogEntryProto.writeTo(out);
    }
  }

  public static InetSocketAddress parseInetSocketAddress(String address) {
    try {
      final String[] hostPortPair = address.split(":");
      if (hostPortPair.length < 2) {
        throw new IllegalArgumentException("Unexpected address format <HOST:PORT>.");
      }
      return new InetSocketAddress(hostPortPair[0], Integer.parseInt(hostPortPair[1]));
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse the server address parameter \"" + address + "\".", e);
    }
  }

  public static RaftPeerId getPeerId(InetSocketAddress address) {
    return getPeerId(address.getHostString(), address.getPort());
  }

  public static RaftPeerId getPeerId(String host, int port) {
    return RaftPeerId.getRaftPeerId(host + "_" + port);
  }
}
