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

package org.apache.hadoop.ozone.shell;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for OzoneRatis.
 */
public class TestOzoneRatis {
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private OzoneRatis ozoneRatis;

  @BeforeEach
  public void setUp() throws UnsupportedEncodingException {
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
    ozoneRatis = new OzoneRatis();
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  /**
   * Execute method to invoke the OzoneRatis class and capture output.
   *
   * @param args command line arguments to pass
   * @return the output from OzoneRatis
   */
  private String execute(String[] args) throws IOException {
    ozoneRatis.execute(args);
    return outContent.toString(StandardCharsets.UTF_8.name());
  }

  @Test
  public void testBasicOzoneRatisCommand() throws IOException {
    String[] args = {""};
    String output = execute(args);
    assertTrue(output.contains("Usage: ratis sh [generic options]"));
  }

  @Test
  public void testLocalRaftMetaConfSubcommand(@TempDir Path tempDir) throws IOException {
    // Set up temporary directory and files
    Path metadataDir = tempDir.resolve("data/metadata/ratis/test-cluster/current/");
    Files.createDirectories(metadataDir);

    // Create a dummy raft-meta.conf file using protobuf
    Path raftMetaConfFile = metadataDir.resolve("raft-meta.conf");

    // Create a LogEntryProto with a dummy index and peer
    RaftProtos.RaftPeerProto raftPeerProto = RaftProtos.RaftPeerProto.newBuilder()
            .setId(ByteString.copyFromUtf8("peer1"))
            .setAddress("localhost:8000")
            .setStartupRole(RaftProtos.RaftPeerRole.FOLLOWER)
            .build();

    RaftProtos.LogEntryProto logEntryProto = RaftProtos.LogEntryProto.newBuilder()
            .setConfigurationEntry(RaftProtos.RaftConfigurationProto.newBuilder()
                    .addPeers(raftPeerProto).build())
            .setIndex(0)
            .build();

    // Write the logEntryProto to the raft-meta.conf file
    try (OutputStream out = Files.newOutputStream(raftMetaConfFile)) {
      logEntryProto.writeTo(out);
    }


    String[] args = {"local", "raftMetaConf", "-peers", "peer1|localhost:8080", "-path", metadataDir.toString()};
    String output = execute(args);

    assertTrue(output.contains("Index in the original file is: 0"));
    assertTrue(output.contains("Generate new LogEntryProto info is:"));

    // Verify that the new raft-meta.conf is generated
    Path newRaftMetaConfFile = metadataDir.resolve("new-raft-meta.conf");
    assertTrue(Files.exists(newRaftMetaConfFile), "New raft-meta.conf file should be created.");

    // Verify content of the newly generated file
    try (InputStream in = Files.newInputStream(newRaftMetaConfFile)) {
      RaftProtos.LogEntryProto newLogEntryProto = RaftProtos.LogEntryProto.parseFrom(in);
      assertEquals(1, newLogEntryProto.getIndex());
      RaftProtos.RaftPeerProto peerProto = newLogEntryProto.getConfigurationEntry().getPeers(0);
      assertEquals("peer1", peerProto.getId().toStringUtf8());
      assertEquals("localhost:8080", peerProto.getAddress());
      assertEquals(RaftProtos.RaftPeerRole.FOLLOWER, peerProto.getStartupRole());
    }
  }

  @Test
  public void testMissingRequiredArguments() throws IOException {
    String[] args = {"local", "raftMetaConf"};
    String output = execute(args);
    assertTrue(output.contains("Failed to parse args for raftMetaConf: Missing required options: peers, path"));
  }

  @Test
  public void testMissingPeerArgument() throws IOException {
    String[] args = {"local", "raftMetaConf", "-path", "/path"};
    String output = execute(args);
    assertTrue(output.contains("Failed to parse args for raftMetaConf: Missing required option: peers"));
  }

  @Test
  public void testMissingPathArgument() throws IOException {
    String[] args = {"local", "raftMetaConf", "-peers", "localhost:8080"};
    String output = execute(args);
    assertTrue(output.contains("Failed to parse args for raftMetaConf: Missing required option: path"));
  }

  @Test
  public void testInvalidPeersFormat() throws IOException {
    String[] args = {"local", "raftMetaConf", "-peers", "localhost8080", "-path", "/path"};
    String output = execute(args);
    assertTrue(output.contains("Failed to parse the server address parameter \"localhost8080\"."));
  }

  @Test
  public void testDuplicatePeersAddress() throws IOException {
    String[] args = {"local", "raftMetaConf", "-peers", "localhost:8080,localhost:8080", "-path", "/path"};
    String output = execute(args);
    assertTrue(output.contains("Found duplicated address: localhost:8080."));
  }

  @Test
  public void testDuplicatePeersId() throws IOException {
    String[] args = {"local", "raftMetaConf", "-peers", "peer1|localhost:8080,peer1|localhost:8081", "-path", "/path"};
    String output = execute(args);
    assertTrue(output.contains("Found duplicated ID: peer1."));
  }
}
