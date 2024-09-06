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
package org.apache.hadoop.hdds.scm.cli.ratis;

import org.apache.hadoop.hdds.scm.cli.ratis.local.RatisLocalRaftMetaConfSubCommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Tests for RatisLocalRaftMetaConfSubCommand class.
 */
public class TestRatisLocalRaftMetaConfSubCommand {

  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private RatisLocalRaftMetaConfSubCommand raftCmd;
  private ScmClient scmClient;

  @BeforeEach
  public void setup() throws IOException {
    raftCmd = new RatisLocalRaftMetaConfSubCommand();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));

    scmClient = mock(ScmClient.class);
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
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
      .setIndex(1)
      .build();

    // Write the logEntryProto to the raft-meta.conf file
    try (OutputStream out = Files.newOutputStream(raftMetaConfFile)) {
      logEntryProto.writeTo(out);
    }

    CommandLine cmd = new CommandLine(raftCmd);
    cmd.parseArgs("-peers", "peer1|localhost:8080", "-path", metadataDir.toString());

    raftCmd.execute(scmClient);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Index in the original file is: 1"));
    assertTrue(output.contains("Generated new LogEntryProto info"));

    // Verify that the new raft-meta.conf is generated
    Path newRaftMetaConfFile = metadataDir.resolve("new-raft-meta.conf");
    assertTrue(Files.exists(newRaftMetaConfFile));

    // Verify content of the newly generated file
    try (InputStream in = Files.newInputStream(newRaftMetaConfFile)) {
      RaftProtos.LogEntryProto newLogEntryProto = RaftProtos.LogEntryProto.parseFrom(in);
      assertEquals(2, newLogEntryProto.getIndex());
      RaftProtos.RaftPeerProto peerProto = newLogEntryProto.getConfigurationEntry().getPeers(0);
      assertEquals("peer1", peerProto.getId().toStringUtf8());
      assertEquals("localhost:8080", peerProto.getAddress());
      assertEquals(RaftProtos.RaftPeerRole.FOLLOWER, peerProto.getStartupRole());
    }
  }

  @Test
  public void testMissingRequiredArguments() {
    CommandLine cmd = new CommandLine(raftCmd);

    Exception exception = assertThrows(CommandLine.MissingParameterException.class, () -> {
      cmd.parseArgs();
      raftCmd.execute(scmClient);
    });

    assertEquals("Missing required options: '-peers=<peers>', '-path=<path>'", exception.getMessage());
  }

  @Test
  public void testMissingPeersArgument() {
    CommandLine cmd = new CommandLine(raftCmd);

    Exception exception = assertThrows(CommandLine.MissingParameterException.class, () -> {
      cmd.parseArgs("-path", "/dummy/path");
      raftCmd.execute(scmClient);
    });

    assertEquals("Missing required option: '-peers=<peers>'", exception.getMessage());
  }

  @Test
  public void testMissingPathArgument() {
    CommandLine cmd = new CommandLine(raftCmd);

    Exception exception = assertThrows(CommandLine.MissingParameterException.class, () -> {
      cmd.parseArgs("-peers", "peer1|localhost:8080");
      raftCmd.execute(scmClient);
    });

    assertEquals("Missing required option: '-path=<path>'", exception.getMessage());
  }

  @Test
  public void testInvalidPeersFormat() {
    CommandLine cmd = new CommandLine(raftCmd);

    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> {
      cmd.parseArgs("-peers", "peer1|localhost8080", "-path", "/dummy/path"); //invalid peers format (missing ':' separator)
      raftCmd.execute(scmClient);
    });

    assertTrue(thrown.getMessage().contains("Failed to parse the server address parameter"));
  }

  @Test
  public void testDuplicatePeersAddress() throws IOException {
    CommandLine cmd = new CommandLine(raftCmd);
    cmd.parseArgs("-peers", "localhost:8080,localhost:8080", "-path", "/dummy/path");
    raftCmd.execute(scmClient);

    String errorOutput = errContent.toString(DEFAULT_ENCODING);
    assertTrue(errorOutput.contains("Found duplicated address"));
  }

  @Test
  public void testDuplicatePeersId() throws IOException {
    CommandLine cmd = new CommandLine(raftCmd);
    cmd.parseArgs("-peers", "peer1|localhost:8080,peer1|localhost:8081", "-path", "/dummy/path");
    raftCmd.execute(scmClient);

    String errorOutput = errContent.toString(DEFAULT_ENCODING);
    assertTrue(errorOutput.contains("Found duplicated ID"));
  }
}
