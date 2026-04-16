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

package org.apache.hadoop.ozone.admin.upgrade;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Unit tests for {@link StatusSubCommand}.
 */
public class TestStatusSubCommand {

  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private StatusSubCommand cmd;

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    cmd = new StatusSubCommand();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
  }

  @Test
  public void testStatusCommandPrintsUpgradeStatus() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    HddsProtos.UpgradeStatus status = HddsProtos.UpgradeStatus.newBuilder()
        .setScmFinalized(false)
        .setNumDatanodesFinalized(1)
        .setNumDatanodesTotal(3)
        .setShouldFinalize(true)
        .build();
    when(scmClient.queryUpgradeStatus()).thenReturn(status);

    new CommandLine(cmd).parseArgs();
    cmd.execute(scmClient);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Upgrade status:"));
    assertTrue(output.contains("SCM Finalized: false"));
    assertTrue(output.contains("Datanodes finalized: 1"));
    assertTrue(output.contains("Total Datanodes: 3"));
    assertTrue(output.contains("Should Finalize: true"));
    verify(scmClient).queryUpgradeStatus();
  }
}
