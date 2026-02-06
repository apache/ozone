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

package org.apache.hadoop.hdds.scm.cli.container;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerListResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Tests for AckMissingContainerSubcommand.
 */
public class TestAckMissingContainerSubcommand {

  private ScmClient scmClient;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  @BeforeEach
  public void setup() throws IOException {
    scmClient = mock(ScmClient.class);

    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @Test
  public void testAckMissingContainer() throws Exception {
    ContainerInfo container = mockContainer(1, false);
    when(scmClient.getContainer(1L)).thenReturn(container);

    AckMissingSubcommand cmd = new AckMissingSubcommand();
    new CommandLine(cmd).parseArgs("1");
    cmd.execute(scmClient);
    verify(scmClient, times(1)).acknowledgeMissingContainer(1L);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertThat(output).contains("Acknowledged container: 1");
  }

  @Test
  public void testListAcknowledgedContainers() throws Exception {
    ContainerInfo container1 = mockContainer(1, true);
    ContainerInfo container2 = mockContainer(2, false);

    List<ContainerInfo> allContainers = Arrays.asList(container1, container2);
    ContainerListResult result = new ContainerListResult(allContainers, 2);
    when(scmClient.listContainer(anyLong(), anyInt())).thenReturn(result);

    AckMissingSubcommand cmd = new AckMissingSubcommand();
    new CommandLine(cmd).parseArgs("--list");
    cmd.execute(scmClient);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertThat(output).contains("1");
    assertThat(output).doesNotContain("2");
  }

  @Test
  public void testUnacknowledgeMissingContainer() throws Exception {
    ContainerInfo container = mockContainer(1, true);
    when(scmClient.getContainer(1L)).thenReturn(container);

    UnackMissingSubcommand cmd = new UnackMissingSubcommand();
    new CommandLine(cmd).parseArgs("1");
    cmd.execute(scmClient);
    verify(scmClient, times(1)).unacknowledgeMissingContainer(1L);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertThat(output).contains("Unacknowledged container: 1");
  }

  private ContainerInfo mockContainer(long containerID, boolean ackMissing) {
    return new ContainerInfo.Builder()
        .setContainerID(containerID)
        .setState(OPEN)
        .setHealthState(ContainerHealthState.MISSING)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
        .setNumberOfKeys(1)
        .setAckMissing(ackMissing)
        .build();
  }
}
