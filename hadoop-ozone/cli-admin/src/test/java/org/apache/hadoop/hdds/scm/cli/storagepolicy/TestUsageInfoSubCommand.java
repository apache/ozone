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

package org.apache.hadoop.hdds.scm.cli.storagepolicy;

import static com.fasterxml.jackson.databind.node.JsonNodeType.ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DatanodeStorageTypeUsageInfoProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StorageTypeUsageInfoProto;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Test for the UsageInfoSubCommand class.
 */
public class TestUsageInfoSubCommand {

  private UsageInfoSubCommand cmd;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    cmd = new UsageInfoSubCommand();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testClusterSummaryJsonOutput() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.listStorageTypeUsageInfo(any())).thenReturn(twoDatanodesTwoStorageTypes());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--json");
    cmd.execute(scmClient);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode json = mapper.readTree(outContent.toString("UTF-8"));

    JsonNode summary = json.get("summary");
    assertThat(summary.getNodeType()).isEqualTo(ARRAY);
    assertThat(summary.size()).isEqualTo(2);

    JsonNode disk = findByStorageType(summary, "DISK");
    assertThat(disk).isNotNull();
    assertThat(disk.get("datanodeCount").intValue()).isEqualTo(1);
    assertThat(disk.get("capacity").longValue()).isEqualTo(2000);
    assertThat(disk.get("used").longValue()).isEqualTo(500);

    JsonNode ssd = findByStorageType(summary, "SSD");
    assertThat(ssd).isNotNull();
    assertThat(ssd.get("datanodeCount").intValue()).isEqualTo(1);
    assertThat(ssd.get("capacity").longValue()).isEqualTo(1000);

    // --with-datanode was not passed, so the per-datanode breakdown must be omitted entirely.
    assertThat(json.has("datanodeUsage")).isFalse();
  }

  @Test
  public void testWithDatanodeJsonOutputIncludesPerDatanodeBreakdown() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.listStorageTypeUsageInfo(any())).thenReturn(twoDatanodesTwoStorageTypes());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--json", "--with-datanode");
    cmd.execute(scmClient);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode json = mapper.readTree(outContent.toString("UTF-8"));

    JsonNode datanodeUsage = json.get("datanodeUsage");
    assertThat(datanodeUsage.getNodeType()).isEqualTo(ARRAY);
    assertThat(datanodeUsage.size()).isEqualTo(2);
    assertThat(datanodeUsage.get(0).get("datanodeDetails")).isNotNull();
  }

  @Test
  public void testClusterSummaryTextOutputFieldsAligning() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.listStorageTypeUsageInfo(any())).thenReturn(twoDatanodesTwoStorageTypes());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs();
    cmd.execute(scmClient);

    String output = outContent.toString(StandardCharsets.UTF_8.name());
    assertThat(output).contains("Cluster StorageType Usage Summary");
    assertThat(output).contains("DISK Datanode Count    :");
    assertThat(output).contains("DISK Capacity          :");
    assertThat(output).contains("SSD Capacity           :");
  }

  @Test
  public void testClusterSummaryAggregationOverflowThrowsInsteadOfWrapping() {
    // Two datanodes whose DISK capacities together overflow Long.MAX_VALUE must fail loudly
    // via Math.addExact rather than silently wrap around to a negative total.
    ScmClient scmClient = mock(ScmClient.class);
    DatanodeStorageTypeUsageInfoProto dn1 =
        datanodeUsage(Long.MAX_VALUE - 1, 0, 0, 0, 0, HddsProtos.StorageTypeProto.DISK);
    DatanodeStorageTypeUsageInfoProto dn2 = datanodeUsage(2, 0, 0, 0, 0, HddsProtos.StorageTypeProto.DISK);
    try {
      when(scmClient.listStorageTypeUsageInfo(any())).thenReturn(Arrays.asList(dn1, dn2));
    } catch (IOException e) {
      throw new AssertionError(e);
    }

    CommandLine c = new CommandLine(cmd);
    c.parseArgs();
    assertThatThrownBy(() -> cmd.execute(scmClient)).isInstanceOf(ArithmeticException.class);
  }

  private JsonNode findByStorageType(JsonNode summary, String storageType) {
    for (JsonNode node : summary) {
      if (storageType.equals(node.get("storageType").asText())) {
        return node;
      }
    }
    return null;
  }

  private List<DatanodeStorageTypeUsageInfoProto> twoDatanodesTwoStorageTypes() {
    return Arrays.asList(
        datanodeUsage(2000, 500, 1500, 0, 0, HddsProtos.StorageTypeProto.DISK),
        datanodeUsage(1000, 200, 800, 0, 0, HddsProtos.StorageTypeProto.SSD));
  }

  private DatanodeStorageTypeUsageInfoProto datanodeUsage(long capacity, long used, long remaining,
      long committed, long freeSpaceToSpare, HddsProtos.StorageTypeProto storageType) {
    return DatanodeStorageTypeUsageInfoProto.newBuilder()
        .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails().getProtoBufMessage())
        .addStorageTypeUsageInfo(StorageTypeUsageInfoProto.newBuilder()
            .setStorageType(storageType)
            .setCapacity(capacity)
            .setUsed(used)
            .setRemaining(remaining)
            .setCommitted(committed)
            .setFreeSpaceToSpare(freeSpaceToSpare)
            .build())
        .build();
  }
}
