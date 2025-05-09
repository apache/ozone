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

package org.apache.hadoop.ozone.scm;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetVolumeInfosResponseProto;
import org.apache.hadoop.hdds.scm.cli.datanode.VolumeSubCommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.datanode.VolumeInfo;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * This unit test is used to verify whether the output of
 * `TestVolumeFailureSubCommand` meets the expected results.
 */
public class TestVolumeCommand {
  private VolumeSubCommand cmd;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    cmd = new VolumeSubCommand();
  }

  @AfterEach
  public void tearDown() {
  }

  @Test
  public void testCheckVolumeFailureJsonAccuracy() throws Exception {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.getVolumeInfos("all", "", "", 20, 1)).
        thenAnswer(invocation -> getUsageProto());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--json", "--length", "20");


    try (GenericTestUtils.SystemOutCapturer capture =
        new GenericTestUtils.SystemOutCapturer()) {
      cmd.execute(scmClient);
      String output = capture.getOutput();
      assertNotNull(output);
      ObjectMapper mapper = new ObjectMapper();
      JsonNode json = mapper.readTree(output);
      assertTrue(json.isArray());
      assertEquals(5, json.size());
      System.out.println(output);
    }
  }

  @Test
  public void testCheckVolumeFailureTableAccuracy() throws Exception {
    ScmClient scmClient = mock(ScmClient.class);

    when(scmClient.getVolumeInfos("all", "", "", 20, 1)).
        thenAnswer(invocation -> getUsageProto());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--table", "--length", "20");

    try (GenericTestUtils.SystemOutCapturer capture =
         new GenericTestUtils.SystemOutCapturer()) {
      cmd.execute(scmClient);
      String output = capture.getOutput();
      assertThat(output).contains("/data0/ozonedata/hdds");
      assertThat(output).contains("6.76 TB");
    }
  }

  private GetVolumeInfosResponseProto getUsageProto() {
    GetVolumeInfosResponseProto.Builder builder = GetVolumeInfosResponseProto.newBuilder();
    List<HddsProtos.VolumeInfoProto> result = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      HddsProtos.DatanodeDetailsProto datanodeDetails = randomDatanodeDetails().getProtoBufMessage();
      String hostName = datanodeDetails.getHostName();
      String uuId = datanodeDetails.getUuid();
      VolumeInfo volumeInfo =
          new VolumeInfo.Builder().
          setHostName(hostName).
          setUuid(uuId).
          setFailed(true).
          setFailureTime(Time.now()).
          setVolumeName("/data" + i + "/ozonedata/hdds").
          setCapacity(7430477791683L).
          build();
      result.add(volumeInfo.getProtobuf());
    }
    builder.setPages(10);
    builder.setTotal(10);
    builder.setCurrentPage(1);
    builder.addAllVolumeInfos(result);
    return builder.build();
  }
}
