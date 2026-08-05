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

package org.apache.hadoop.hdds.scm;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestXceiverClientGrpcChannel {

  private static final int PORT = 9882;
  private static final String HOSTNAME = "dn-host.example.com";
  private static final String IP_ADDRESS = "192.168.1.100";

  @ParameterizedTest(name = "useDatanodeHostname={0}")
  @ValueSource(booleans = {false, true})
  void createChannelUsesConfiguredAddress(boolean useHostname) throws IOException, InterruptedException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME, useHostname);

    DatanodeDetails dn = datanodeWithDistinctHostAndIp();
    Pipeline pipeline = MockPipeline.createPipeline(Collections.singletonList(dn));

    try (XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf)) {
      ManagedChannel channel = client.createChannel(dn, PORT).build();
      try {
        String expectedHost = useHostname ? HOSTNAME : IP_ADDRESS;
        assertThat(channel.authority()).isEqualTo(expectedHost + ":" + PORT);
      } finally {
        channel.shutdownNow();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    }
  }

  private static DatanodeDetails datanodeWithDistinctHostAndIp() {
    return MockDatanodeDetails.createDatanodeDetails(
        DatanodeID.randomID(), HOSTNAME, IP_ADDRESS, "/rack");
  }

}
