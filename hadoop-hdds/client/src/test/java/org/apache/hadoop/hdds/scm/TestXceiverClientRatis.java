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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.ratis.client.api.DataStreamApi;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link XceiverClientRatis}.
 */
class TestXceiverClientRatis {

  /**
   * Read-only data streams take turns on as many clients, and so connections, as configured, starting with the main
   * client; other data streams keep using the main client.
   */
  @Test
  void readStreamsTakeTurnsOnTheConfiguredConnections() throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();
    final OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setRatisStreamReadConnections(3);
    conf.setFromObject(clientConfig);

    final XceiverClientRatis client =
        XceiverClientRatis.newXceiverClientRatis(MockPipeline.createRatisPipeline(), conf);
    client.connect();
    try {
      final DataStreamApi main = client.getDataStreamApi();
      final List<DataStreamApi> apis = new ArrayList<>();
      for (int i = 0; i < 6; i++) {
        apis.add(client.getReadStreamApi());
      }

      assertSame(main, apis.get(0));
      assertNotSame(apis.get(0), apis.get(1));
      assertNotSame(apis.get(0), apis.get(2));
      assertNotSame(apis.get(1), apis.get(2));
      assertEquals(apis.subList(0, 3), apis.subList(3, 6));
      assertSame(main, client.getDataStreamApi());
    } finally {
      client.close();
    }
  }
}
