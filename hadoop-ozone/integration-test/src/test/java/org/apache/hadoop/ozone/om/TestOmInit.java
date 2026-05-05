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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test Ozone Manager Init.
 */
public class TestOmInit {

  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster =  MiniOzoneCluster.newBuilder(conf)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }


  /**
   * Tests the OM Initialization.
   * @throws IOException, AuthenticationException
   */
  @Test
  public void testOmInitAgain() throws IOException,
      AuthenticationException {
    // Stop the Ozone Manager
    cluster.getOzoneManager().stop();
    // Now try to init the OM again. It should succeed
    assertTrue(OzoneManager.omInit(conf));
  }

}
