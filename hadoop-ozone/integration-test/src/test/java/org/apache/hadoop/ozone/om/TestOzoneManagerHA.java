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

import org.junit.jupiter.api.BeforeAll;

/**
 * Base class for Ozone Manager HA tests.
 */
public abstract class TestOzoneManagerHA extends AbstractOzoneManagerHATest {

  @BeforeAll
  public static void init() throws Exception {
    initCluster(false);
  }

  /**
   * Stop the current leader OM.
   */
  protected void stopLeaderOM() {
    // The omFailoverProxyProvider will point to the current leader OM node.
    final String leaderOMNodeId = OmTestUtil.getCurrentOmProxyNodeId(getObjectStore());

    // Stop one of the ozone manager, to see when the OM leader changes
    // multipart upload is happening successfully or not.
    getCluster().stopOzoneManager(leaderOMNodeId);
  }
}
