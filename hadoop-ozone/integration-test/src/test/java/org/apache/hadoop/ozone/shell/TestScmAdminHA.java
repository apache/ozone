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

import java.net.InetSocketAddress;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.apache.ozone.test.HATests;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * This class tests ozone admin scm commands.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestScmAdminHA implements HATests.TestCase {

  private OzoneAdmin ozoneAdmin;
  private MiniOzoneCluster cluster;

  @BeforeAll
  void init() {
    ozoneAdmin = new OzoneAdmin();
    cluster = cluster();
  }

  @Test
  public void testGetRatisRoles() {
    InetSocketAddress address =
        cluster.getStorageContainerManager().getClientRpcAddress();
    String hostPort = address.getHostName() + ":" + address.getPort();
    String[] args = {"--scm", hostPort, "scm", "roles"};
    ozoneAdmin.execute(args);
  }
}
