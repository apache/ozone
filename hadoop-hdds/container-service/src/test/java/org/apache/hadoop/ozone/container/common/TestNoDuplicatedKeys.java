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

package org.apache.hadoop.ozone.container.common;

import org.apache.hadoop.hdds.conf.ConfigurationReflectionUtil;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.RatisServerConfiguration;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerConfiguration;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.hadoop.ozone.container.replication.ReplicationServer;
import org.junit.jupiter.api.Test;

/**
 * Test that no keys are duplicated in the classes using @Config.
 */
class TestNoDuplicatedKeys {

  @Test
  void testNoDuplicatedKeys() {
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(ContainerScannerConfiguration.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(DatanodeConfiguration.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(DiskBalancerConfiguration.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(RatisServerConfiguration.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(ReplicationServer.class);
  }
}
