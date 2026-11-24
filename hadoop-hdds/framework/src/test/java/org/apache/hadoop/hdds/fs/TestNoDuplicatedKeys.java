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

package org.apache.hadoop.hdds.fs;

import org.apache.hadoop.hdds.conf.ConfigurationReflectionUtil;
import org.apache.hadoop.hdds.conf.DatanodeRatisGrpcConfig;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.scm.proxy.SCMClientConfig;
import org.apache.hadoop.hdds.utils.db.RocksDBConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Test that no keys are duplicated in the classes using @Config.
 */
class TestNoDuplicatedKeys {

  @Test
  void testNoDuplicatedKeys() {
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(DUFactory.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(DatanodeRatisGrpcConfig.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(DatanodeRatisServerConfig.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(DedicatedDiskSpaceUsageFactory.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(RocksDBConfiguration.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(SCMClientConfig.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(SpaceUsageCheckFactory.class);
  }
}
