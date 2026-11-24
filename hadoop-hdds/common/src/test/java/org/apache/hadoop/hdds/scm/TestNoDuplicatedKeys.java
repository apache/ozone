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

import org.apache.hadoop.hdds.client.ReplicationConfigValidator;
import org.apache.hadoop.hdds.conf.ConfigurationReflectionUtil;
import org.apache.hadoop.hdds.conf.HddsPrometheusConfig;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.recon.ReconConfig;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerConfiguration;
import org.apache.hadoop.hdds.tracing.TracingConfig;
import org.apache.hadoop.ozone.conf.OzoneServiceConfig;
import org.junit.jupiter.api.Test;

/**
 * Test that no keys are duplicated in the classes using @Config.
 */
class TestNoDuplicatedKeys {

  @Test
  void testNoDuplicatedKeys() {
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(ContainerBalancerConfiguration.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(HddsPrometheusConfig.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(OzoneServiceConfig.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(RatisClientConfig.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(ReconConfig.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(ReplicationConfigValidator.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(ScmConfig.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(ScmRatisServerConfig.class);
    ConfigurationReflectionUtil.assertNoDuplicatedKeys(TracingConfig.class);
  }
}
