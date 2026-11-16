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

package org.apache.hadoop.ozone.s3.awssdk;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.service.KeyLifecycleServiceConfig;
import org.apache.hadoop.ozone.s3.MultiS3GatewayService;
import org.apache.hadoop.ozone.s3.S3GatewayService;
import org.apache.hadoop.ozone.s3.awssdk.v2.AbstractS3SDKV2Tests;
import org.apache.ozone.test.ClusterForTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class OzoneS3SDKTests extends ClusterForTests<MiniOzoneCluster> {

  @BeforeAll
  void beforeAll() throws Exception {
    startCluster();
  }

  @AfterAll
  void afterAll() {
    shutdownCluster();
  }

  @Override
  protected OzoneConfiguration createOzoneConfig() {
    OzoneConfiguration conf = createBaseConfiguration();
    conf.setBoolean(KeyLifecycleServiceConfig.Keys.ENABLED, true);
    conf.setTimeDuration(KeyLifecycleServiceConfig.Keys.INTERVAL, 1, TimeUnit.SECONDS);
    return conf;
  }

  @Override
  protected MiniOzoneCluster createCluster() throws Exception {
    return newClusterBuilder()
        .addService(new MultiS3GatewayService(5))
        // Outside the proxy, so only a test that addresses it reads the cells of EC keys in parallel.
        .addService(new S3GatewayService(conf -> conf.setBoolean("ozone.client.ec.parallel.read.enabled", true),
            AbstractS3SDKV2Tests.EC_PARALLEL_READ_S3G_HTTP_ADDRESS_KEY))
        .build();
  }

  @Nested
  class V2 extends AbstractS3SDKV2Tests {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

}
