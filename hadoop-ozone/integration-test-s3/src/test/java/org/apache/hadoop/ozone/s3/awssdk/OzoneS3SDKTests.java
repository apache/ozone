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

import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.s3.MultiS3GatewayService;
import org.apache.hadoop.ozone.s3.awssdk.v1.AbstractS3SDKV1Tests;
import org.apache.hadoop.ozone.s3.awssdk.v2.AbstractS3SDKV2Tests;
import org.apache.ozone.test.ClusterForTests;
import org.junit.jupiter.api.Nested;

abstract class OzoneS3SDKTests extends ClusterForTests<MiniOzoneCluster> {

  @Override
  protected MiniOzoneCluster createCluster() throws Exception {
    return newClusterBuilder()
        .addService(new MultiS3GatewayService(5))
        .build();
  }

  @Nested
  class V1 extends AbstractS3SDKV1Tests {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class V2 extends AbstractS3SDKV2Tests {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }
}
