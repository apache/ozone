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

package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;

class ContainerBalancerConfigBuilder {
  private static final int DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER = 15;

  private final ContainerBalancerConfiguration config;

  ContainerBalancerConfigBuilder(int nodeCount) {
    this(new OzoneConfiguration(), nodeCount);
  }

  ContainerBalancerConfigBuilder(OzoneConfiguration ozoneConfig, int nodeCount) {
    config = ozoneConfig.getObject(ContainerBalancerConfiguration.class);
    config.setIterations(1);
    config.setThreshold(10);
    config.setMaxSizeToMovePerIteration(50 * TestContainerBalancerTask.STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * TestContainerBalancerTask.STORAGE_UNIT);
    if (nodeCount < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
  }

  ContainerBalancerConfiguration build() {
    return config;
  }
}
