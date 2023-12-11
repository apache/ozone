/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.balancer.iteration;

import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerConfiguration;

import javax.annotation.Nonnull;

/**
 * Class using for building strategy for looking target nodes.
 */
final class FindTargetStrategyFactory {
  private FindTargetStrategyFactory() { }

  /**
   * @param scm                      StorageContainerManager instance.
   * @param isNetworkTopologyEnabled Value from {@link ContainerBalancerConfiguration}.
   *                                 Specifies the instance of FindTargetStrategy that will be created
   * @return the instance of FindTargetStrategy
   */
  public static @Nonnull FindTargetStrategy create(
      @Nonnull StorageContainerManager scm,
      boolean isNetworkTopologyEnabled
  ) {
    if (isNetworkTopologyEnabled) {
      return new FindTargetGreedyByNetworkTopology(scm);
    } else {
      return new FindTargetGreedyByUsageInfo(scm);
    }
  }
}
