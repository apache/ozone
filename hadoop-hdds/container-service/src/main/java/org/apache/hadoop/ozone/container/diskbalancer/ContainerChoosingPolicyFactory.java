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

package org.apache.hadoop.ozone.container.diskbalancer;

import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerConfiguration.HDDS_DATANODE_DISKBALANCER_CONTAINER_CHOOSING_POLICY;

import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.diskbalancer.policy.ContainerChoosingPolicy;
import org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultContainerChoosingPolicy;
import org.apache.ratis.util.ReflectionUtils;

/**
 * A factory to create {@link ContainerChoosingPolicy} instances for the DiskBalancer.
 * The policy class is configured via
 * {@link DiskBalancerConfiguration#getContainerChoosingPolicyClass()}.
 */
public final class ContainerChoosingPolicyFactory {

  private static final Class<? extends ContainerChoosingPolicy>
      DEFAULT_CONTAINER_CHOOSING_POLICY = DefaultContainerChoosingPolicy.class;

  private ContainerChoosingPolicyFactory() {
  }

  /**
   * Creates a ContainerChoosingPolicy instance from configuration.
   *
   * @param conf the configuration source
   * @return a configured ContainerChoosingPolicy instance
   */
  public static ContainerChoosingPolicy getDiskBalancerPolicy(ConfigurationSource conf) {
    Class<? extends ContainerChoosingPolicy> policyClass = conf.getClass(
        HDDS_DATANODE_DISKBALANCER_CONTAINER_CHOOSING_POLICY,
        DEFAULT_CONTAINER_CHOOSING_POLICY, ContainerChoosingPolicy.class);
    return ReflectionUtils.newInstance(policyClass, new Class<?>[]{ReentrantLock.class},
        VolumeChoosingPolicyFactory.getVolumeSpaceReservationLock());
  }
}
