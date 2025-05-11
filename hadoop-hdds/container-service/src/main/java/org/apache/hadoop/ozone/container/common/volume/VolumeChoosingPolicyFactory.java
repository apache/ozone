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

package org.apache.hadoop.ozone.container.common.volume;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_VOLUME_CHOOSING_POLICY;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.ratis.util.ReflectionUtils;

/**
 * A factory to create volume choosing policy instance based on configuration
 * property {@link HddsConfigKeys#HDDS_DATANODE_VOLUME_CHOOSING_POLICY}.
 */
public final class VolumeChoosingPolicyFactory {

  private static final Class<? extends VolumeChoosingPolicy>
      DEFAULT_VOLUME_CHOOSING_POLICY = CapacityVolumeChoosingPolicy.class;

  private VolumeChoosingPolicyFactory() {
  }

  public static VolumeChoosingPolicy getPolicy(ConfigurationSource conf) {
    Class<? extends VolumeChoosingPolicy> policyClass = conf.getClass(
        HDDS_DATANODE_VOLUME_CHOOSING_POLICY,
        DEFAULT_VOLUME_CHOOSING_POLICY, VolumeChoosingPolicy.class);
    return ReflectionUtils.newInstance(policyClass);
  }
}
