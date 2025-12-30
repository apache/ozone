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

package org.apache.hadoop.hdds.scm.pipeline.leader.choose.algorithms;

import java.lang.reflect.Constructor;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineStateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory to create leader choose policy instance based on configuration
 * property {@link ScmConfigKeys#OZONE_SCM_PIPELINE_LEADER_CHOOSING_POLICY}.
 */
public final class LeaderChoosePolicyFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(LeaderChoosePolicyFactory.class);

  private static final Class<? extends LeaderChoosePolicy>
      OZONE_SCM_PIPELINE_LEADER_CHOOSING_POLICY_DEFAULT =
      MinLeaderCountChoosePolicy.class;

  private LeaderChoosePolicyFactory() {
  }

  public static LeaderChoosePolicy getPolicy(
      ConfigurationSource conf, final NodeManager nodeManager,
      final PipelineStateManager pipelineStateManager) throws SCMException {
    final Class<? extends LeaderChoosePolicy> policyClass = conf
        .getClass(ScmConfigKeys.OZONE_SCM_PIPELINE_LEADER_CHOOSING_POLICY,
            OZONE_SCM_PIPELINE_LEADER_CHOOSING_POLICY_DEFAULT,
            LeaderChoosePolicy.class);
    Constructor<? extends LeaderChoosePolicy> constructor;
    try {
      constructor = policyClass.getDeclaredConstructor(NodeManager.class,
          PipelineStateManager.class);
      LOG.info("Create leader choose policy of type {}",
          policyClass.getCanonicalName());
    } catch (NoSuchMethodException e) {
      String msg = "Failed to find constructor(NodeManager, " +
          "PipelineStateManagerImpl) for class " +
          policyClass.getCanonicalName();
      LOG.error(msg);
      throw new SCMException(msg,
          SCMException.ResultCodes.FAILED_TO_INIT_LEADER_CHOOSE_POLICY);
    }

    try {
      return constructor.newInstance(nodeManager, pipelineStateManager);
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate class " +
          policyClass.getCanonicalName() + " for " + e.getMessage());
    }
  }
}
