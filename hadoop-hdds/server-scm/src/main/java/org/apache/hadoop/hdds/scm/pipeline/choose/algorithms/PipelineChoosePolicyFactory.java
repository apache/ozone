/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline.choose.algorithms;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

/**
 * A factory to create pipeline choose policy instance based on configuration
 * property {@link ScmConfig}.
 */
public final class PipelineChoosePolicyFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineChoosePolicyFactory.class);

  @VisibleForTesting
  public static final Class<? extends PipelineChoosePolicy>
      OZONE_SCM_PIPELINE_CHOOSE_POLICY_IMPL_DEFAULT =
      RandomPipelineChoosePolicy.class;

  private PipelineChoosePolicyFactory() {
  }

  public static PipelineChoosePolicy getPolicy(
      ConfigurationSource conf) throws SCMException {
    ScmConfig scmConfig = conf.getObject(ScmConfig.class);
    Class<? extends PipelineChoosePolicy> policyClass = getClass(
        scmConfig.getPipelineChoosePolicyName(), PipelineChoosePolicy.class);

    try {
      return createPipelineChoosePolicyFromClass(policyClass);
    } catch (Exception e) {
      if (policyClass != OZONE_SCM_PIPELINE_CHOOSE_POLICY_IMPL_DEFAULT) {
        LOG.error("Met an exception while create pipeline choose policy "
            + "for the given class " + policyClass.getName()
            + ". Fallback to the default pipeline choose policy "
            + OZONE_SCM_PIPELINE_CHOOSE_POLICY_IMPL_DEFAULT, e);
        return createPipelineChoosePolicyFromClass(
            OZONE_SCM_PIPELINE_CHOOSE_POLICY_IMPL_DEFAULT);
      }
      throw e;
    }
  }

  private static PipelineChoosePolicy createPipelineChoosePolicyFromClass(
      Class<? extends PipelineChoosePolicy> policyClass) throws SCMException {
    Constructor<? extends PipelineChoosePolicy> constructor;
    try {
      constructor = policyClass.getDeclaredConstructor();
      LOG.info("Create pipeline choose policy of type {}",
          policyClass.getCanonicalName());
    } catch (NoSuchMethodException e) {
      String msg = "Failed to find constructor() for class " +
          policyClass.getCanonicalName();
      LOG.error(msg);
      throw new SCMException(msg,
          SCMException.ResultCodes.FAILED_TO_INIT_PIPELINE_CHOOSE_POLICY);
    }

    try {
      return constructor.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate class " +
          policyClass.getCanonicalName() + " for " + e.getMessage());
    }
  }

  private static <U> Class<? extends U> getClass(String name,
      Class<U> xface) {
    try {
      Class<?> theClass = Class.forName(name);
      if (!xface.isAssignableFrom(theClass)) {
        throw new RuntimeException(theClass + " not " + xface.getName());
      } else {
        return theClass.asSubclass(xface);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
