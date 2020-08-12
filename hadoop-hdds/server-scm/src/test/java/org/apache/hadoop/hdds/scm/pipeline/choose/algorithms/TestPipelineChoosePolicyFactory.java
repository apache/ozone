/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.pipeline.choose.algorithms;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PipelineRequestInformation;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.PipelineChoosePolicyFactory.OZONE_SCM_PIPELINE_CHOOSE_POLICY_IMPL_DEFAULT;

/**
 * Test for scm pipeline choose policy factory.
 */
public class TestPipelineChoosePolicyFactory {

  private OzoneConfiguration conf;

  private ScmConfig scmConfig;

  @Before
  public void setup() {
    //initialize network topology instance
    conf = new OzoneConfiguration();
    scmConfig = conf.getObject(ScmConfig.class);
  }

  @Test
  public void testDefaultPolicy() throws IOException {
    PipelineChoosePolicy policy = PipelineChoosePolicyFactory
        .getPolicy(conf);
    Assert.assertSame(OZONE_SCM_PIPELINE_CHOOSE_POLICY_IMPL_DEFAULT,
        policy.getClass());
  }


  /**
   * A dummy pipeline choose policy implementation for test.
   */
  public static class DummyImpl implements PipelineChoosePolicy {

    public DummyImpl(String dummy) {
    }

    @Override
    public Pipeline choosePipeline(List<Pipeline> pipelineList,
        PipelineRequestInformation pri) {
      return null;
    }
  }

  @Test
  public void testConstuctorNotFound() throws SCMException {
    // set a policy class which does't have the right constructor implemented
    scmConfig.setPipelineChoosePolicyName(DummyImpl.class.getName());
    PipelineChoosePolicy policy = PipelineChoosePolicyFactory.getPolicy(conf);
    Assert.assertSame(OZONE_SCM_PIPELINE_CHOOSE_POLICY_IMPL_DEFAULT,
        policy.getClass());
  }

  @Test
  public void testClassNotImplemented() throws SCMException {
    // set a placement class not implemented
    conf.set(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        "org.apache.hadoop.hdds.scm.pipeline.choose.policy.HelloWorld");
    PipelineChoosePolicy policy = PipelineChoosePolicyFactory.getPolicy(conf);
    Assert.assertSame(OZONE_SCM_PIPELINE_CHOOSE_POLICY_IMPL_DEFAULT,
        policy.getClass());
  }
}
