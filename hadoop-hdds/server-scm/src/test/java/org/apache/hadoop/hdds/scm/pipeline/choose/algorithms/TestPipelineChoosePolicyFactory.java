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

package org.apache.hadoop.hdds.scm.pipeline.choose.algorithms;

import static org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.PipelineChoosePolicyFactory.OZONE_SCM_EC_PIPELINE_CHOOSE_POLICY_IMPL_DEFAULT;
import static org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.PipelineChoosePolicyFactory.OZONE_SCM_PIPELINE_CHOOSE_POLICY_IMPL_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PipelineRequestInformation;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for scm pipeline choose policy factory.
 */
public class TestPipelineChoosePolicyFactory {

  private ScmConfig scmConfig;

  private NodeManager nodeManager;

  @BeforeEach
  public void setup() {
    //initialize network topology instance
    OzoneConfiguration conf = new OzoneConfiguration();
    scmConfig = conf.getObject(ScmConfig.class);
    nodeManager = new MockNodeManager(true, 5);
  }

  @Test
  public void testDefaultPolicy() throws IOException {
    PipelineChoosePolicy policy = PipelineChoosePolicyFactory
        .getPolicy(nodeManager, scmConfig, false);
    assertSame(OZONE_SCM_PIPELINE_CHOOSE_POLICY_IMPL_DEFAULT,
        policy.getClass());
  }

  @Test
  public void testDefaultPolicyEC() throws IOException {
    PipelineChoosePolicy policy = PipelineChoosePolicyFactory
        .getPolicy(nodeManager, scmConfig, true);
    assertSame(OZONE_SCM_EC_PIPELINE_CHOOSE_POLICY_IMPL_DEFAULT,
        policy.getClass());
  }

  @Test
  public void testNonDefaultPolicyEC() throws IOException {
    scmConfig.setECPipelineChoosePolicyName(DummyGoodImpl.class.getName());
    PipelineChoosePolicy policy = PipelineChoosePolicyFactory
        .getPolicy(nodeManager, scmConfig, true);
    assertSame(DummyGoodImpl.class, policy.getClass());
  }

  /**
   * A dummy pipeline choose policy implementation for test with an invalid
   * constructor.
   */
  public static class DummyImpl implements PipelineChoosePolicy {

    public DummyImpl(String dummy) {
    }

    @Override
    public Pipeline choosePipeline(List<Pipeline> pipelineList,
        PipelineRequestInformation pri) {
      return null;
    }

    @Override
    public int choosePipelineIndex(List<Pipeline> pipelineList,
        PipelineRequestInformation pri) {
      return -1;
    }
  }

  /**
   * A dummy pipeline choose policy implementation for test that is valid.
   */
  public static class DummyGoodImpl implements PipelineChoosePolicy {

    @Override
    public Pipeline choosePipeline(List<Pipeline> pipelineList,
                                   PipelineRequestInformation pri) {
      return null;
    }

    @Override
    public int choosePipelineIndex(List<Pipeline> pipelineList,
                                   PipelineRequestInformation pri) {
      return -1;
    }
  }

  @Test
  public void testConstructorNotFound() throws SCMException {
    // set a policy class which does't have the right constructor implemented
    scmConfig.setPipelineChoosePolicyName(DummyImpl.class.getName());
    scmConfig.setECPipelineChoosePolicyName(DummyImpl.class.getName());
    PipelineChoosePolicy policy =
        PipelineChoosePolicyFactory.getPolicy(nodeManager, scmConfig, false);
    assertSame(OZONE_SCM_PIPELINE_CHOOSE_POLICY_IMPL_DEFAULT,
        policy.getClass());
    policy = PipelineChoosePolicyFactory.getPolicy(nodeManager, scmConfig, true);
    assertSame(OZONE_SCM_EC_PIPELINE_CHOOSE_POLICY_IMPL_DEFAULT,
        policy.getClass());
  }

  @Test
  public void testClassNotImplemented() throws SCMException {
    // set a placement class not implemented
    scmConfig.setPipelineChoosePolicyName(
        "org.apache.hadoop.hdds.scm.pipeline.choose.policy.HelloWorld");
    scmConfig.setECPipelineChoosePolicyName(
        "org.apache.hadoop.hdds.scm.pipeline.choose.policy.HelloWorld");
    PipelineChoosePolicy policy =
        PipelineChoosePolicyFactory.getPolicy(nodeManager, scmConfig, false);
    assertSame(OZONE_SCM_PIPELINE_CHOOSE_POLICY_IMPL_DEFAULT,
        policy.getClass());
    policy = PipelineChoosePolicyFactory.getPolicy(nodeManager, scmConfig, true);
    assertSame(OZONE_SCM_EC_PIPELINE_CHOOSE_POLICY_IMPL_DEFAULT,
        policy.getClass());
  }
}
