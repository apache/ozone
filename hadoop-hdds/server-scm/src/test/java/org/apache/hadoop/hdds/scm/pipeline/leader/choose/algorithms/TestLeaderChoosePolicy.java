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

package org.apache.hadoop.hdds.scm.pipeline.leader.choose.algorithms;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineStateManager;
import org.apache.hadoop.hdds.scm.pipeline.RatisPipelineProvider;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link LeaderChoosePolicy}.
 */
public class TestLeaderChoosePolicy {
  private OzoneConfiguration conf;

  private ScmConfig scmConfig;

  @Before
  public void setup() {
    //initialize network topology instance
    conf = new OzoneConfiguration();
    scmConfig = conf.getObject(ScmConfig.class);
  }

  @Test
  public void testDefaultPolicy() {
    RatisPipelineProvider ratisPipelineProvider = new RatisPipelineProvider(
        mock(NodeManager.class),
        mock(PipelineStateManager.class),
        conf,
        mock(EventPublisher.class));
    Assert.assertSame(
        ratisPipelineProvider.getLeaderChoosePolicy().getClass(),
        MinLeaderCountChoosePolicy.class);
  }

  @Test(expected = RuntimeException.class)
  public void testClassNotImplemented() {
    // set a class not implemented
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_LEADER_CHOOSING_POLICY,
        "org.apache.hadoop.hdds.scm.pipeline.leader.choose.algorithms" +
            ".HelloWorld");
    new RatisPipelineProvider(
        mock(NodeManager.class),
        mock(PipelineStateManager.class),
        conf,
        mock(EventPublisher.class));

    // expecting exception
  }
}
