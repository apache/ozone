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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineStateManagerImpl;
import org.apache.hadoop.hdds.scm.pipeline.RatisPipelineProvider;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link LeaderChoosePolicy}.
 */
public class TestLeaderChoosePolicy {
  private OzoneConfiguration conf;

  @BeforeEach
  public void setup() {
    //initialize network topology instance
    conf = new OzoneConfiguration();
  }

  @Test
  public void testDefaultPolicy() {
    RatisPipelineProvider ratisPipelineProvider = new RatisPipelineProvider(
        mock(NodeManager.class),
        mock(PipelineStateManagerImpl.class),
        conf,
        mock(EventPublisher.class),
        SCMContext.emptyContext());
    assertSame(
        ratisPipelineProvider.getLeaderChoosePolicy().getClass(),
        MinLeaderCountChoosePolicy.class);
  }

  @Test
  public void testClassNotImplemented() {
    // set a class not implemented
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_LEADER_CHOOSING_POLICY,
        "org.apache.hadoop.hdds.scm.pipeline.leader.choose.algorithms" +
            ".HelloWorld");
    assertThrows(RuntimeException.class, () ->
        new RatisPipelineProvider(
            mock(NodeManager.class),
            mock(PipelineStateManagerImpl.class),
            conf,
            mock(EventPublisher.class),
            SCMContext.emptyContext())
    );

    // expecting exception
  }
}
