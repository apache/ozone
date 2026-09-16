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

package org.apache.hadoop.ozone.recon.chatbot.recon;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.recon.api.BucketEndpoint;
import org.apache.hadoop.ozone.recon.api.ClusterStateEndpoint;
import org.apache.hadoop.ozone.recon.api.ContainerEndpoint;
import org.apache.hadoop.ozone.recon.api.NSSummaryEndpoint;
import org.apache.hadoop.ozone.recon.api.NodeEndpoint;
import org.apache.hadoop.ozone.recon.api.OMDBInsightEndpoint;
import org.apache.hadoop.ozone.recon.api.PipelineEndpoint;
import org.apache.hadoop.ozone.recon.api.TaskStatusService;
import org.apache.hadoop.ozone.recon.api.UtilizationEndpoint;
import org.apache.hadoop.ozone.recon.api.VolumeEndpoint;
import org.apache.hadoop.ozone.recon.chatbot.agent.LlmToolSpecFactory;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMClient.ToolSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Guards the chatbot's "single source of truth" invariant: a Recon tool must be declared in all
 * three places that reference it, or it cannot work safely. The three sources are:
 * <ul>
 *   <li>{@link LlmToolSpecFactory#getToolSpecs()} — what the LLM is told it may call,</li>
 *   <li>{@link ReconApiAllowlist} — what the agent permits before executing,</li>
 *   <li>{@link ReconEndpointRouter} — what actually dispatches to a Recon bean.</li>
 * </ul>
 *
 * <p>Adding, renaming, or removing a tool in only one or two of these places is a real,
 * easy-to-make bug (the catalog drifts silently). These tests fail fast when that happens.</p>
 */
public class TestReconToolCatalogConsistency {

  private Set<String> toolSpecNames;
  private ReconApiAllowlist allowlist;
  private ReconEndpointRouter router;

  @BeforeEach
  public void setUp() {
    toolSpecNames = new LlmToolSpecFactory().getToolSpecs().stream()
        .map(ToolSpec::getName)
        .collect(Collectors.toSet());

    allowlist = new ReconApiAllowlist();

    // Mocked beans return null Responses — routability only depends on a matching switch case,
    // not on the bean's behavior.
    router = new ReconEndpointRouter(
        mock(ClusterStateEndpoint.class), mock(NodeEndpoint.class), mock(PipelineEndpoint.class),
        mock(ContainerEndpoint.class), mock(OMDBInsightEndpoint.class), mock(VolumeEndpoint.class),
        mock(BucketEndpoint.class), mock(TaskStatusService.class), mock(UtilizationEndpoint.class),
        mock(NSSummaryEndpoint.class), allowlist);
  }

  @Test
  public void testEveryToolSpecIsAllowlisted() {
    for (String name : toolSpecNames) {
      assertTrue(allowlist.isRegistered(name),
          "Tool '" + name + "' is advertised to the LLM but missing from ReconApiAllowlist");
    }
  }

  @Test
  public void testAllowlistAndToolSpecsAreExactlyInSync() {
    // Set equality catches drift in both directions: a spec without an allowlist entry, and an
    // orphaned allowlist entry with no corresponding tool spec.
    assertEquals(allowlist.getRegisteredTools(), toolSpecNames,
        "ReconApiAllowlist and LlmToolSpecFactory tool names have drifted out of sync");
  }

  @Test
  public void testEveryToolSpecIsRoutable() {
    for (String name : toolSpecNames) {
      // A registered tool must hit a switch case; the default case throws IllegalArgumentException.
      assertDoesNotThrow(() -> router.route(name, Collections.emptyMap()),
          "Tool '" + name + "' has no in-process route in ReconEndpointRouter");
    }
  }
}
