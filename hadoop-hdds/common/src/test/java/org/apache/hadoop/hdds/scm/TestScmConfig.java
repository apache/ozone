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

package org.apache.hadoop.hdds.scm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ScmConfig}.
 */
class TestScmConfig {

  @Test
  void testPipelineExcludedNodesDefaultsToEmpty() {
    OzoneConfiguration conf = new OzoneConfiguration();
    ScmConfig scmConfig = conf.getObject(ScmConfig.class);

    assertTrue(scmConfig.getPipelineExcludedNodes().isEmpty());
  }

  @Test
  void testPipelineExcludedNodesParsesUuidHostnameAndIp() {
    String uuid = UUID.randomUUID().toString();
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("hdds.scm.pipeline.exclude.datanodes",
        uuid + ", DN-1.EXAMPLE.COM, 10.0.0.12, dn-1.example.com");
    ScmConfig scmConfig = conf.getObject(ScmConfig.class);

    PipelineExcludedNodes first = scmConfig.getPipelineExcludedNodes();
    PipelineExcludedNodes second = scmConfig.getPipelineExcludedNodes();

    assertFalse(first.isEmpty());
    assertEquals(first.getExcludedDatanodeIds(), second.getExcludedDatanodeIds());
    assertEquals(first.getExcludedAddressTokens(), second.getExcludedAddressTokens());
    assertEquals(1, first.getExcludedDatanodeIds().size());
    assertTrue(first.getExcludedDatanodeIds().contains(DatanodeID.fromUuidString(uuid)));
    assertTrue(first.getExcludedAddressTokens().contains("dn-1.example.com"));
    assertTrue(first.getExcludedAddressTokens().contains("10.0.0.12"));
    assertEquals(2, first.getExcludedAddressTokens().size());
  }

  @Test
  void testPipelineExcludedNodesMatchesDatanodeByIdAndAddress() {
    DatanodeDetails datanode = MockDatanodeDetails.randomDatanodeDetails();
    datanode.setHostName("dn-2.example.com");
    datanode.setIpAddress("10.10.10.10");

    PipelineExcludedNodes byUUID = PipelineExcludedNodes.parse(datanode.getUuidString());
    assertTrue(byUUID.isExcluded(datanode));

    PipelineExcludedNodes byHost = PipelineExcludedNodes.parse("DN-2.EXAMPLE.COM");
    assertTrue(byHost.isExcluded(datanode));

    PipelineExcludedNodes byIp = PipelineExcludedNodes.parse("10.10.10.10");
    assertTrue(byIp.isExcluded(datanode));
  }
}

