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

package org.apache.hadoop.ozone.om.lock;

import static com.google.common.graph.Graphs.hasCycle;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import org.junit.jupiter.api.Test;

/**
 * Test class for DAGLeveledResource.
 */
public class TestDAGLeveledResource {

  @Test
  public void ensureNoCycleInDAGLevelResource() {
    MutableGraph<DAGLeveledResource> graph = GraphBuilder.directed().build();
    for (DAGLeveledResource resource : DAGLeveledResource.values()) {
      graph.addNode(resource);
    }
    for (DAGLeveledResource resource : DAGLeveledResource.values()) {
      resource.getChildren().forEach(child -> graph.putEdge(resource, child));
    }
    assertFalse(hasCycle(graph), "DAGLeveledResource has one or more cycles and it needs to be DAG");
  }
}
