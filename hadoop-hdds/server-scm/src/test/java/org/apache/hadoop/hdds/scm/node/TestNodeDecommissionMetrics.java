/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.SimpleMockNodeManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManagerMetrics;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;

/**
 * Tests for the NodeDecommissionMetrics class.
 */
public class TestNodeDecommissionMetrics {
    private NodeDecommissionManager nodeDecommissionManager;
    private NodeDecommissionMetrics metrics;

    private SimpleMockNodeManager nodeManager;
    private OzoneConfiguration conf;
    private DatanodeAdminMonitorImpl monitor;
    private DatanodeAdminHandler startAdminHandler;
    private ReplicationManager repManager;
    private EventQueue eventQueue;


    @BeforeEach
    public void setup() {

        conf = new OzoneConfiguration();
        eventQueue = new EventQueue();
        startAdminHandler = new DatanodeAdminHandler();
        eventQueue.addHandler(SCMEvents.START_ADMIN_ON_NODE, startAdminHandler);
        nodeManager = new SimpleMockNodeManager();
        repManager = Mockito.mock(ReplicationManager.class);
        monitor =
                new DatanodeAdminMonitorImpl(conf, eventQueue, nodeManager, repManager);

        nodeDecommissionManager = Mockito.mock(NodeDecommissionManager.class);
        metrics = NodeDecommissionMetrics.create(nodeDecommissionManager);
        monitor.setMetrics(metrics);
    }

    @AfterEach
    public void after() {
        metrics.unRegister();
    }

    @Test
    public void testDecommMonitorCollectTrackedNodes() {
        DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
        nodeManager.register(dn1,
                new NodeStatus(ENTERING_MAINTENANCE,
                        HddsProtos.NodeState.HEALTHY));
        monitor.startMonitoring(dn1);
        monitor.run();
        Assertions.assertEquals(1,
                metrics.getTotalTrackedDecommissioningMaintenanceNodes());
    }

    private static class DatanodeAdminHandler implements
            EventHandler<DatanodeDetails> {

        private AtomicInteger invocation = new AtomicInteger(0);

        @Override
        public void onMessage(final DatanodeDetails dn,
                              final EventPublisher publisher) {
            invocation.incrementAndGet();
        }

        public int getInvocation() {
            return invocation.get();
        }
    }
}
