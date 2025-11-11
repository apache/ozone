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

package org.apache.hadoop.ozone.recon.tasks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for OM Update Event Buffer functionality.
 */
public class TestOMUpdateEventBuffer {

  private OMUpdateEventBuffer eventBuffer;
  private static final int TEST_CAPACITY = 100;

  @BeforeEach
  void setUp() {
    eventBuffer = new OMUpdateEventBuffer(TEST_CAPACITY, null);
  }

  @Test
  void testOfferAndPoll() {
    assertEquals(0, eventBuffer.getQueueSize());

    // Create test event batch
    List<OMDBUpdateEvent> events = new ArrayList<>();
    events.add(createTestEvent("test1"));
    events.add(createTestEvent("test2"));
    OMUpdateEventBatch batch = new OMUpdateEventBatch(events, 1);

    // Offer event batch
    assertTrue(eventBuffer.offer(batch));
    assertEquals(1, eventBuffer.getQueueSize());

    // Poll event batch
    ReconEvent polled = eventBuffer.poll(100);
    assertEquals(batch, polled);
    assertEquals(0, eventBuffer.getQueueSize());
  }

  @Test
  void testCapacityLimits() {
    // Fill buffer to capacity
    for (int i = 0; i < TEST_CAPACITY; i++) {
      List<OMDBUpdateEvent> events = new ArrayList<>();
      events.add(createTestEvent("test" + i));
      OMUpdateEventBatch batch = new OMUpdateEventBatch(events, i);
      assertTrue(eventBuffer.offer(batch));
    }
    
    assertEquals(TEST_CAPACITY, eventBuffer.getQueueSize());
    assertEquals(0, eventBuffer.getDroppedBatches());
    
    // Try to add one more - should be dropped
    List<OMDBUpdateEvent> events = new ArrayList<>();
    events.add(createTestEvent("overflow"));
    OMUpdateEventBatch batch = new OMUpdateEventBatch(events, TEST_CAPACITY);
    assertFalse(eventBuffer.offer(batch));
    assertEquals(1, eventBuffer.getDroppedBatches());
  }

  @Test
  void testPollTimeout() {
    // Poll from empty buffer should return null after timeout
    assertNull(eventBuffer.poll(10)); // 10ms timeout
  }
  
  @Test
  void testCustomReInitializationEvent() {
    // Test that custom reinitialization events can be buffered and polled
    // Mock the checkpointed metadata manager required by the constructor
    org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager mockCheckpointedManager = 
        org.mockito.Mockito.mock(org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager.class);

    ReconTaskReInitializationEvent reinitEvent =
        new ReconTaskReInitializationEvent(ReconTaskReInitializationEvent.ReInitializationReason.BUFFER_OVERFLOW,
            mockCheckpointedManager);
    
    assertTrue(eventBuffer.offer(reinitEvent));
    assertEquals(1, eventBuffer.getQueueSize());
    
    ReconEvent polled = eventBuffer.poll(100);
    assertEquals(reinitEvent, polled);
    assertEquals(ReconEvent.EventType.TASK_REINITIALIZATION, polled.getEventType());
    assertEquals(1, polled.getEventCount());
    assertEquals(0, eventBuffer.getQueueSize());
    
    // Verify the checkpointed manager is accessible
    ReconTaskReInitializationEvent reinitEventCast = (ReconTaskReInitializationEvent) polled;
    assertEquals(mockCheckpointedManager, reinitEventCast.getCheckpointedOMMetadataManager());
  }

  @Test
  void testClear() {
    // Add some events
    List<OMDBUpdateEvent> events = new ArrayList<>();
    events.add(createTestEvent("test"));
    OMUpdateEventBatch batch = new OMUpdateEventBatch(events, 1);
    eventBuffer.offer(batch);
    
    assertEquals(1, eventBuffer.getQueueSize());
    
    // Clear buffer
    eventBuffer.clear();
    assertEquals(0, eventBuffer.getQueueSize());
    // Note: droppedBatches is not reset by clear() to maintain overflow detection
  }

  @Test
  void testResetDroppedBatches() {
    // Fill buffer to capacity and trigger overflow
    for (int i = 0; i <= TEST_CAPACITY; i++) {
      List<OMDBUpdateEvent> events = new ArrayList<>();
      events.add(createTestEvent("test" + i));
      OMUpdateEventBatch batch = new OMUpdateEventBatch(events, i);
      eventBuffer.offer(batch);
    }
    
    // Should have dropped batches
    assertTrue(eventBuffer.getDroppedBatches() > 0);
    
    // Reset dropped batches
    eventBuffer.resetDroppedBatches();
    assertEquals(0, eventBuffer.getDroppedBatches());
  }

  @Test 
  void testClearPreservesDroppedBatches() {
    // Fill buffer to capacity and trigger overflow
    for (int i = 0; i <= TEST_CAPACITY; i++) {
      List<OMDBUpdateEvent> events = new ArrayList<>();
      events.add(createTestEvent("test" + i));
      OMUpdateEventBatch batch = new OMUpdateEventBatch(events, i);
      eventBuffer.offer(batch);
    }
    
    long droppedBefore = eventBuffer.getDroppedBatches();
    assertTrue(droppedBefore > 0);
    
    // Clear should not reset dropped batches counter
    eventBuffer.clear();
    assertEquals(0, eventBuffer.getQueueSize());
    assertEquals(droppedBefore, eventBuffer.getDroppedBatches());
  }

  private OMDBUpdateEvent createTestEvent(String key) {
    return new OMDBUpdateEvent.OMUpdateEventBuilder<>()
        .setKey(key)
        .setValue("value_" + key)
        .setTable("testTable")
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
        .build();
  }
}
