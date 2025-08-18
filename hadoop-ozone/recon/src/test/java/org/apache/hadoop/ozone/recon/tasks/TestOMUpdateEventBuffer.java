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
    eventBuffer = new OMUpdateEventBuffer(TEST_CAPACITY);
  }

  @Test
  void testBasicBuffering() {
    assertFalse(eventBuffer.isBuffering());
    assertEquals(0, eventBuffer.getQueueSize());

    eventBuffer.startBuffering();
    assertTrue(eventBuffer.isBuffering());

    // Create test event batch
    List<OMDBUpdateEvent> events = new ArrayList<>();
    events.add(createTestEvent("test1"));
    events.add(createTestEvent("test2"));
    OMUpdateEventBatch batch = new OMUpdateEventBatch(events, 1);

    // Offer event batch
    assertTrue(eventBuffer.offer(batch));
    assertEquals(1, eventBuffer.getQueueSize());
    assertEquals(2, eventBuffer.getTotalBufferedEvents());

    // Stop buffering and drain
    List<OMUpdateEventBatch> drained = eventBuffer.stopBufferingAndDrain();
    assertEquals(1, drained.size());
    assertEquals(2, drained.get(0).getEvents().size());
    assertFalse(eventBuffer.isBuffering());
    assertEquals(0, eventBuffer.getQueueSize());
  }

  @Test
  void testCapacityLimits() {
    eventBuffer.startBuffering();
    
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
  void testNearCapacity() {
    eventBuffer.startBuffering();
    
    // Fill to 94% - should not be near capacity
    int nearCapacityCount = (int)(TEST_CAPACITY * 0.94);
    for (int i = 0; i < nearCapacityCount; i++) {
      List<OMDBUpdateEvent> events = new ArrayList<>();
      events.add(createTestEvent("test" + i));
      OMUpdateEventBatch batch = new OMUpdateEventBatch(events, i);
      eventBuffer.offer(batch);
    }
    assertFalse(eventBuffer.isNearCapacity());
    
    // Fill to 95% - should be near capacity
    int capacityThreshold = (int)(TEST_CAPACITY * 0.95);
    for (int i = nearCapacityCount; i < capacityThreshold; i++) {
      List<OMDBUpdateEvent> events = new ArrayList<>();
      events.add(createTestEvent("test" + i));
      OMUpdateEventBatch batch = new OMUpdateEventBatch(events, i);
      eventBuffer.offer(batch);
    }
    assertTrue(eventBuffer.isNearCapacity());
  }

  @Test
  void testOfferWithoutBuffering() {
    // Not in buffering mode
    assertFalse(eventBuffer.isBuffering());
    
    List<OMDBUpdateEvent> events = new ArrayList<>();
    events.add(createTestEvent("test"));
    OMUpdateEventBatch batch = new OMUpdateEventBatch(events, 1);
    
    // Should return false when not buffering
    assertFalse(eventBuffer.offer(batch));
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
