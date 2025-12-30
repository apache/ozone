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

package org.apache.hadoop.hdds.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SlidingWindow} class.
 */
class TestSlidingWindow {

  private TestClock testClock;

  @BeforeEach
  void setup() {
    testClock = TestClock.newInstance();
  }
  
  @Test
  void testConstructorValidation() {
    // Test invalid window size
    assertThrows(IllegalArgumentException.class, () -> new SlidingWindow(-1, Duration.ofMillis(100)));

    // Test invalid expiry duration
    assertThrows(IllegalArgumentException.class, () -> new SlidingWindow(1, Duration.ofMillis(0)));
    assertThrows(IllegalArgumentException.class, () -> new SlidingWindow(1, Duration.ofMillis(-1)));
  }

  @Test
  void testAdd() {
    SlidingWindow slidingWindow = new SlidingWindow(3, Duration.ofSeconds(5), testClock);
    for (int i = 0; i < slidingWindow.getWindowSize(); i++) {
      slidingWindow.add();
      assertEquals(i + 1, slidingWindow.getNumEvents());
      assertFalse(slidingWindow.isExceeded());
    }

    slidingWindow.add();
    assertEquals(slidingWindow.getWindowSize() + 1, slidingWindow.getNumEvents());
    assertTrue(slidingWindow.isExceeded());
  }

  @Test
  void testEventExpiration() {
    SlidingWindow slidingWindow = new SlidingWindow(2, Duration.ofMillis(500), testClock);

    // Add events to reach threshold
    slidingWindow.add();
    slidingWindow.add();
    slidingWindow.add();
    assertEquals(3, slidingWindow.getNumEvents());
    assertTrue(slidingWindow.isExceeded());

    // Fast forward time to expire events
    testClock.fastForward(600);

    assertEquals(0, slidingWindow.getNumEvents());
    assertFalse(slidingWindow.isExceeded());

    // Add one more event - should not be enough to mark as full
    slidingWindow.add();
    assertEquals(1, slidingWindow.getNumEvents());
    assertFalse(slidingWindow.isExceeded());
  }

  @Test
  void testPartialExpiration() {
    SlidingWindow slidingWindow = new SlidingWindow(3, Duration.ofSeconds(1), testClock);

    slidingWindow.add();
    slidingWindow.add();
    slidingWindow.add();
    slidingWindow.add();
    assertEquals(4, slidingWindow.getNumEvents());
    assertTrue(slidingWindow.isExceeded());

    testClock.fastForward(600);
    slidingWindow.add(); // this will remove the oldest event as the window is full
    assertEquals(4, slidingWindow.getNumEvents());

    // Fast forward time to expire the oldest events
    testClock.fastForward(500);
    assertEquals(1, slidingWindow.getNumEvents());
    assertFalse(slidingWindow.isExceeded());
  }

  @Test
  void testZeroWindowSize() {
    SlidingWindow slidingWindow = new SlidingWindow(0, Duration.ofSeconds(5), testClock);
    
    // Verify initial state
    assertEquals(0, slidingWindow.getWindowSize());
    assertEquals(0, slidingWindow.getNumEvents());
    assertFalse(slidingWindow.isExceeded());
    
    // Add an event - with window size 0, any event should cause isExceeded to return true
    slidingWindow.add();
    assertEquals(1, slidingWindow.getNumEvents());
    assertTrue(slidingWindow.isExceeded());
    
    // Add another event - should replace the previous one as window is exceeded
    slidingWindow.add();
    assertEquals(1, slidingWindow.getNumEvents());
    assertTrue(slidingWindow.isExceeded());
    
    // Test expiration
    testClock.fastForward(6000); // Move past expiry time
    assertEquals(0, slidingWindow.getNumEvents());
    assertFalse(slidingWindow.isExceeded());
    
    // Add multiple events in sequence - should always keep only the latest one
    for (int i = 0; i < 5; i++) {
      slidingWindow.add();
      assertEquals(1, slidingWindow.getNumEvents());
      assertTrue(slidingWindow.isExceeded());
    }
  }
}
