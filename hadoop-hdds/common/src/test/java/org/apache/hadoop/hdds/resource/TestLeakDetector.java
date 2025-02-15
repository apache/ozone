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

package org.apache.hadoop.hdds.resource;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.utils.LeakDetector;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.junit.jupiter.api.Test;

/**
 * Test LeakDetector.
 */
class TestLeakDetector {
  private static final LeakDetector LEAK_DETECTOR = new LeakDetector("test");
  private final AtomicInteger leaks = new AtomicInteger(0);

  @Test
  void testNoLeaks() throws Exception {
    // create and close resource => no leaks.
    createResource(true);
    System.gc();
    Thread.sleep(100);
    assertEquals(0, leaks.get());
  }

  @Test
  void testLeaks() throws Exception {
    // create and not close => leaks.
    createResource(false);
    System.gc();
    Thread.sleep(100);
    assertEquals(1, leaks.get());
  }

  private void createResource(boolean close) {
    MyResource resource = new MyResource(leaks);
    if (close) {
      resource.close();
    }
  }

  private static final class MyResource implements AutoCloseable {
    private final UncheckedAutoCloseable leakTracker;

    private MyResource(final AtomicInteger leaks) {
      leakTracker = LEAK_DETECTOR.track(this, () -> leaks.incrementAndGet());
    }

    @Override
    public void close() {
      leakTracker.close();
    }
  }
}
