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
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.google.common.util.concurrent.Striped;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.jupiter.api.Test;

/**
 * Test cases for {@link SimpleStriped}.
 */
public class TestSimpleStriped {
  @Test
  void testReadWriteLocks() {
    testReadWriteLocks(true);
    testReadWriteLocks(false);
  }

  private void testReadWriteLocks(boolean fair) {
    Striped<ReadWriteLock> striped = SimpleStriped.readWriteLock(128,
        fair);
    assertEquals(128, striped.size());
    ReadWriteLock lock = striped.get("key1");
    assertEquals(fair, ((ReentrantReadWriteLock) lock).isFair());

    // Ensure same key return same lock.
    assertEquals(lock, striped.get("key1"));

    // And different key (probably) return a different lock/
    assertNotEquals(lock, striped.get("key2"));
  }

  @Test
  void testCustomStripes() {
    int size = 128;
    Striped<Lock> striped = SimpleStriped.custom(size,
        ReentrantLock::new);
    assertEquals(128, striped.size());
    Lock lock = striped.get("key1");
    // Ensure same key return same lock.
    assertEquals(lock, striped.get("key1"));
    // And different key (probably) return a different lock/
    assertNotEquals(lock, striped.get("key2"));
  }
}
