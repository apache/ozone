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

package org.apache.hadoop.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Test class for validating the functionality of the base class BackgroundService.
 */
public class TestBackgroundService {

  @Test
  public void testBackgroundServiceRunWaitsForTasks() throws InterruptedException, TimeoutException {

    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    Map<Integer, Integer> map = new HashMap<>();
    Map<Integer, Lock> locks = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      int index = i;
      locks.put(index, new ReentrantLock());
      map.put(index, 0);
      queue.add(() -> {
        locks.get(index).lock();
        try {
          map.compute(index, (k, v) -> v == null ? 1 : (v + 1));
        } finally {
          locks.get(index).unlock();
        }
        return new BackgroundTaskResult.EmptyTaskResult();
      });
    }

    BackgroundService backgroundService = new BackgroundService("test", 100, TimeUnit.MILLISECONDS, 10, 10000) {
      @Override
      public BackgroundTaskQueue getTasks() {
        return queue;
      }
    };
    List<Lock> lockList = new ArrayList<>();
    // Acquire locks on all even indices.
    for (int i = 0; i < 10; i += 2) {
      lockList.add(locks.get(i));
      locks.get(i).lock();
    }

    backgroundService.start();
    List<Integer> values = new ArrayList<>();
    GenericTestUtils.waitFor(() -> {
      values.clear();
      for (int i = 0; i < 10; i++) {
        values.add(map.get(i));
        if (i % 2 == 1 && map.get(i) != 1) {
          return false;
        }
        if (i % 2 == 0 && map.get(i) != 0) {
          return false;
        }
      }
      return true;
    }, 100, 3000);
    Thread.sleep(3000);
    assertEquals(values, IntStream.range(0, 10).boxed().map(map::get).collect(Collectors.toList()));
    lockList.forEach(Lock::unlock);
    backgroundService.shutdown();
  }
}
