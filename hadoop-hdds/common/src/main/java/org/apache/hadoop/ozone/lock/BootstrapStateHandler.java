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

package org.apache.hadoop.ozone.lock;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ratis.util.UncheckedAutoCloseable;

/** Bootstrap state handler interface. */
public interface BootstrapStateHandler {
  Lock getBootstrapStateLock();

  /** Bootstrap state handler lock implementation. Should be always acquired before opening any snapshot to avoid
   * deadlocks*/
  class Lock {
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private UncheckedAutoCloseable lock(boolean readLock) {
      java.util.concurrent.locks.Lock lock = readLock ? readWriteLock.readLock() : readWriteLock.writeLock();
      lock.lock();
      return lock::unlock;
    }

    public UncheckedAutoCloseable acquireWriteLock() throws InterruptedException {
      return lock(false);
    }

    public UncheckedAutoCloseable acquireReadLock() throws InterruptedException {
      return lock(true);
    }
  }
}
