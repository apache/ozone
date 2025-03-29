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

package com.google.common.util.concurrent;

import java.util.Comparator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Locks obtained from {@link Striped}. */
public class StripedLock<K> {
  private static final Comparator<StripedLock<?>> COMPARATOR = Comparator.comparingInt(StripedLock::getIndex);

  public static Comparator<StripedLock<?>> getComparator() {
    return COMPARATOR;
  }

  private final K stripeKey;
  private final ReentrantReadWriteLock lock;
  /** Stripe index. */
  private final int index;

  StripedLock(K stripeKey, ReentrantReadWriteLock lock, int index) {
    this.stripeKey = stripeKey;
    this.lock = lock;
    this.index = index;
  }

  public K getStripeKey() {
    return stripeKey;
  }

  public ReentrantReadWriteLock getLock() {
    return lock;
  }

  /** @return the stripe index. */
  public int getIndex() {
    return index;
  }
}
