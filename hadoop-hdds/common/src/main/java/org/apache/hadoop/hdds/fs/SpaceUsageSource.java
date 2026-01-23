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

package org.apache.hadoop.hdds.fs;

import java.io.UncheckedIOException;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;

/**
 * Interface for implementations that can tell how much space
 * is used in a directory.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface SpaceUsageSource {
  Fixed UNKNOWN = new Fixed(0, 0, 0);

  /**
   * @return space usage in bytes
   * @throws UncheckedIOException if I/O exception occurs while calculating
   * space use info
   */
  long getUsedSpace();

  long getCapacity();

  long getAvailable();

  default Fixed snapshot() {
    return new Fixed(getCapacity(), getAvailable(), getUsedSpace());
  }

  /**
   * A static source of space usage.  Can be a point in time snapshot of a
   * real volume usage, or can be used for testing.
   */
  final class Fixed implements SpaceUsageSource {

    private final long capacity;
    private final long available;
    private final long used;

    public Fixed(long capacity, long available, long used) {
      this.capacity = capacity;
      this.available = Math.max(Math.min(available, capacity - used), 0);
      this.used = used;
    }

    @Override
    public long getCapacity() {
      return capacity;
    }

    @Override
    public long getAvailable() {
      return available;
    }

    @Override
    public long getUsedSpace() {
      return used;
    }

    @Override
    public Fixed snapshot() {
      return this; // immutable
    }

    @Override
    public String toString() {
      return "capacity=" + capacity + ", used=" + used + ", available=" + available;
    }
  }
}
