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

import java.util.OptionalLong;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;

/**
 * Interface for saving and loading space usage information.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface SpaceUsagePersistence {

  /**
   * @return an {@link OptionalLong} with the value if loaded successfully,
   * otherwise an empty one
   */
  OptionalLong load();

  /**
   * Save the space usage information got from
   * {@link SpaceUsageSource#getUsedSpace()}.
   */
  void save(SpaceUsageSource source);

  /**
   * Does not persist space usage information at all.  Use for sources that are
   * relatively cheap to use (and also for testing).
   */
  class None implements SpaceUsagePersistence {

    public static final SpaceUsagePersistence INSTANCE = new None();

    @Override
    public OptionalLong load() {
      return OptionalLong.empty();
    }

    @Override
    public void save(SpaceUsageSource source) {
      // no-op
    }
  }

}
