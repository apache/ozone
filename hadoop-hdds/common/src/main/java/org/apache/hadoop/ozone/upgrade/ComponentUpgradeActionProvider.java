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

package org.apache.hadoop.ozone.upgrade;

import java.util.Map;
import org.apache.hadoop.hdds.ComponentVersion;

/**
 * Supplies upgrade actions keyed by {@link ComponentVersion}. Implementations typically perform classpath scanning or
 * return a fixed map for tests. The component version manager decides when each action is invoked.
 *
 * @param <A> concrete upgrade action type (for example OM-specific or HDDS-specific)
 */
@FunctionalInterface
public interface ComponentUpgradeActionProvider<A> {

  /**
   * Returns all upgrade actions from this provider, keyed by component version.
   * <p>
   * Implementations must return a newly allocated map on each call; the caller may retain and use it directly.
   */
  Map<ComponentVersion, A> load();
}
