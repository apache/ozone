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

import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

/**
 * Base class for metrics sources that use MetricsRegistry.
 * Eliminates the need for each metrics class to declare its own registry field.
 */
public abstract class BaseMetricsSource implements MetricsSource {

  /**
   * MetricsRegistry for creating and managing metrics.
   * Protected so subclasses can access it directly.
   */
  @SuppressWarnings("PMD.SingularField")
  protected MetricsRegistry registry;
}

