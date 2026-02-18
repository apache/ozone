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

import java.util.Optional;
import org.apache.hadoop.ozone.Versioned;

/**
 * Generic Layout feature interface for Ozone.
 */
public interface LayoutFeature extends Versioned {
  String name();

  int layoutVersion();

  String description();

  default Optional<? extends UpgradeAction> action() {
    return Optional.empty();
  }

  /**
   * Generic UpgradeAction interface. An upgrade action is an operation that
   * is run at least once as a pre-requisite to finalizing a layout feature.
   * @param <T>
   */
  interface UpgradeAction<T> {

    default String name() {
      return getClass().getSimpleName();
    }

    void execute(T arg) throws Exception;
  }

  @Override
  default int version() {
    return this.layoutVersion();
  }
}
