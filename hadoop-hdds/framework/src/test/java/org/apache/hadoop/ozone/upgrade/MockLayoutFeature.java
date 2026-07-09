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

/**
 * Mock Layout Feature list.
 */
enum MockLayoutFeature implements LayoutFeature {
  VERSION_1(1),
  VERSION_2(2),
  VERSION_3(3);

  private int layoutVersion;
  private UpgradeAction action;

  MockLayoutFeature(final int layoutVersion) {
    this.layoutVersion = layoutVersion;
  }

  @Override
  public int layoutVersion() {
    return layoutVersion;
  }

  @Override
  public String description() {
    return null;
  }

  public void addAction(UpgradeAction upgradeAction) {
    this.action = upgradeAction;
  }

  @Override
  public Optional<? extends UpgradeAction> action() {
    return Optional.ofNullable(action);
  }
}
