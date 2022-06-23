/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.upgrade;

import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManagerImpl;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManager;

import java.io.IOException;

/**
 * An implementation of {@link FinalizationManager} that supports injecting
 * the {@link FinalizationStateManager} for testing.
 */
public class FinalizationManagerTestImpl extends FinalizationManagerImpl {

  public FinalizationManagerTestImpl(Builder builder) throws IOException {
    super(builder, builder.finalizationStateManager);
  }

  /**
   * Builds a {@link FinalizationManagerTestImpl}.
   */
  public static class Builder extends FinalizationManagerImpl.Builder {
    private FinalizationStateManager finalizationStateManager;

    public Builder setFinalizationStateManager(
        FinalizationStateManager stateManager) {
      this.finalizationStateManager = stateManager;
      return this;
    }

    @Override
    public FinalizationManagerTestImpl build() throws IOException {
      return new FinalizationManagerTestImpl(this);
    }
  }
}
