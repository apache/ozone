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

package org.apache.hadoop.hdds.scm.upgrade;

import java.io.IOException;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManagerImpl;

/**
 * An implementation of {@link FinalizationStateManager} built without an
 * invocation handler for testing.
 */
public class FinalizationStateManagerTestImpl extends
    FinalizationStateManagerImpl {
  public FinalizationStateManagerTestImpl(Builder builder) throws IOException {
    super(builder);
  }

  /**
   * Builds a {@link FinalizationStateManagerTestImpl}.
   */
  public static class Builder extends FinalizationStateManagerImpl.Builder {
    @Override
    public FinalizationStateManagerTestImpl build() throws IOException {
      return new FinalizationStateManagerTestImpl(this);
    }
  }
}
