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

package org.apache.hadoop.hdds.scm.server.upgrade;

import java.util.Objects;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;

/**
 * Provided to methods in the {@link SCMUpgradeFinalizer} to supply objects
 * needed to operate.
 */
public final class SCMUpgradeFinalizationContext {
  private final NodeManager nodeManager;
  private final FinalizationStateManager finalizationStateManager;
  private final SCMStorageConfig storage;
  private final SCMContext scmContext;

  private SCMUpgradeFinalizationContext(Builder builder) {
    nodeManager = builder.nodeManager;
    finalizationStateManager = builder.finalizationStateManager;
    storage = builder.storage;
    scmContext = builder.scmContext;
  }

  public NodeManager getNodeManager() {
    return nodeManager;
  }

  public FinalizationStateManager getFinalizationStateManager() {
    return finalizationStateManager;
  }

  public SCMContext getSCMContext() {
    return scmContext;
  }

  public SCMStorageConfig getStorage() {
    return storage;
  }

  /**
   * Builds an {@link SCMUpgradeFinalizationContext}.
   */
  public static final class Builder {
    private NodeManager nodeManager;
    private FinalizationStateManager finalizationStateManager;
    private SCMStorageConfig storage;
    private SCMContext scmContext;

    public Builder() {
    }

    public Builder setSCMContext(SCMContext context) {
      this.scmContext = context;
      return this;
    }

    public Builder setNodeManager(NodeManager nodeManager) {
      this.nodeManager = nodeManager;
      return this;
    }

    public Builder setFinalizationStateManager(
        FinalizationStateManager finalizationStateManager) {
      this.finalizationStateManager = finalizationStateManager;
      return this;
    }

    public Builder setStorage(SCMStorageConfig storage) {
      this.storage = storage;
      return this;
    }

    public SCMUpgradeFinalizationContext build() {
      Objects.requireNonNull(scmContext, "scmContext == null");
      Objects.requireNonNull(nodeManager, "nodeManager == null");
      Objects.requireNonNull(storage, "storage == null");
      Objects.requireNonNull(finalizationStateManager, "finalizationStateManager == null");
      return new SCMUpgradeFinalizationContext(this);
    }
  }
}
