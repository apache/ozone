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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;

/**
 * Provided to methods in the {@link SCMUpgradeFinalizer} to supply objects
 * needed to operate.
 */
public final class SCMUpgradeFinalizationContext {
  private final PipelineManager pipelineManager;
  private final NodeManager nodeManager;
  private final FinalizationStateManager finalizationStateManager;
  private final SCMStorageConfig storage;
  private final HDDSLayoutVersionManager versionManager;
  private final OzoneConfiguration conf;
  private final SCMContext scmContext;

  private SCMUpgradeFinalizationContext(Builder builder) {
    pipelineManager = builder.pipelineManager;
    nodeManager = builder.nodeManager;
    finalizationStateManager = builder.finalizationStateManager;
    storage = builder.storage;
    versionManager = builder.versionManager;
    conf = builder.conf;
    scmContext = builder.scmContext;
  }

  public NodeManager getNodeManager() {
    return nodeManager;
  }

  public PipelineManager getPipelineManager() {
    return pipelineManager;
  }

  public FinalizationStateManager getFinalizationStateManager() {
    return finalizationStateManager;
  }

  public OzoneConfiguration getConfiguration() {
    return conf;
  }

  public HDDSLayoutVersionManager getLayoutVersionManager() {
    return versionManager;
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
    private PipelineManager pipelineManager;
    private NodeManager nodeManager;
    private FinalizationStateManager finalizationStateManager;
    private SCMStorageConfig storage;
    private HDDSLayoutVersionManager versionManager;
    private OzoneConfiguration conf;
    private SCMContext scmContext;

    public Builder() {
    }

    public Builder setSCMContext(SCMContext context) {
      this.scmContext = context;
      return this;
    }

    public Builder setPipelineManager(PipelineManager pipelineManager) {
      this.pipelineManager = pipelineManager;
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

    public Builder setLayoutVersionManager(
        HDDSLayoutVersionManager layoutVersionManager) {
      this.versionManager = layoutVersionManager;
      return this;
    }

    public Builder setConfiguration(OzoneConfiguration configuration) {
      this.conf = configuration;
      return this;
    }

    public SCMUpgradeFinalizationContext build() {
      Objects.requireNonNull(scmContext, "scmContext == null");
      Objects.requireNonNull(pipelineManager, "pipelineManager == null");
      Objects.requireNonNull(nodeManager, "nodeManager == null");
      Objects.requireNonNull(storage, "storage == null");
      Objects.requireNonNull(versionManager, "versionManager == null");
      Objects.requireNonNull(conf, "conf == null");
      Objects.requireNonNull(finalizationStateManager, "finalizationStateManager == null");
      return new SCMUpgradeFinalizationContext(this);
    }
  }
}
