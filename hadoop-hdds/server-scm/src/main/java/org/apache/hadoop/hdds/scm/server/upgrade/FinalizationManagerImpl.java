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

import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.utils.db.Table;

/**
 * Class to initiate SCM finalization and query its progress.
 */
public final class FinalizationManagerImpl implements FinalizationManager {
  private final FinalizationStateManager finalizationStateManager;

  private FinalizationManagerImpl(Builder builder) throws IOException {
    this.finalizationStateManager = new FinalizationStateManagerImpl.Builder()
        .setFinalizationStore(builder.finalizationStore)
        .setTransactionBuffer(builder.scmHAManager.getDBTransactionBuffer())
        .setRatisServer(builder.scmHAManager.getRatisServer())
        .setVersionManager(builder.versionManager)
        .build();
  }

  @Override
  public void finalizeUpgrade() throws IOException {
    finalizationStateManager.finalizeUpgrade();
  }

  @Override
  public void reinitialize(Table<String, String> finalizationStore) throws IOException {
    finalizationStateManager.reinitialize(finalizationStore);
  }

  /**
   * Builds a {@link FinalizationManagerImpl}.
   */
  public static class Builder {
    private Table<String, String> finalizationStore;
    private SCMHAManager scmHAManager;
    private ScmVersionManager versionManager;

    public Builder() {
    }

    public Builder setVersionManager(ScmVersionManager versionManager) {
      this.versionManager = versionManager;
      return this;
    }

    public Builder setHAManager(SCMHAManager haManager) {
      this.scmHAManager = haManager;
      return this;
    }

    public Builder setFinalizationStore(
        Table<String, String> finalizationStore) {
      this.finalizationStore = finalizationStore;
      return this;
    }

    public FinalizationManagerImpl build() throws IOException {
      Objects.requireNonNull(scmHAManager, "scmHAManager == null");
      Objects.requireNonNull(finalizationStore, "finalizationStore == null");
      Objects.requireNonNull(versionManager, "versionManager == null");

      return new FinalizationManagerImpl(this);
    }
  }
}
