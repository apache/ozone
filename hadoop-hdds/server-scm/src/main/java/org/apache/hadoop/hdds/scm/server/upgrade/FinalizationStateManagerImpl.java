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
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the state of finalization in SCM.
 */
public class FinalizationStateManagerImpl implements FinalizationStateManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(FinalizationStateManagerImpl.class);

  private Table<String, String> finalizationStore;
  private final DBTransactionBuffer transactionBuffer;
  private final ScmVersionManager versionManager;

  protected FinalizationStateManagerImpl(Builder builder) {
    this.finalizationStore = builder.finalizationStore;
    this.transactionBuffer = builder.transactionBuffer;
    this.versionManager = builder.versionManager;
  }

  @Override
  public void finalizeUpgrade() throws IOException {
    versionManager.finalizeUpgrade();
    transactionBuffer.addToBuffer(finalizationStore,
        OzoneConsts.APPARENT_VERSION_KEY, String.valueOf(versionManager.getApparentVersion().serialize()));
  }

  /**
   * Called on snapshot installation, which is coordinated by Ratis.
   */
  @Override
  public void reinitialize(Table<String, String> newFinalizationStore) throws IOException {
    try {
      this.finalizationStore = newFinalizationStore;
      versionManager.finalizeFromSnapshotIfRequired(finalizationStore);
    } catch (Exception ex) {
      LOG.error("Failed to reinitialize finalization state", ex);
      throw new IOException(ex);
    }
  }

  /**
   * Builds a {@link FinalizationManagerImpl}.
   */
  public static class Builder {
    private Table<String, String> finalizationStore;
    private DBTransactionBuffer transactionBuffer;
    private SCMRatisServer ratisServer;
    private ScmVersionManager versionManager;

    public Builder() {
    }

    public Builder setRatisServer(SCMRatisServer ratisServer) {
      this.ratisServer = ratisServer;
      return this;
    }

    public Builder setVersionManager(ScmVersionManager versionManager) {
      this.versionManager = versionManager;
      return this;
    }

    public Builder setFinalizationStore(
        Table<String, String> finalizationStore) {
      this.finalizationStore = finalizationStore;
      return this;
    }

    public Builder setTransactionBuffer(DBTransactionBuffer transactionBuffer) {
      this.transactionBuffer = transactionBuffer;
      return this;
    }

    public FinalizationStateManager build() throws IOException {
      Objects.requireNonNull(finalizationStore, "finalizationStore == null");
      Objects.requireNonNull(transactionBuffer, "transactionBuffer == null");
      Objects.requireNonNull(versionManager, "versionManager == null");

      return ratisServer.getProxyHandler(FinalizationStateManager.class, new FinalizationStateManagerImpl(this));
    }
  }
}
