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

package org.apache.hadoop.hdds.scm.ha;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.utils.db.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements methods to save and read configurations of a
 * stateful service from DB.
 */
public final class StatefulServiceStateManagerImpl
    implements StatefulServiceStateManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(StatefulServiceStateManagerImpl.class);

  // this table maps the service name to the configuration (ByteString)
  private Table<String, ByteString> statefulServiceConfig;
  private final DBTransactionBuffer transactionBuffer;

  private StatefulServiceStateManagerImpl(
      Table<String, ByteString> statefulServiceConfig,
      DBTransactionBuffer scmDBTransactionBuffer) {
    this.statefulServiceConfig = statefulServiceConfig;
    this.transactionBuffer = scmDBTransactionBuffer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void saveConfiguration(String serviceName, ByteString bytes)
      throws IOException {
    transactionBuffer.addToBuffer(statefulServiceConfig, serviceName, bytes);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Added specified bytes to the transaction buffer for key " +
          "{} to table {}", serviceName, statefulServiceConfig.getName());
    }

    if (transactionBuffer instanceof SCMHADBTransactionBuffer) {
      SCMHADBTransactionBuffer buffer =
              (SCMHADBTransactionBuffer) transactionBuffer;
      buffer.flush();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Transaction buffer flushed");
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ByteString readConfiguration(String serviceName) throws IOException {
    return statefulServiceConfig.get(serviceName);
  }

  /**
   * Deletes the persisted configuration mapped to the specified serviceName.
   * @param serviceName name of the {@link StatefulService}, obtained through
   * {@link SCMService#getServiceName()}
   * @throws IOException on failure
   */
  @Override
  public void deleteConfiguration(String serviceName) throws IOException {
    statefulServiceConfig.delete(serviceName);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reinitialize(Table<String, ByteString> configs) {
    this.statefulServiceConfig = configs;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for StatefulServiceStateManager.
   */
  public static class Builder {
    private Table<String, ByteString> statefulServiceConfig;
    private DBTransactionBuffer transactionBuffer;
    private SCMRatisServer scmRatisServer;

    public Builder setStatefulServiceConfig(
        final Table<String, ByteString> statefulServiceConfig) {
      this.statefulServiceConfig = statefulServiceConfig;
      return this;
    }

    public Builder setSCMDBTransactionBuffer(
        final DBTransactionBuffer dbTransactionBuffer) {
      this.transactionBuffer = dbTransactionBuffer;
      return this;
    }

    public Builder setRatisServer(final SCMRatisServer ratisServer) {
      scmRatisServer = ratisServer;
      return this;
    }

    public StatefulServiceStateManager build() {
      Objects.requireNonNull(statefulServiceConfig, "statefulServiceConfig == null");
      Objects.requireNonNull(transactionBuffer, "transactionBuffer == null");

      final StatefulServiceStateManager stateManager =
          new StatefulServiceStateManagerImpl(statefulServiceConfig,
              transactionBuffer);

      return scmRatisServer.getProxyHandler(RequestType.STATEFUL_SERVICE_CONFIG,
          StatefulServiceStateManager.class, stateManager);
    }
  }
}
