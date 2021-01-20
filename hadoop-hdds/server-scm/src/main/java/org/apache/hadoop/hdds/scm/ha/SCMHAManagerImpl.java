/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * SCMHAManagerImpl uses Apache Ratis for HA implementation. We will have 2N+1
 * node Ratis ring. The Ratis ring will have one Leader node and 2N follower
 * nodes.
 *
 * TODO
 *
 */
public class SCMHAManagerImpl implements SCMHAManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMHAManagerImpl.class);

  private final SCMRatisServer ratisServer;
  private final ConfigurationSource conf;
  private final SCMDBTransactionBuffer transactionBuffer;

  /**
   * Creates SCMHAManager instance.
   */
  public SCMHAManagerImpl(final ConfigurationSource conf,
      final StorageContainerManager scm) throws IOException {
    this.conf = conf;
    this.transactionBuffer =
        new SCMDBTransactionBuffer(scm.getScmMetadataStore());
    this.ratisServer = new SCMRatisServerImpl(
        conf.getObject(SCMHAConfiguration.class), conf, scm, transactionBuffer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void start() throws IOException {
    ratisServer.start();
  }

  public SCMRatisServer getRatisServer() {
    return ratisServer;
  }

  @Override
  public DBTransactionBuffer getDBTransactionBuffer() {
    return transactionBuffer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void shutdown() throws IOException {
    ratisServer.stop();
  }
}
