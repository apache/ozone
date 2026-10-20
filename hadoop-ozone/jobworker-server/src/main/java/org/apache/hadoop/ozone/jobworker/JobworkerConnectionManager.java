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

package org.apache.hadoop.ozone.jobworker;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.jobworker.protocol.JobworkerProtocol;
import org.apache.hadoop.ozone.jobworker.protocolPB.JobworkerProtocolClientSideTranslatorPB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobworkerConnectionManager manages connections between Jobworker and
 * OzoneManager (OM). It supports connections to multiple OM groups.
 */
public class JobworkerConnectionManager implements Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(JobworkerConnectionManager.class);

  private final ReadWriteLock mapLock;
  private final Map<InetSocketAddress, JobworkerEndpointStateMachine> omMachines;
  private final ConfigurationSource conf;
  private final OzoneConfiguration ozoneConf;

  public JobworkerConnectionManager(ConfigurationSource conf) {
    this.mapLock = new ReentrantReadWriteLock();
    this.omMachines = new HashMap<>();
    this.conf = conf;
    this.ozoneConf = (OzoneConfiguration) conf;
  }

  /**
   * Returns Config.
   *
   * @return configuration
   */
  public ConfigurationSource getConf() {
    return conf;
  }

  /**
   * Takes a read lock.
   */
  public void readLock() {
    this.mapLock.readLock().lock();
  }

  /**
   * Releases the read lock.
   */
  public void readUnlock() {
    this.mapLock.readLock().unlock();
  }

  /**
   * Takes the write lock.
   */
  public void writeLock() {
    this.mapLock.writeLock().lock();
  }

  /**
   * Releases the write lock.
   */
  public void writeUnlock() {
    this.mapLock.writeLock().unlock();
  }

  /**
   * Adds a new OM endpoint to the target set.
   *
   * @param omAddress        - Address of the OM machine
   * @param threadNamePrefix - Prefix for thread names
   * @param configuredOmServiceId  - OM service ID in the current jobworker configuration
   * @throws IOException if error occurs
   */
  public void addOMEndpoint(InetSocketAddress omAddress, String threadNamePrefix,
                            String configuredOmServiceId) throws IOException {
    writeLock();
    try {
      if (omMachines.containsKey(omAddress)) {
        throw new IllegalArgumentException("Trying to add an existing OM machine: " + omAddress);
      }

      JobworkerProtocol rpcClient =
          new JobworkerProtocolClientSideTranslatorPB(omAddress.getHostName(), omAddress.getPort(), ozoneConf);
      JobworkerEndpointStateMachine endpoint = new JobworkerEndpointStateMachine(
          omAddress, rpcClient, this.conf, threadNamePrefix, configuredOmServiceId);
      omMachines.put(omAddress, endpoint);
      LOG.info("Added OM endpoint at {}", omAddress);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Returns all known endpoints.
   *
   * @return Collection of endpoints
   */
  public Collection<JobworkerEndpointStateMachine> getAllEndpoints() {
    readLock();
    try {
      return new ArrayList<>(omMachines.values());
    } finally {
      readUnlock();
    }
  }

  @Override
  public void close() throws IOException {
    for (JobworkerEndpointStateMachine endpoint : getAllEndpoints()) {
      try {
        endpoint.close();
      } catch (Exception e) {
        LOG.error("Error closing endpoint {}", endpoint.getAddress(), e);
      }
    }
  }
}
