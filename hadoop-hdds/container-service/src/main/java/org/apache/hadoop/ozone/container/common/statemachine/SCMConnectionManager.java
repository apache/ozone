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

package org.apache.hadoop.ozone.container.common.statemachine;

import static java.util.Collections.unmodifiableList;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmRpcRetryCount;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmRpcRetryInterval;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmRpcTimeOutInMilliseconds;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.management.ObjectName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine.EndPointStates;
import org.apache.hadoop.ozone.protocolPB.ReconDatanodeProtocolPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SCMConnectionManager - Acts as a class that manages the membership
 * information of the SCMs that we are working with.
 */
public class SCMConnectionManager
    implements Closeable, SCMConnectionManagerMXBean {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMConnectionManager.class);

  private final ReadWriteLock mapLock;
  private final Map<InetSocketAddress, EndpointStateMachine> scmMachines;

  private final int rpcTimeout;
  private final ConfigurationSource conf;
  private ObjectName jmxBean;

  public SCMConnectionManager(ConfigurationSource conf) {
    this.mapLock = new ReentrantReadWriteLock();
    Long timeOut = getScmRpcTimeOutInMilliseconds(conf);
    this.rpcTimeout = timeOut.intValue();
    this.scmMachines = new HashMap<>();
    this.conf = conf;
    jmxBean = MBeans.register("HddsDatanode",
        "SCMConnectionManager",
        this);
  }

  /**
   * Returns Config.
   *
   * @return ozoneConfig.
   */
  public ConfigurationSource getConf() {
    return conf;
  }

  /**
   * Get RpcTimeout.
   *
   * @return - Return RPC timeout.
   */
  public int getRpcTimeout() {
    return rpcTimeout;
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
   * adds a new SCM machine to the target set.
   *
   * @param address - Address of the SCM machine to send heartbeat to.
   * @throws IOException
   */
  public void addSCMServer(InetSocketAddress address,
      String threadNamePrefix) throws IOException {
    writeLock();
    try {
      if (scmMachines.containsKey(address)) {
        LOG.warn("Trying to add an existing SCM Machine to Machines group. " +
            "Ignoring the request.");
        return;
      }

      Configuration hadoopConfig =
          LegacyHadoopConfigurationSource.asHadoopConfiguration(this.conf);
      RPC.setProtocolEngine(
          hadoopConfig,
          StorageContainerDatanodeProtocolPB.class,
          ProtobufRpcEngine.class);
      long version =
          RPC.getProtocolVersion(StorageContainerDatanodeProtocolPB.class);

      RetryPolicy retryPolicy =
          RetryPolicies.retryUpToMaximumCountWithFixedSleep(
              getScmRpcRetryCount(conf), getScmRpcRetryInterval(conf),
              TimeUnit.MILLISECONDS);

      StorageContainerDatanodeProtocolPB rpcProxy = RPC.getProtocolProxy(
          StorageContainerDatanodeProtocolPB.class, version,
          address, UserGroupInformation.getCurrentUser(), hadoopConfig,
          NetUtils.getDefaultSocketFactory(hadoopConfig), getRpcTimeout(),
          retryPolicy).getProxy();

      StorageContainerDatanodeProtocolClientSideTranslatorPB rpcClient =
          new StorageContainerDatanodeProtocolClientSideTranslatorPB(
              rpcProxy);

      EndpointStateMachine endPoint = new EndpointStateMachine(address,
          rpcClient, this.conf, threadNamePrefix);
      endPoint.setPassive(false);
      scmMachines.put(address, endPoint);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Adds a new Recon server to the set of endpoints.
   *
   * @param address Recon address.
   * @throws IOException
   */
  public void addReconServer(InetSocketAddress address,
      String threadNamePrefix) throws IOException {
    LOG.info("Adding Recon Server : {}", address.toString());
    writeLock();
    try {
      if (scmMachines.containsKey(address)) {
        LOG.warn("Trying to add an existing SCM Machine to Machines group. " +
            "Ignoring the request.");
        return;
      }
      Configuration hadoopConfig =
          LegacyHadoopConfigurationSource.asHadoopConfiguration(this.conf);
      RPC.setProtocolEngine(hadoopConfig, ReconDatanodeProtocolPB.class,
          ProtobufRpcEngine.class);
      long version =
          RPC.getProtocolVersion(ReconDatanodeProtocolPB.class);

      RetryPolicy retryPolicy =
          RetryPolicies.retryUpToMaximumCountWithFixedSleep(
              getScmRpcRetryCount(conf), getScmRpcRetryInterval(conf),
              TimeUnit.MILLISECONDS);
      ReconDatanodeProtocolPB rpcProxy = RPC.getProtocolProxy(
          ReconDatanodeProtocolPB.class, version,
          address, UserGroupInformation.getCurrentUser(), hadoopConfig,
          NetUtils.getDefaultSocketFactory(hadoopConfig), getRpcTimeout(),
          retryPolicy).getProxy();

      StorageContainerDatanodeProtocolClientSideTranslatorPB rpcClient =
          new StorageContainerDatanodeProtocolClientSideTranslatorPB(rpcProxy);

      EndpointStateMachine endPoint =
          new EndpointStateMachine(address, rpcClient, conf, threadNamePrefix);
      endPoint.setPassive(true);
      scmMachines.put(address, endPoint);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Removes a  SCM machine for the target set.
   *
   * @param address - Address of the SCM machine to send heartbeat to.
   * @throws IOException
   */
  public void removeSCMServer(InetSocketAddress address) throws IOException {
    writeLock();
    try {
      EndpointStateMachine endPoint = scmMachines.remove(address);
      if (endPoint == null) {
        LOG.warn("Trying to remove a non-existent SCM machine. " +
            "Ignoring the request.");
        return;
      }
      endPoint.setState(EndPointStates.SHUTDOWN);
      endPoint.close();
    } finally {
      writeUnlock();
    }
  }

  /**
   * Returns all known RPCEndpoints.
   *
   * @return - List of RPC Endpoints.
   */
  public Collection<EndpointStateMachine> getValues() {
    readLock();
    try {
      return unmodifiableList(new ArrayList<>(scmMachines.values()));
    } finally {
      readUnlock();
    }
  }

  @Override
  public void close() throws IOException {
    getValues().forEach(endpointStateMachine
        -> IOUtils.cleanupWithLogger(LOG, endpointStateMachine));
    if (jmxBean != null) {
      MBeans.unregister(jmxBean);
      jmxBean = null;
    }
  }

  @Override
  public List<EndpointStateMachineMBean> getSCMServers() {
    readLock();
    try {
      return unmodifiableList(new ArrayList<>(scmMachines.values()));
    } finally {
      readUnlock();
    }
  }

  /**
   * @return the number of connections (both SCM and Recon)
   */
  public int getNumOfConnections() {
    readLock();
    try {
      return scmMachines.size();
    } finally {
      readUnlock();
    }
  }
}
