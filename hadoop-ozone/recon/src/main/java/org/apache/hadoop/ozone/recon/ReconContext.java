/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.recon;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * ReconContext is the single source of truth for some key information shared
 * across multiple modules within Recon, including:
 * 1) ReconNodeManager
 * 2) OzoneManagerServiceProviderImpl
 *
 * If current Recon is not healthy, isHealthy check will return true/false accordingly.
 * UnHealthyModuleErrorMap will maintain the error info for the module failed to load.
 */
@Singleton
public final class ReconContext {
  private static final Logger LOG = LoggerFactory.getLogger(ReconContext.class);

  private final String threadNamePrefix;
  private OzoneConfiguration ozoneConfiguration;
  private ReconUtils reconUtils;
  private boolean isHealthy = true;

  private Map<ErrorCode, String> errCodeMsgMap = new HashMap<>();
  private Map<ErrorCode, List<String>> errCodeImpactMap = new HashMap<>();
  private List<ErrorCode> errors = new ArrayList<>();
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  @Inject
  public ReconContext(OzoneConfiguration configuration, ReconUtils reconUtils) {
    this.reconUtils = reconUtils;
    this.ozoneConfiguration = configuration;
    threadNamePrefix = reconUtils.getReconNodeDetails(configuration).threadNamePrefix();
    initializeErrCodeMetaData();
  }

  private void initializeErrCodeMetaData() {
    // This maps the error code to error message.
    errCodeMsgMap.put(ErrorCode.OK, "Recon is healthy !!!");
    errCodeMsgMap.put(ErrorCode.INVALID_NETWORK_TOPOLOGY,
        "Invalid network topology of datanodes. Failed to register and load datanodes. Pipelines may not be " +
            "healthy !!!");
    errCodeMsgMap.put(ErrorCode.CERTIFICATE_INIT_FAILED, "Error during initializing Recon certificate !!!");
    errCodeMsgMap.put(ErrorCode.INTERNAL_ERROR,
        "Unexpected internal error. Kindly refer ozone-recon.log file for details !!!");
    errCodeMsgMap.put(ErrorCode.GET_OM_DB_SNAPSHOT_FAILED, "OM DB Snapshot sync failed !!!");

    // This maps the error codes to impacted areas of Recon.
    errCodeImpactMap.put(ErrorCode.INVALID_NETWORK_TOPOLOGY, Arrays.asList("Datanodes", "Pipelines"));
    errCodeImpactMap.put(ErrorCode.CERTIFICATE_INIT_FAILED, Arrays.asList("Initializing secure Recon"));
    errCodeImpactMap.put(ErrorCode.INTERNAL_ERROR, Arrays.asList("Recon health"));
    errCodeImpactMap.put(ErrorCode.GET_OM_DB_SNAPSHOT_FAILED, Arrays.asList("Overview (OM Data)", "OM DB Insights"));
  }

  /**
   * @param healthStatus : update Health Status of Recon.
   */
  public void updateHealthStatus(boolean healthStatus) {
    lock.writeLock().lock();
    try {
      LOG.info("Update healthStatus of Recon from {} to {}.", isHealthy, healthStatus);
      this.isHealthy = healthStatus;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public boolean isHealthy() {
    lock.readLock().lock();
    try {
      return this.isHealthy;
    } finally {
      lock.readLock().unlock();
    }
  }

  public String threadNamePrefix() {
    return threadNamePrefix;
  }

  public Map<ErrorCode, String> getErrCodeMsgMap() {
    return errCodeMsgMap;
  }

  public Map<ErrorCode, List<String>> getErrCodeImpactMap() {
    return errCodeImpactMap;
  }

  public List<ErrorCode> getErrors() {
    return errors;
  }

  public void updateErrors(ErrorCode errorCode) {
    lock.writeLock().lock();
    try {
      LOG.info("Added error of Recon {}.", errorCode);
      errors.add(errorCode);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Error codes to make it easy to decode these errors in Recon.
   */
  public enum ErrorCode {
    OK,
    INVALID_NETWORK_TOPOLOGY,
    CERTIFICATE_INIT_FAILED,
    INTERNAL_ERROR,
    GET_OM_DB_SNAPSHOT_FAILED
  }
}
