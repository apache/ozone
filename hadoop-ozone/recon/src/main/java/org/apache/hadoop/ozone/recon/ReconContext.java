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

package org.apache.hadoop.ozone.recon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private String clusterId;
  private OzoneConfiguration ozoneConfiguration;
  private ReconUtils reconUtils;
  private AtomicBoolean isHealthy = new AtomicBoolean(true);

  private Map<ErrorCode, String> errCodeMsgMap = new HashMap<>();
  private Map<ErrorCode, List<String>> errCodeImpactMap = new HashMap<>();
  private List<ErrorCode> errors = Collections.synchronizedList(new ArrayList<>());

  @Inject
  public ReconContext(OzoneConfiguration configuration, ReconUtils reconUtils) {
    this.reconUtils = reconUtils;
    this.ozoneConfiguration = configuration;
    threadNamePrefix = reconUtils.getReconNodeDetails(configuration).threadNamePrefix();
    initializeErrCodeMetaData();
  }

  private void initializeErrCodeMetaData() {
    for (ErrorCode errorCode : ErrorCode.values()) {
      errCodeMsgMap.put(errorCode, errorCode.getMessage());
      errCodeImpactMap.put(errorCode, errorCode.getImpacts());
    }
  }

  /**
   * @param healthStatus : update Health Status of Recon.
   */
  public void updateHealthStatus(AtomicBoolean healthStatus) {
    boolean oldHealthStatus = isHealthy.getAndSet(healthStatus.get());
    LOG.info("Update healthStatus of Recon from {} to {}.", oldHealthStatus, isHealthy.get());
  }

  public AtomicBoolean isHealthy() {
    return isHealthy;
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
    errors.add(errorCode);
  }

  public void setClusterId(String clusterId) {
    this.clusterId = clusterId;
  }

  public String getClusterId() {
    return clusterId;
  }

  /**
   * Error codes to make it easy to decode these errors in Recon.
   */
  public enum ErrorCode {
    OK("Recon is healthy !!!", Collections.emptyList()),
    INVALID_NETWORK_TOPOLOGY(
        "Invalid network topology of datanodes. Failed to register and load datanodes. Pipelines may not be " +
            "healthy !!!", Arrays.asList("Datanodes", "Pipelines")),
    CERTIFICATE_INIT_FAILED(
        "Error during initializing Recon certificate !!!",
        Arrays.asList("Initializing secure Recon")),
    INTERNAL_ERROR(
        "Unexpected internal error. Kindly refer ozone-recon.log file for details !!!",
        Arrays.asList("Recon health")),
    GET_OM_DB_SNAPSHOT_FAILED(
        "OM DB Snapshot sync failed !!!",
        Arrays.asList("Overview (OM Data)", "OM DB Insights")),
    GET_SCM_DB_SNAPSHOT_FAILED(
        "SCM DB Snapshot sync failed !!!",
        Arrays.asList("Containers", "Pipelines")),
    UPGRADE_FAILURE(
        "Schema upgrade failed. Recon encountered an issue while finalizing the layout upgrade.",
        Arrays.asList("Recon startup", "Metadata Layout Version"));

    private final String message;
    private final List<String> impacts;

    ErrorCode(String message, List<String> impacts) {
      this.message = message;
      this.impacts = impacts;
    }

    public String getMessage() {
      return message;
    }

    public List<String> getImpacts() {
      return impacts;
    }
  }
}
