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

package org.apache.hadoop.hdds.scm;

import java.time.Duration;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.ReconfigurableConfig;

/**
 * The configuration class for the SCM service.
 */
@ConfigGroup(prefix = "hdds.scm")
public class ScmConfig extends ReconfigurableConfig {
  public static final String HDDS_SCM_UNKNOWN_CONTAINER_ACTION = "hdds.scm.unknown-container.action";

  @Config(key = "hdds.scm.kerberos.principal",
      type = ConfigType.STRING,
      defaultValue = "",
      tags = { ConfigTag.SECURITY, ConfigTag.OZONE },
      description = "This Kerberos principal is used by the SCM service."
  )
  private String principal;

  @Config(key = "hdds.scm.kerberos.keytab.file",
      type = ConfigType.STRING,
      defaultValue = "",
      tags = { ConfigTag.SECURITY, ConfigTag.OZONE },
      description = "The keytab file used by SCM daemon to login as " +
          "its service principal."
  )
  private String keytab;

  @Config(key = "hdds.scm.unknown-container.action",
      type = ConfigType.STRING,
      defaultValue = "WARN",
      tags = { ConfigTag.SCM, ConfigTag.MANAGEMENT },
      description =
          "The action taken by SCM to process unknown "
          + "containers that reported by Datanodes. The default "
          + "action is just logging container not found warning, "
          + "another available action is DELETE action. "
          + "These unknown containers will be deleted under this "
          + "action way."
  )
  private String action;

  private static final String DESCRIPTION_COMMON_CHOICES_OF_PIPELINE_CHOOSE_POLICY_IMPL =
      "One of the following values can be used: "
      + "(1) org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RandomPipelineChoosePolicy"
      + " : chooses a pipeline randomly. "
      + "(2) org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.HealthyPipelineChoosePolicy"
      + " : chooses a healthy pipeline randomly. "
      + "(3) org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.CapacityPipelineChoosePolicy"
      + " : chooses the pipeline with lower utilization from two random pipelines. Note that"
      + " random choose method will be executed twice in this policy."
      + "(4) org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RoundRobinPipelineChoosePolicy"
      + " : chooses a pipeline in a round robin fashion. Intended for troubleshooting and testing purposes only.";

  // hdds.scm.pipeline.choose.policy.impl
  @Config(key = "hdds.scm.pipeline.choose.policy.impl",
      type = ConfigType.STRING,
      defaultValue = "org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RandomPipelineChoosePolicy",
      tags = { ConfigTag.SCM, ConfigTag.PIPELINE },
      description =
          "Sets the policy for choosing a pipeline for a Ratis container. The value should be "
          + "the full name of a class which implements org.apache.hadoop.hdds.scm.PipelineChoosePolicy. "
          + "The class decides which pipeline will be used to find or allocate Ratis containers. If not set, "
          + "org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RandomPipelineChoosePolicy"
          + " will be used as default value. " + DESCRIPTION_COMMON_CHOICES_OF_PIPELINE_CHOOSE_POLICY_IMPL
  )
  private String pipelineChoosePolicyName;

  // hdds.scm.ec.pipeline.choose.policy.impl
  @Config(key = "hdds.scm.ec.pipeline.choose.policy.impl",
      type = ConfigType.STRING,
      defaultValue = "org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RandomPipelineChoosePolicy",
      tags = { ConfigTag.SCM, ConfigTag.PIPELINE },
      description =
          "Sets the policy for choosing an EC pipeline. The value should be "
          + "the full name of a class which implements org.apache.hadoop.hdds.scm.PipelineChoosePolicy. "
          + "The class decides which pipeline will be used when selecting an EC Pipeline. If not set, "
          + "org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RandomPipelineChoosePolicy"
          + " will be used as default value. " + DESCRIPTION_COMMON_CHOICES_OF_PIPELINE_CHOOSE_POLICY_IMPL
  )
  private String ecPipelineChoosePolicyName;

  @Config(key = "hdds.scm.block.deletion.per-interval.max",
      type = ConfigType.INT,
      defaultValue = "500000",
      reconfigurable = true,
      tags = { ConfigTag.SCM, ConfigTag.DELETION},
      description =
          "Maximum number of blocks which SCM processes during an interval. "
              + "The block num is counted at the replica level."
              + "If SCM has 100000 blocks which need to be deleted and the "
              + "configuration is 5000 then it would only send 5000 blocks "
              + "for deletion to the datanodes."
  )
  private int blockDeletionLimit;

  @Config(key = "hdds.scm.block.deleting.service.interval",
      defaultValue = "60s",
      type = ConfigType.TIME,
      tags = { ConfigTag.SCM, ConfigTag.DELETION },
      description =
          "Time interval of the scm block deleting service. The block deleting"
              + "service runs on SCM periodically and deletes blocks "
              + "queued for deletion. Unit could be defined with "
              + "postfix (ns,ms,s,m,h,d). "
  )
  private Duration blockDeletionInterval = Duration.ofSeconds(60);

  @Config(key = "hdds.scm.block.deletion.txn.dn.commit.map.limit",
      defaultValue = "5000000",
      type = ConfigType.INT,
      tags = { ConfigTag.SCM },
      description =
          " This value indicates the size of the transactionToDNsCommitMap after which" +
              " we will skip one round of scm block deleting interval."
  )
  private int transactionToDNsCommitMapLimit = 5000000;

  public int getTransactionToDNsCommitMapLimit() {
    return transactionToDNsCommitMapLimit;
  }

  public Duration getBlockDeletionInterval() {
    return blockDeletionInterval;
  }

  public void setBlockDeletionInterval(Duration duration) {
    blockDeletionInterval = duration;
  }

  public void setKerberosPrincipal(String kerberosPrincipal) {
    this.principal = kerberosPrincipal;
  }

  public void setKerberosKeytab(String kerberosKeytab) {
    this.keytab = kerberosKeytab;
  }

  public void setUnknownContainerAction(String unknownContainerAction) {
    this.action = unknownContainerAction;
  }

  public void setPipelineChoosePolicyName(String pipelineChoosePolicyName) {
    this.pipelineChoosePolicyName = pipelineChoosePolicyName;
  }

  public void setECPipelineChoosePolicyName(String policyName) {
    this.ecPipelineChoosePolicyName = policyName;
  }

  public void setBlockDeletionLimit(int blockDeletionLimit) {
    this.blockDeletionLimit = blockDeletionLimit;
  }

  public String getKerberosPrincipal() {
    return this.principal;
  }

  public String getKerberosKeytab() {
    return this.keytab;
  }

  public String getUnknownContainerAction() {
    return this.action;
  }

  public String getPipelineChoosePolicyName() {
    return pipelineChoosePolicyName;
  }

  public String getECPipelineChoosePolicyName() {
    return ecPipelineChoosePolicyName;
  }

  public int getBlockDeletionLimit() {
    return blockDeletionLimit;
  }

  /**
   * Configuration strings class.
   * required for SCMSecurityProtocol where the KerberosInfo references
   * the old configuration with
   * the annotation shown below:-
   * {@code @KerberosInfo(serverPrincipal = ScmConfigKeys.HDDS_SCM_KERBEROS_PRINCIPAL_KEY)}
   */
  public static class ConfigStrings {
    public static final String HDDS_SCM_KERBEROS_PRINCIPAL_KEY = "hdds.scm.kerberos.principal";
    public static final String HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY = "hdds.scm.kerberos.keytab.file";
  }
}
