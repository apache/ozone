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

package org.apache.hadoop.ozone.audit;

/**
 * Enum to define Audit Action types for SCM.
 */
public enum SCMAction implements AuditAction {

  GET_VERSION,
  REGISTER,
  SEND_HEARTBEAT,
  GET_SCM_INFO,
  ALLOCATE_BLOCK,
  DELETE_KEY_BLOCK,
  ALLOCATE_CONTAINER,
  GET_CONTAINER,
  GET_CONTAINER_WITH_PIPELINE,
  LIST_CONTAINER,
  CREATE_PIPELINE,
  LIST_PIPELINE,
  CLOSE_PIPELINE,
  ACTIVATE_PIPELINE,
  DEACTIVATE_PIPELINE,
  CLOSE_CONTAINER,
  DELETE_CONTAINER,
  IN_SAFE_MODE,
  FORCE_EXIT_SAFE_MODE,
  SORT_DATANODE,
  START_REPLICATION_MANAGER,
  STOP_REPLICATION_MANAGER,
  GET_REPLICATION_MANAGER_STATUS,
  START_CONTAINER_BALANCER,
  STOP_CONTAINER_BALANCER,
  GET_CONTAINER_BALANCER_STATUS,
  GET_CONTAINER_BALANCER_STATUS_INFO,
  GET_CONTAINER_WITH_PIPELINE_BATCH,
  ADD_SCM,
  GET_REPLICATION_MANAGER_REPORT,
  TRANSFER_LEADERSHIP,
  GET_CONTAINER_REPLICAS,
  GET_CONTAINERS_ON_DECOM_NODE,
  DECOMMISSION_NODES,
  START_MAINTENANCE_NODES,
  GET_SAFE_MODE_RULE_STATUSES,
  FINALIZE_SCM_UPGRADE,
  QUERY_UPGRADE_FINALIZATION_PROGRESS,
  GET_DATANODE_USAGE_INFO,
  GET_CONTAINER_TOKEN,
  GET_CONTAINER_COUNT,
  DECOMMISSION_SCM,
  GET_METRICS,
  QUERY_NODE,
  GET_PIPELINE,
  RECONCILE_CONTAINER,
  GET_DELETED_BLOCK_SUMMARY,
  ACKNOWLEDGE_MISSING_CONTAINER,
  UNACKNOWLEDGE_MISSING_CONTAINER;

  @Override
  public String getAction() {
    return this.toString();
  }

}
