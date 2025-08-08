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

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeFloat;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * Including OM performance related metrics.
 */
public class OMPerformanceMetrics {
  private static final String SOURCE_NAME =
      OMPerformanceMetrics.class.getSimpleName();

  @Metric(about = "Overall lookupKey in nanoseconds")
  private MutableRate lookupLatencyNs;

  @Metric(about = "Read key info from meta in nanoseconds")
  private MutableRate lookupReadKeyInfoLatencyNs;

  @Metric(about = "Block token generation latency in nanoseconds")
  private MutableRate lookupGenerateBlockTokenLatencyNs;

  @Metric(about = "Refresh location nanoseconds")
  private MutableRate lookupRefreshLocationLatencyNs;

  @Metric(about = "ACLs check nanoseconds")
  private MutableRate lookupAclCheckLatencyNs;

  @Metric(about = "resolveBucketLink latency nanoseconds")
  private MutableRate lookupResolveBucketLatencyNs;

  @Metric(about = "Overall getKeyInfo in nanoseconds")
  private MutableRate getKeyInfoLatencyNs;

  @Metric(about = "Read key info from db in getKeyInfo")
  private MutableRate getKeyInfoReadKeyInfoLatencyNs;

  @Metric(about = "Block token generation latency in getKeyInfo")
  private MutableRate getKeyInfoGenerateBlockTokenLatencyNs;

  @Metric(about = "Refresh location latency in getKeyInfo")
  private MutableRate getKeyInfoRefreshLocationLatencyNs;

  @Metric(about = "ACLs check in getKeyInfo")
  private MutableRate getKeyInfoAclCheckLatencyNs;

  @Metric(about = "Sort datanodes latency in getKeyInfo")
  private MutableRate getKeyInfoSortDatanodesLatencyNs;

  @Metric(about = "resolveBucketLink latency in getKeyInfo")
  private MutableRate getKeyInfoResolveBucketLatencyNs;

  @Metric(about = "s3VolumeInfo latency nanoseconds")
  private MutableRate s3VolumeContextLatencyNs;

  @Metric(about = "Client requests forcing container info cache refresh")
  private MutableRate forceContainerCacheRefresh;

  @Metric(about = "checkAccess latency in nanoseconds")
  private MutableRate checkAccessLatencyNs;

  @Metric(about = "listKeys latency in nanoseconds")
  private MutableRate listKeysLatencyNs;

  @Metric(about = "Validate request latency in nano seconds")
  private MutableRate validateRequestLatencyNs;

  @Metric(about = "Validate response latency in nano seconds")
  private MutableRate validateResponseLatencyNs;

  @Metric(about = "PreExecute latency in nano seconds")
  private MutableRate preExecuteLatencyNs;

  @Metric(about = "Ratis latency in nano seconds")
  private MutableRate submitToRatisLatencyNs;

  @Metric(about = "Convert om request to ratis request nano seconds")
  private MutableRate createRatisRequestLatencyNs;

  @Metric(about = "Convert ratis response to om response nano seconds")
  private MutableRate createOmResponseLatencyNs;

  @Metric(about = "Ratis local command execution latency in nano seconds")
  private MutableRate validateAndUpdateCacheLatencyNs;

  @Metric(about = "average pagination for listKeys")
  private MutableRate listKeysAveragePagination;

  @Metric(about = "ops per second for listKeys")
  private MutableGaugeFloat listKeysOpsPerSec;

  @Metric(about = "ACLs check latency in listKeys")
  private MutableRate listKeysAclCheckLatencyNs;

  @Metric(about = "resolveBucketLink latency in listKeys")
  private MutableRate listKeysResolveBucketLatencyNs;

  @Metric(about = "deleteKeyFailure latency in nano seconds")
  private MutableRate deleteKeyFailureLatencyNs;

  @Metric(about = "deleteKeySuccess latency in nano seconds")
  private MutableRate deleteKeySuccessLatencyNs;

  @Metric(about = "resolveBucketLink latency in deleteKeys")
  private MutableRate deleteKeysResolveBucketLatencyNs;

  @Metric(about = "ACLs check latency in deleteKeys")
  private MutableRate deleteKeysAclCheckLatencyNs;

  @Metric(about = "resolveBucketLink and ACLs check latency in deleteKey")
  private MutableRate deleteKeyResolveBucketAndAclCheckLatencyNs;
  
  @Metric(about = "readFromRockDb latency in listKeys")
  private MutableRate listKeysReadFromRocksDbLatencyNs;

  @Metric(about = "resolveBucketLink latency in getObjectTagging")
  private MutableRate getObjectTaggingResolveBucketLatencyNs;

  @Metric(about = "ACLs check in getObjectTagging")
  private MutableRate getObjectTaggingAclCheckLatencyNs;

  @Metric(about = "Latency of each iteration of DirectoryDeletingService in ms")
  private MutableGaugeLong directoryDeletingServiceLatencyMs;

  @Metric(about = "Latency of each iteration of KeyDeletingService in ms")
  private MutableGaugeLong keyDeletingServiceLatencyMs;

  @Metric(about = "Latency of each iteration of OpenKeyCleanupService in ms")
  private MutableGaugeLong openKeyCleanupServiceLatencyMs;

  @Metric(about = "ResolveBucketLink and ACL check latency for createKey in nanoseconds")
  private MutableRate createKeyResolveBucketAndAclCheckLatencyNs;
  
  @Metric(about = "check quota for createKey in nanoseconds")
  private MutableRate createKeyQuotaCheckLatencyNs;

  @Metric(about = "Block allocation latency for createKey in nanoseconds")
  private MutableRate createKeyAllocateBlockLatencyNs;

  @Metric(about = "createKeyFailure latency in nanoseconds")
  private MutableRate createKeyFailureLatencyNs;

  @Metric(about = "creteKeySuccess latency in nanoseconds")
  private MutableRate createKeySuccessLatencyNs;

  public static OMPerformanceMetrics register() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        "OzoneManager Request Performance",
        new OMPerformanceMetrics());
  }

  public static void unregister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  public void addLookupLatency(long latencyInNs) {
    lookupLatencyNs.add(latencyInNs);
  }

  MutableRate getLookupRefreshLocationLatencyNs() {
    return lookupRefreshLocationLatencyNs;
  }

  MutableRate getLookupGenerateBlockTokenLatencyNs() {
    return lookupGenerateBlockTokenLatencyNs;
  }

  MutableRate getLookupReadKeyInfoLatencyNs() {
    return lookupReadKeyInfoLatencyNs;
  }

  MutableRate getLookupAclCheckLatencyNs() {
    return lookupAclCheckLatencyNs;
  }

  public void addS3VolumeContextLatencyNs(long latencyInNs) {
    s3VolumeContextLatencyNs.add(latencyInNs);
  }

  MutableRate getLookupResolveBucketLatencyNs() {
    return lookupResolveBucketLatencyNs;
  }

  public void addGetKeyInfoLatencyNs(long value) {
    getKeyInfoLatencyNs.add(value);
  }

  MutableRate getGetKeyInfoAclCheckLatencyNs() {
    return getKeyInfoAclCheckLatencyNs;
  }

  MutableRate getGetKeyInfoGenerateBlockTokenLatencyNs() {
    return getKeyInfoGenerateBlockTokenLatencyNs;
  }

  MutableRate getGetKeyInfoReadKeyInfoLatencyNs() {
    return getKeyInfoReadKeyInfoLatencyNs;
  }

  MutableRate getGetKeyInfoRefreshLocationLatencyNs() {
    return getKeyInfoRefreshLocationLatencyNs;
  }

  MutableRate getGetKeyInfoResolveBucketLatencyNs() {
    return getKeyInfoResolveBucketLatencyNs;
  }

  MutableRate getGetKeyInfoSortDatanodesLatencyNs() {
    return getKeyInfoSortDatanodesLatencyNs;
  }

  public void setForceContainerCacheRefresh(boolean value) {
    forceContainerCacheRefresh.add(value ? 1L : 0L);
  }

  public void setCheckAccessLatencyNs(long latencyInNs) {
    checkAccessLatencyNs.add(latencyInNs);
  }

  public void addListKeysLatencyNs(long latencyInNs) {
    listKeysLatencyNs.add(latencyInNs);
  }

  public MutableRate getValidateRequestLatencyNs() {
    return validateRequestLatencyNs;
  }

  public MutableRate getValidateResponseLatencyNs() {
    return validateResponseLatencyNs;
  }

  public MutableRate getPreExecuteLatencyNs() {
    return preExecuteLatencyNs;
  }

  public MutableRate getSubmitToRatisLatencyNs() {
    return submitToRatisLatencyNs;
  }

  public MutableRate getCreateRatisRequestLatencyNs() {
    return createRatisRequestLatencyNs;
  }

  public MutableRate getCreateOmResponseLatencyNs() {
    return createOmResponseLatencyNs;
  }

  public MutableRate getValidateAndUpdateCacheLatencyNs() {
    return validateAndUpdateCacheLatencyNs;
  }

  public void setListKeysAveragePagination(long keyCount) {
    listKeysAveragePagination.add(keyCount);
  }

  public void setListKeysOpsPerSec(float opsPerSec) {
    listKeysOpsPerSec.set(opsPerSec);
  }

  MutableRate getListKeysAclCheckLatencyNs() {
    return listKeysAclCheckLatencyNs;
  }

  MutableRate getListKeysResolveBucketLatencyNs() {
    return listKeysResolveBucketLatencyNs;
  }

  public void setDeleteKeyFailureLatencyNs(long latencyInNs) {
    deleteKeyFailureLatencyNs.add(latencyInNs);
  }

  public void setDeleteKeySuccessLatencyNs(long latencyInNs) {
    deleteKeySuccessLatencyNs.add(latencyInNs);
  }

  public void setDeleteKeysResolveBucketLatencyNs(long latencyInNs) {
    deleteKeysResolveBucketLatencyNs.add(latencyInNs);
  }

  public void setDeleteKeysAclCheckLatencyNs(long latencyInNs) {
    deleteKeysAclCheckLatencyNs.add(latencyInNs);
  }

  public MutableRate getDeleteKeyResolveBucketAndAclCheckLatencyNs() {
    return deleteKeyResolveBucketAndAclCheckLatencyNs;
  }

  public MutableRate getCreateKeyResolveBucketAndAclCheckLatencyNs() {
    return createKeyResolveBucketAndAclCheckLatencyNs;
  }

  public void addCreateKeyQuotaCheckLatencyNs(long latencyInNs) {
    createKeyQuotaCheckLatencyNs.add(latencyInNs);
  }

  public MutableRate getCreateKeyAllocateBlockLatencyNs() {
    return createKeyAllocateBlockLatencyNs;
  }

  public void addCreateKeyFailureLatencyNs(long latencyInNs) {
    createKeyFailureLatencyNs.add(latencyInNs);
  }

  public void addCreateKeySuccessLatencyNs(long latencyInNs) {
    createKeySuccessLatencyNs.add(latencyInNs);
  }
    
  public void addListKeysReadFromRocksDbLatencyNs(long latencyInNs) {
    listKeysReadFromRocksDbLatencyNs.add(latencyInNs);
  }

  public MutableRate getGetObjectTaggingResolveBucketLatencyNs() {
    return getObjectTaggingResolveBucketLatencyNs;
  }

  public MutableRate getGetObjectTaggingAclCheckLatencyNs() {
    return getObjectTaggingAclCheckLatencyNs;
  }

  public void addGetObjectTaggingLatencyNs(long latencyInNs) {
    getObjectTaggingAclCheckLatencyNs.add(latencyInNs);
  }

  public void setDirectoryDeletingServiceLatencyMs(long latencyInMs) {
    directoryDeletingServiceLatencyMs.set(latencyInMs);
  }

  public void setKeyDeletingServiceLatencyMs(long latencyInMs) {
    keyDeletingServiceLatencyMs.set(latencyInMs);
  }

  public void setOpenKeyCleanupServiceLatencyMs(long latencyInMs) {
    openKeyCleanupServiceLatencyMs.set(latencyInMs);
  }
}
