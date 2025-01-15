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
import org.apache.hadoop.metrics2.lib.MutableGaugeFloat;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.metrics.OzoneMetricsSystem;
import org.apache.hadoop.ozone.metrics.OzoneMutableRate;

/**
 * Including OM performance related metrics.
 */
public class OMPerformanceMetrics {
  private static final String SOURCE_NAME =
      OMPerformanceMetrics.class.getSimpleName();

  public static OMPerformanceMetrics register() {
    MetricsSystem ms = OzoneMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
            "OzoneManager Request Performance",
            new OMPerformanceMetrics());
  }

  public static void unregister() {
    MetricsSystem ms = OzoneMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  @Metric(about = "Overall lookupKey in nanoseconds")
  private OzoneMutableRate lookupLatencyNs;

  @Metric(about = "Read key info from meta in nanoseconds")
  private OzoneMutableRate lookupReadKeyInfoLatencyNs;

  @Metric(about = "Block token generation latency in nanoseconds")
  private OzoneMutableRate lookupGenerateBlockTokenLatencyNs;

  @Metric(about = "Refresh location nanoseconds")
  private OzoneMutableRate lookupRefreshLocationLatencyNs;

  @Metric(about = "ACLs check nanoseconds")
  private OzoneMutableRate lookupAclCheckLatencyNs;

  @Metric(about = "resolveBucketLink latency nanoseconds")
  private OzoneMutableRate lookupResolveBucketLatencyNs;


  @Metric(about = "Overall getKeyInfo in nanoseconds")
  private OzoneMutableRate getKeyInfoLatencyNs;

  @Metric(about = "Read key info from db in getKeyInfo")
  private OzoneMutableRate getKeyInfoReadKeyInfoLatencyNs;

  @Metric(about = "Block token generation latency in getKeyInfo")
  private OzoneMutableRate getKeyInfoGenerateBlockTokenLatencyNs;

  @Metric(about = "Refresh location latency in getKeyInfo")
  private OzoneMutableRate getKeyInfoRefreshLocationLatencyNs;

  @Metric(about = "ACLs check in getKeyInfo")
  private OzoneMutableRate getKeyInfoAclCheckLatencyNs;

  @Metric(about = "Sort datanodes latency in getKeyInfo")
  private OzoneMutableRate getKeyInfoSortDatanodesLatencyNs;

  @Metric(about = "resolveBucketLink latency in getKeyInfo")
  private OzoneMutableRate getKeyInfoResolveBucketLatencyNs;

  @Metric(about = "s3VolumeInfo latency nanoseconds")
  private OzoneMutableRate s3VolumeContextLatencyNs;

  @Metric(about = "Client requests forcing container info cache refresh")
  private OzoneMutableRate forceContainerCacheRefresh;

  @Metric(about = "checkAccess latency in nanoseconds")
  private OzoneMutableRate checkAccessLatencyNs;

  @Metric(about = "listKeys latency in nanoseconds")
  private OzoneMutableRate listKeysLatencyNs;

  @Metric(about = "Validate request latency in nano seconds")
  private OzoneMutableRate validateRequestLatencyNs;

  @Metric(about = "Validate response latency in nano seconds")
  private OzoneMutableRate validateResponseLatencyNs;

  @Metric(about = "PreExecute latency in nano seconds")
  private OzoneMutableRate preExecuteLatencyNs;

  @Metric(about = "Ratis latency in nano seconds")
  private OzoneMutableRate submitToRatisLatencyNs;

  @Metric(about = "Convert om request to ratis request nano seconds")
  private OzoneMutableRate createRatisRequestLatencyNs;

  @Metric(about = "Convert ratis response to om response nano seconds")
  private OzoneMutableRate createOmResponseLatencyNs;

  @Metric(about = "Ratis local command execution latency in nano seconds")
  private OzoneMutableRate validateAndUpdateCacheLatencyNs;

  @Metric(about = "average pagination for listKeys")
  private OzoneMutableRate listKeysAveragePagination;

  @Metric(about = "ops per second for listKeys")
  private MutableGaugeFloat listKeysOpsPerSec;

  @Metric(about = "ACLs check latency in listKeys")
  private OzoneMutableRate listKeysAclCheckLatencyNs;

  @Metric(about = "resolveBucketLink latency in listKeys")
  private OzoneMutableRate listKeysResolveBucketLatencyNs;

  @Metric(about = "deleteKeyFailure latency in nano seconds")
  private OzoneMutableRate deleteKeyFailureLatencyNs;

  @Metric(about = "deleteKeySuccess latency in nano seconds")
  private OzoneMutableRate deleteKeySuccessLatencyNs;

  @Metric(about = "resolveBucketLink latency in deleteKeys")
  private OzoneMutableRate deleteKeysResolveBucketLatencyNs;

  @Metric(about = "ACLs check latency in deleteKeys")
  private OzoneMutableRate deleteKeysAclCheckLatencyNs;

  @Metric(about = "resolveBucketLink and ACLs check latency in deleteKey")
  private OzoneMutableRate deleteKeyResolveBucketAndAclCheckLatencyNs;
  
  @Metric(about = "readFromRockDb latency in listKeys")
  private OzoneMutableRate listKeysReadFromRocksDbLatencyNs;

  @Metric(about = "resolveBucketLink latency in getObjectTagging")
  private OzoneMutableRate getObjectTaggingResolveBucketLatencyNs;

  @Metric(about = "ACLs check in getObjectTagging")
  private OzoneMutableRate getObjectTaggingAclCheckLatencyNs;

  @Metric(about = "Latency of each iteration of DirectoryDeletingService in ms")
  private MutableGaugeLong directoryDeletingServiceLatencyMs;

  @Metric(about = "Latency of each iteration of KeyDeletingService in ms")
  private MutableGaugeLong keyDeletingServiceLatencyMs;

  @Metric(about = "Latency of each iteration of OpenKeyCleanupService in ms")
  private MutableGaugeLong openKeyCleanupServiceLatencyMs;

  public void addLookupLatency(long latencyInNs) {
    lookupLatencyNs.add(latencyInNs);
  }

  OzoneMutableRate getLookupRefreshLocationLatencyNs() {
    return lookupRefreshLocationLatencyNs;
  }


  OzoneMutableRate getLookupGenerateBlockTokenLatencyNs() {
    return lookupGenerateBlockTokenLatencyNs;
  }

  OzoneMutableRate getLookupReadKeyInfoLatencyNs() {
    return lookupReadKeyInfoLatencyNs;
  }

  OzoneMutableRate getLookupAclCheckLatencyNs() {
    return lookupAclCheckLatencyNs;
  }

  public void addS3VolumeContextLatencyNs(long latencyInNs) {
    s3VolumeContextLatencyNs.add(latencyInNs);
  }

  OzoneMutableRate getLookupResolveBucketLatencyNs() {
    return lookupResolveBucketLatencyNs;
  }

  public void addGetKeyInfoLatencyNs(long value) {
    getKeyInfoLatencyNs.add(value);
  }

  OzoneMutableRate getGetKeyInfoAclCheckLatencyNs() {
    return getKeyInfoAclCheckLatencyNs;
  }

  OzoneMutableRate getGetKeyInfoGenerateBlockTokenLatencyNs() {
    return getKeyInfoGenerateBlockTokenLatencyNs;
  }

  OzoneMutableRate getGetKeyInfoReadKeyInfoLatencyNs() {
    return getKeyInfoReadKeyInfoLatencyNs;
  }

  OzoneMutableRate getGetKeyInfoRefreshLocationLatencyNs() {
    return getKeyInfoRefreshLocationLatencyNs;
  }

  OzoneMutableRate getGetKeyInfoResolveBucketLatencyNs() {
    return getKeyInfoResolveBucketLatencyNs;
  }

  OzoneMutableRate getGetKeyInfoSortDatanodesLatencyNs() {
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

  public OzoneMutableRate getValidateRequestLatencyNs() {
    return validateRequestLatencyNs;
  }

  public OzoneMutableRate getValidateResponseLatencyNs() {
    return validateResponseLatencyNs;
  }

  public OzoneMutableRate getPreExecuteLatencyNs() {
    return preExecuteLatencyNs;
  }

  public OzoneMutableRate getSubmitToRatisLatencyNs() {
    return submitToRatisLatencyNs;
  }

  public OzoneMutableRate getCreateRatisRequestLatencyNs() {
    return createRatisRequestLatencyNs;
  }

  public OzoneMutableRate getCreateOmResponseLatencyNs() {
    return createOmResponseLatencyNs;
  }

  public OzoneMutableRate getValidateAndUpdateCacheLatencyNs() {
    return validateAndUpdateCacheLatencyNs;
  }

  public void setListKeysAveragePagination(long keyCount) {
    listKeysAveragePagination.add(keyCount);
  }

  public void setListKeysOpsPerSec(float opsPerSec) {
    listKeysOpsPerSec.set(opsPerSec);
  }

  OzoneMutableRate getListKeysAclCheckLatencyNs() {
    return listKeysAclCheckLatencyNs;
  }

  OzoneMutableRate getListKeysResolveBucketLatencyNs() {
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

  public OzoneMutableRate getDeleteKeyResolveBucketAndAclCheckLatencyNs() {
    return deleteKeyResolveBucketAndAclCheckLatencyNs;
  }
    
  public void addListKeysReadFromRocksDbLatencyNs(long latencyInNs) {
    listKeysReadFromRocksDbLatencyNs.add(latencyInNs);
  }

  public OzoneMutableRate getGetObjectTaggingResolveBucketLatencyNs() {
    return getObjectTaggingResolveBucketLatencyNs;
  }

  public OzoneMutableRate getGetObjectTaggingAclCheckLatencyNs() {
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
