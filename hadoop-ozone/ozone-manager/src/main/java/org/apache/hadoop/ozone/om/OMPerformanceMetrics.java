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

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeFloat;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.util.ConcurrentMutableRate;

/**
 * Including OM performance related metrics.
 *
 * <p>The latency counters use {@link ConcurrentMutableRate} so that
 * {@code add(long)} is non-blocking under concurrent handler threads recording
 * on the same metric instance. The metric names emitted for each counter
 * ({@code <Name>NumOps} / {@code <Name>AvgTime}) are unchanged from the
 * previous {@code MutableRate}-based implementation.
 */
@Metrics(about = "OzoneManager Request Performance", context = OzoneConsts.OZONE)
public class OMPerformanceMetrics implements MetricsSource {
  private static final String SOURCE_NAME =
      OMPerformanceMetrics.class.getSimpleName();

  // TODO: https://issues.apache.org/jira/browse/HDDS-13555
  @SuppressWarnings("PMD.SingularField")
  private final MetricsRegistry registry;

  private final ConcurrentMutableRate lookupLatencyNs;
  private final ConcurrentMutableRate lookupReadKeyInfoLatencyNs;
  private final ConcurrentMutableRate lookupGenerateBlockTokenLatencyNs;
  private final ConcurrentMutableRate lookupRefreshLocationLatencyNs;
  private final ConcurrentMutableRate lookupAclCheckLatencyNs;
  private final ConcurrentMutableRate lookupResolveBucketLatencyNs;
  private final ConcurrentMutableRate getKeyInfoLatencyNs;
  private final ConcurrentMutableRate getKeyInfoReadKeyInfoLatencyNs;
  private final ConcurrentMutableRate getKeyInfoGenerateBlockTokenLatencyNs;
  private final ConcurrentMutableRate getKeyInfoRefreshLocationLatencyNs;
  private final ConcurrentMutableRate getKeyInfoAclCheckLatencyNs;
  private final ConcurrentMutableRate getKeyInfoSortDatanodesLatencyNs;
  private final ConcurrentMutableRate allocateBlockSortDatanodesLatencyNs;
  private final ConcurrentMutableRate getKeyInfoResolveBucketLatencyNs;
  private final ConcurrentMutableRate s3VolumeContextLatencyNs;
  private final ConcurrentMutableRate forceContainerCacheRefresh;
  private final ConcurrentMutableRate checkAccessLatencyNs;
  private final ConcurrentMutableRate listKeysLatencyNs;
  private final ConcurrentMutableRate validateRequestLatencyNs;
  private final ConcurrentMutableRate validateResponseLatencyNs;
  private final ConcurrentMutableRate preExecuteLatencyNs;
  private final ConcurrentMutableRate submitToRatisLatencyNs;
  private final ConcurrentMutableRate createRatisRequestLatencyNs;
  private final ConcurrentMutableRate createOmResponseLatencyNs;
  private final ConcurrentMutableRate validateAndUpdateCacheLatencyNs;
  private final ConcurrentMutableRate listKeysAveragePagination;
  private final ConcurrentMutableRate listKeysAclCheckLatencyNs;
  private final ConcurrentMutableRate listKeysResolveBucketLatencyNs;
  private final ConcurrentMutableRate deleteKeyFailureLatencyNs;
  private final ConcurrentMutableRate deleteKeySuccessLatencyNs;
  private final ConcurrentMutableRate deleteKeysResolveBucketLatencyNs;
  private final ConcurrentMutableRate deleteKeysAclCheckLatencyNs;
  private final ConcurrentMutableRate deleteKeyResolveBucketAndAclCheckLatencyNs;
  private final ConcurrentMutableRate listKeysReadFromRocksDbLatencyNs;
  private final ConcurrentMutableRate getObjectTaggingResolveBucketLatencyNs;
  private final ConcurrentMutableRate getObjectTaggingAclCheckLatencyNs;
  private final ConcurrentMutableRate getBucketTaggingResolveBucketLatencyNs;
  private final ConcurrentMutableRate getBucketTaggingAclCheckLatencyNs;
  private final ConcurrentMutableRate getBucketTaggingLatencyNs;
  private final ConcurrentMutableRate createKeyResolveBucketAndAclCheckLatencyNs;
  private final ConcurrentMutableRate createKeyQuotaCheckLatencyNs;
  private final ConcurrentMutableRate createKeyAllocateBlockLatencyNs;
  private final ConcurrentMutableRate createKeyFailureLatencyNs;
  private final ConcurrentMutableRate createKeySuccessLatencyNs;

  @Metric(about = "ops per second for listKeys")
  private MutableGaugeFloat listKeysOpsPerSec;

  @Metric(about = "Latency of each iteration of DirectoryDeletingService in ms")
  private MutableGaugeLong directoryDeletingServiceLatencyMs;

  @Metric(about = "Latency of each iteration of KeyDeletingService in ms")
  private MutableGaugeLong keyDeletingServiceLatencyMs;

  @Metric(about = "Latency of each iteration of OpenKeyCleanupService in ms")
  private MutableGaugeLong openKeyCleanupServiceLatencyMs;

  @Metric(about = "Latency of the last snapshot full defragmentation operation in ms")
  private MutableGaugeLong snapshotDefragServiceFullLatencyMs;

  @Metric(about = "Latency of the last snapshot incremental defragmentation operation in ms")
  private MutableGaugeLong snapshotDefragServiceIncLatencyMs;

  public OMPerformanceMetrics() {
    registry = new MetricsRegistry(SOURCE_NAME);
    lookupLatencyNs = stat("LookupLatencyNs",
        "Overall lookupKey in nanoseconds");
    lookupReadKeyInfoLatencyNs = stat("LookupReadKeyInfoLatencyNs",
        "Read key info from meta in nanoseconds");
    lookupGenerateBlockTokenLatencyNs = stat("LookupGenerateBlockTokenLatencyNs",
        "Block token generation latency in nanoseconds");
    lookupRefreshLocationLatencyNs = stat("LookupRefreshLocationLatencyNs",
        "Refresh location nanoseconds");
    lookupAclCheckLatencyNs = stat("LookupAclCheckLatencyNs",
        "ACLs check nanoseconds");
    lookupResolveBucketLatencyNs = stat("LookupResolveBucketLatencyNs",
        "resolveBucketLink latency nanoseconds");
    getKeyInfoLatencyNs = stat("GetKeyInfoLatencyNs",
        "Overall getKeyInfo in nanoseconds");
    getKeyInfoReadKeyInfoLatencyNs = stat("GetKeyInfoReadKeyInfoLatencyNs",
        "Read key info from db in getKeyInfo");
    getKeyInfoGenerateBlockTokenLatencyNs = stat("GetKeyInfoGenerateBlockTokenLatencyNs",
        "Block token generation latency in getKeyInfo");
    getKeyInfoRefreshLocationLatencyNs = stat("GetKeyInfoRefreshLocationLatencyNs",
        "Refresh location latency in getKeyInfo");
    getKeyInfoAclCheckLatencyNs = stat("GetKeyInfoAclCheckLatencyNs",
        "ACLs check in getKeyInfo");
    getKeyInfoSortDatanodesLatencyNs = stat("GetKeyInfoSortDatanodesLatencyNs",
        "Sort datanodes latency in getKeyInfo");
    allocateBlockSortDatanodesLatencyNs = stat("AllocateBlockSortDatanodesLatencyNs",
        "Sort datanodes latency in allocateBlock (streaming write)");
    getKeyInfoResolveBucketLatencyNs = stat("GetKeyInfoResolveBucketLatencyNs",
        "resolveBucketLink latency in getKeyInfo");
    s3VolumeContextLatencyNs = stat("S3VolumeContextLatencyNs",
        "s3VolumeInfo latency nanoseconds");
    forceContainerCacheRefresh = stat("ForceContainerCacheRefresh",
        "Client requests forcing container info cache refresh");
    checkAccessLatencyNs = stat("CheckAccessLatencyNs",
        "checkAccess latency in nanoseconds");
    listKeysLatencyNs = stat("ListKeysLatencyNs",
        "listKeys latency in nanoseconds");
    validateRequestLatencyNs = stat("ValidateRequestLatencyNs",
        "Validate request latency in nano seconds");
    validateResponseLatencyNs = stat("ValidateResponseLatencyNs",
        "Validate response latency in nano seconds");
    preExecuteLatencyNs = stat("PreExecuteLatencyNs",
        "PreExecute latency in nano seconds");
    submitToRatisLatencyNs = stat("SubmitToRatisLatencyNs",
        "Ratis latency in nano seconds");
    createRatisRequestLatencyNs = stat("CreateRatisRequestLatencyNs",
        "Convert om request to ratis request nano seconds");
    createOmResponseLatencyNs = stat("CreateOmResponseLatencyNs",
        "Convert ratis response to om response nano seconds");
    validateAndUpdateCacheLatencyNs = stat("ValidateAndUpdateCacheLatencyNs",
        "Ratis local command execution latency in nano seconds");
    listKeysAveragePagination = stat("ListKeysAveragePagination",
        "average pagination for listKeys");
    listKeysAclCheckLatencyNs = stat("ListKeysAclCheckLatencyNs",
        "ACLs check latency in listKeys");
    listKeysResolveBucketLatencyNs = stat("ListKeysResolveBucketLatencyNs",
        "resolveBucketLink latency in listKeys");
    deleteKeyFailureLatencyNs = stat("DeleteKeyFailureLatencyNs",
        "deleteKeyFailure latency in nano seconds");
    deleteKeySuccessLatencyNs = stat("DeleteKeySuccessLatencyNs",
        "deleteKeySuccess latency in nano seconds");
    deleteKeysResolveBucketLatencyNs = stat("DeleteKeysResolveBucketLatencyNs",
        "resolveBucketLink latency in deleteKeys");
    deleteKeysAclCheckLatencyNs = stat("DeleteKeysAclCheckLatencyNs",
        "ACLs check latency in deleteKeys");
    deleteKeyResolveBucketAndAclCheckLatencyNs = stat("DeleteKeyResolveBucketAndAclCheckLatencyNs",
        "resolveBucketLink and ACLs check latency in deleteKey");
    listKeysReadFromRocksDbLatencyNs = stat("ListKeysReadFromRocksDbLatencyNs",
        "readFromRockDb latency in listKeys");
    getObjectTaggingResolveBucketLatencyNs = stat("GetObjectTaggingResolveBucketLatencyNs",
        "resolveBucketLink latency in getObjectTagging");
    getObjectTaggingAclCheckLatencyNs = stat("GetObjectTaggingAclCheckLatencyNs",
        "ACLs check in getObjectTagging");
    getBucketTaggingResolveBucketLatencyNs = stat("GetBucketTaggingResolveBucketLatencyNs",
        "resolveBucketLink latency in getBucketTagging");
    getBucketTaggingAclCheckLatencyNs = stat("GetBucketTaggingAclCheckLatencyNs",
        "ACLs check latency in getBucketTagging");
    getBucketTaggingLatencyNs = stat("GetBucketTaggingLatencyNs",
        "End-to-end latency in getBucketTagging");
    createKeyResolveBucketAndAclCheckLatencyNs = stat("CreateKeyResolveBucketAndAclCheckLatencyNs",
        "ResolveBucketLink and ACL check latency for createKey in nanoseconds");
    createKeyQuotaCheckLatencyNs = stat("CreateKeyQuotaCheckLatencyNs",
        "check quota for createKey in nanoseconds");
    createKeyAllocateBlockLatencyNs = stat("CreateKeyAllocateBlockLatencyNs",
        "Block allocation latency for createKey in nanoseconds");
    createKeyFailureLatencyNs = stat("CreateKeyFailureLatencyNs",
        "createKeyFailure latency in nanoseconds");
    createKeySuccessLatencyNs = stat("CreateKeySuccessLatencyNs",
        "creteKeySuccess latency in nanoseconds");
  }

  private static ConcurrentMutableRate stat(String name, String description) {
    return new ConcurrentMutableRate(name, description, false);
  }

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

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(SOURCE_NAME);
    lookupLatencyNs.snapshot(builder, all);
    lookupReadKeyInfoLatencyNs.snapshot(builder, all);
    lookupGenerateBlockTokenLatencyNs.snapshot(builder, all);
    lookupRefreshLocationLatencyNs.snapshot(builder, all);
    lookupAclCheckLatencyNs.snapshot(builder, all);
    lookupResolveBucketLatencyNs.snapshot(builder, all);
    getKeyInfoLatencyNs.snapshot(builder, all);
    getKeyInfoReadKeyInfoLatencyNs.snapshot(builder, all);
    getKeyInfoGenerateBlockTokenLatencyNs.snapshot(builder, all);
    getKeyInfoRefreshLocationLatencyNs.snapshot(builder, all);
    getKeyInfoAclCheckLatencyNs.snapshot(builder, all);
    getKeyInfoSortDatanodesLatencyNs.snapshot(builder, all);
    allocateBlockSortDatanodesLatencyNs.snapshot(builder, all);
    getKeyInfoResolveBucketLatencyNs.snapshot(builder, all);
    s3VolumeContextLatencyNs.snapshot(builder, all);
    forceContainerCacheRefresh.snapshot(builder, all);
    checkAccessLatencyNs.snapshot(builder, all);
    listKeysLatencyNs.snapshot(builder, all);
    validateRequestLatencyNs.snapshot(builder, all);
    validateResponseLatencyNs.snapshot(builder, all);
    preExecuteLatencyNs.snapshot(builder, all);
    submitToRatisLatencyNs.snapshot(builder, all);
    createRatisRequestLatencyNs.snapshot(builder, all);
    createOmResponseLatencyNs.snapshot(builder, all);
    validateAndUpdateCacheLatencyNs.snapshot(builder, all);
    listKeysAveragePagination.snapshot(builder, all);
    listKeysAclCheckLatencyNs.snapshot(builder, all);
    listKeysResolveBucketLatencyNs.snapshot(builder, all);
    deleteKeyFailureLatencyNs.snapshot(builder, all);
    deleteKeySuccessLatencyNs.snapshot(builder, all);
    deleteKeysResolveBucketLatencyNs.snapshot(builder, all);
    deleteKeysAclCheckLatencyNs.snapshot(builder, all);
    deleteKeyResolveBucketAndAclCheckLatencyNs.snapshot(builder, all);
    listKeysReadFromRocksDbLatencyNs.snapshot(builder, all);
    getObjectTaggingResolveBucketLatencyNs.snapshot(builder, all);
    getObjectTaggingAclCheckLatencyNs.snapshot(builder, all);
    getBucketTaggingResolveBucketLatencyNs.snapshot(builder, all);
    getBucketTaggingAclCheckLatencyNs.snapshot(builder, all);
    getBucketTaggingLatencyNs.snapshot(builder, all);
    createKeyResolveBucketAndAclCheckLatencyNs.snapshot(builder, all);
    createKeyQuotaCheckLatencyNs.snapshot(builder, all);
    createKeyAllocateBlockLatencyNs.snapshot(builder, all);
    createKeyFailureLatencyNs.snapshot(builder, all);
    createKeySuccessLatencyNs.snapshot(builder, all);
    listKeysOpsPerSec.snapshot(builder, all);
    directoryDeletingServiceLatencyMs.snapshot(builder, all);
    keyDeletingServiceLatencyMs.snapshot(builder, all);
    openKeyCleanupServiceLatencyMs.snapshot(builder, all);
    snapshotDefragServiceFullLatencyMs.snapshot(builder, all);
    snapshotDefragServiceIncLatencyMs.snapshot(builder, all);
  }

  public void addLookupLatency(long latencyInNs) {
    lookupLatencyNs.add(latencyInNs);
  }

  ConcurrentMutableRate getLookupRefreshLocationLatencyNs() {
    return lookupRefreshLocationLatencyNs;
  }

  ConcurrentMutableRate getLookupGenerateBlockTokenLatencyNs() {
    return lookupGenerateBlockTokenLatencyNs;
  }

  ConcurrentMutableRate getLookupReadKeyInfoLatencyNs() {
    return lookupReadKeyInfoLatencyNs;
  }

  ConcurrentMutableRate getLookupAclCheckLatencyNs() {
    return lookupAclCheckLatencyNs;
  }

  public void addS3VolumeContextLatencyNs(long latencyInNs) {
    s3VolumeContextLatencyNs.add(latencyInNs);
  }

  ConcurrentMutableRate getLookupResolveBucketLatencyNs() {
    return lookupResolveBucketLatencyNs;
  }

  public void addGetKeyInfoLatencyNs(long value) {
    getKeyInfoLatencyNs.add(value);
  }

  ConcurrentMutableRate getGetKeyInfoAclCheckLatencyNs() {
    return getKeyInfoAclCheckLatencyNs;
  }

  ConcurrentMutableRate getGetKeyInfoGenerateBlockTokenLatencyNs() {
    return getKeyInfoGenerateBlockTokenLatencyNs;
  }

  ConcurrentMutableRate getGetKeyInfoReadKeyInfoLatencyNs() {
    return getKeyInfoReadKeyInfoLatencyNs;
  }

  ConcurrentMutableRate getGetKeyInfoRefreshLocationLatencyNs() {
    return getKeyInfoRefreshLocationLatencyNs;
  }

  ConcurrentMutableRate getGetKeyInfoResolveBucketLatencyNs() {
    return getKeyInfoResolveBucketLatencyNs;
  }

  ConcurrentMutableRate getGetKeyInfoSortDatanodesLatencyNs() {
    return getKeyInfoSortDatanodesLatencyNs;
  }

  ConcurrentMutableRate getAllocateBlockSortDatanodesLatencyNs() {
    return allocateBlockSortDatanodesLatencyNs;
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

  public ConcurrentMutableRate getValidateRequestLatencyNs() {
    return validateRequestLatencyNs;
  }

  public ConcurrentMutableRate getValidateResponseLatencyNs() {
    return validateResponseLatencyNs;
  }

  public ConcurrentMutableRate getPreExecuteLatencyNs() {
    return preExecuteLatencyNs;
  }

  public ConcurrentMutableRate getSubmitToRatisLatencyNs() {
    return submitToRatisLatencyNs;
  }

  public ConcurrentMutableRate getCreateRatisRequestLatencyNs() {
    return createRatisRequestLatencyNs;
  }

  public ConcurrentMutableRate getCreateOmResponseLatencyNs() {
    return createOmResponseLatencyNs;
  }

  public ConcurrentMutableRate getValidateAndUpdateCacheLatencyNs() {
    return validateAndUpdateCacheLatencyNs;
  }

  public void setListKeysAveragePagination(long keyCount) {
    listKeysAveragePagination.add(keyCount);
  }

  public void setListKeysOpsPerSec(float opsPerSec) {
    listKeysOpsPerSec.set(opsPerSec);
  }

  ConcurrentMutableRate getListKeysAclCheckLatencyNs() {
    return listKeysAclCheckLatencyNs;
  }

  ConcurrentMutableRate getListKeysResolveBucketLatencyNs() {
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

  public ConcurrentMutableRate getDeleteKeyResolveBucketAndAclCheckLatencyNs() {
    return deleteKeyResolveBucketAndAclCheckLatencyNs;
  }

  public ConcurrentMutableRate getCreateKeyResolveBucketAndAclCheckLatencyNs() {
    return createKeyResolveBucketAndAclCheckLatencyNs;
  }

  public void addCreateKeyQuotaCheckLatencyNs(long latencyInNs) {
    createKeyQuotaCheckLatencyNs.add(latencyInNs);
  }

  public ConcurrentMutableRate getCreateKeyAllocateBlockLatencyNs() {
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

  public ConcurrentMutableRate getGetObjectTaggingResolveBucketLatencyNs() {
    return getObjectTaggingResolveBucketLatencyNs;
  }

  public ConcurrentMutableRate getGetObjectTaggingAclCheckLatencyNs() {
    return getObjectTaggingAclCheckLatencyNs;
  }

  public void addGetObjectTaggingLatencyNs(long latencyInNs) {
    getObjectTaggingAclCheckLatencyNs.add(latencyInNs);
  }

  public ConcurrentMutableRate getGetBucketTaggingResolveBucketLatencyNs() {
    return getBucketTaggingResolveBucketLatencyNs;
  }

  public ConcurrentMutableRate getGetBucketTaggingAclCheckLatencyNs() {
    return getBucketTaggingAclCheckLatencyNs;
  }

  public void addGetBucketTaggingLatencyNs(long latencyInNs) {
    getBucketTaggingLatencyNs.add(latencyInNs);
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

  public void setSnapshotDefragServiceFullLatencyMs(long latencyInMs) {
    snapshotDefragServiceFullLatencyMs.set(latencyInMs);
  }

  public void setSnapshotDefragServiceIncLatencyMs(long latencyInMs) {
    snapshotDefragServiceIncLatencyMs.set(latencyInMs);
  }
}
