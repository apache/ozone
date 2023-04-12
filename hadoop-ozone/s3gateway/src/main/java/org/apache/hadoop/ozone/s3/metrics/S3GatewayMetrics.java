/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.metrics;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.util.Time;

/**
 * This class maintains S3 Gateway related metrics.
 */
@InterfaceAudience.Private
@Metrics(about = "S3 Gateway Metrics", context = OzoneConsts.OZONE)
public final class S3GatewayMetrics implements MetricsSource {

  public static final String SOURCE_NAME =
      S3GatewayMetrics.class.getSimpleName();

  private MetricsRegistry registry;
  private static S3GatewayMetrics instance;

  // BucketEndpoint
  private @Metric MutableCounterLong getBucketSuccess;
  private @Metric MutableCounterLong getBucketFailure;
  private @Metric MutableCounterLong createBucketSuccess;
  private @Metric MutableCounterLong createBucketFailure;
  private @Metric MutableCounterLong headBucketSuccess;
  private @Metric MutableCounterLong deleteBucketSuccess;
  private @Metric MutableCounterLong deleteBucketFailure;
  private @Metric MutableCounterLong getAclSuccess;
  private @Metric MutableCounterLong getAclFailure;
  private @Metric MutableCounterLong putAclSuccess;
  private @Metric MutableCounterLong putAclFailure;
  private @Metric MutableCounterLong listMultipartUploadsSuccess;
  private @Metric MutableCounterLong listMultipartUploadsFailure;

  // RootEndpoint
  private @Metric MutableCounterLong listS3BucketsSuccess;
  private @Metric MutableCounterLong listS3BucketsFailure;

  // ObjectEndpoint
  private @Metric MutableCounterLong createMultipartKeySuccess;
  private @Metric MutableCounterLong createMultipartKeyFailure;
  private @Metric MutableCounterLong copyObjectSuccess;
  private @Metric MutableCounterLong copyObjectFailure;
  private @Metric MutableCounterLong createKeySuccess;
  private @Metric MutableCounterLong createKeyFailure;
  private @Metric MutableCounterLong listPartsSuccess;
  private @Metric MutableCounterLong listPartsFailure;
  private @Metric MutableCounterLong getKeySuccess;
  private @Metric MutableCounterLong getKeyFailure;
  private @Metric MutableCounterLong headKeySuccess;
  private @Metric MutableCounterLong headKeyFailure;
  private @Metric MutableCounterLong initMultipartUploadSuccess;
  private @Metric MutableCounterLong initMultipartUploadFailure;
  private @Metric MutableCounterLong completeMultipartUploadSuccess;
  private @Metric MutableCounterLong completeMultipartUploadFailure;
  private @Metric MutableCounterLong abortMultipartUploadSuccess;
  private @Metric MutableCounterLong abortMultipartUploadFailure;
  private @Metric MutableCounterLong deleteKeySuccess;
  private @Metric MutableCounterLong deleteKeyFailure;

  // S3 Gateway Latency Metrics
  // BucketEndpoint

  @Metric(about = "Latency for successfully retrieving an S3 bucket in " +
      "nanoseconds")
  private MutableRate getBucketSuccessLatencyNs;

  @Metric(about = "Latency for failing to retrieve an S3 bucket in nanoseconds")
  private MutableRate getBucketFailureLatencyNs;

  @Metric(about = "Latency for successfully creating an S3 bucket in " +
      "nanoseconds")
  private MutableRate createBucketSuccessLatencyNs;

  @Metric(about = "Latency for failing to create an S3 bucket in nanoseconds")
  private MutableRate createBucketFailureLatencyNs;

  @Metric(about = "Latency for successfully checking the existence of an " +
      "S3 bucket in nanoseconds")
  private MutableRate headBucketSuccessLatencyNs;

  @Metric(about = "Latency for successfully deleting an S3 bucket in " +
      "nanoseconds")
  private MutableRate deleteBucketSuccessLatencyNs;

  @Metric(about = "Latency for failing to delete an S3 bucket in nanoseconds")
  private MutableRate deleteBucketFailureLatencyNs;

  @Metric(about = "Latency for successfully retrieving an S3 bucket ACL " +
      "in nanoseconds")
  private MutableRate getAclSuccessLatencyNs;

  @Metric(about = "Latency for failing to retrieve an S3 bucket ACL " +
      "in nanoseconds")
  private MutableRate getAclFailureLatencyNs;

  @Metric(about = "Latency for successfully setting an S3 bucket ACL " +
      "in nanoseconds")
  private MutableRate putAclSuccessLatencyNs;

  @Metric(about = "Latency for failing to set an S3 bucket ACL " +
      "in nanoseconds")
  private MutableRate putAclFailureLatencyNs;

  @Metric(about = "Latency for successfully listing multipart uploads " +
      "in nanoseconds")
  private MutableRate listMultipartUploadsSuccessLatencyNs;

  @Metric(about = "Latency for failing to list multipart uploads " +
      "in nanoseconds")
  private MutableRate listMultipartUploadsFailureLatencyNs;

  // RootEndpoint

  @Metric(about = "Latency for successfully listing S3 buckets " +
      "in nanoseconds")
  private MutableRate listS3BucketsSuccessLatencyNs;

  @Metric(about = "Latency for failing to list S3 buckets " +
      "in nanoseconds")
  private MutableRate listS3BucketsFailureLatencyNs;

  // ObjectEndpoint

  @Metric(about = "Latency for successfully creating a multipart object key " +
      "in nanoseconds")
  private MutableRate createMultipartKeySuccessLatencyNs;

  @Metric(about = "Latency for failing to create a multipart object key in " +
      "nanoseconds")
  private MutableRate createMultipartKeyFailureLatencyNs;

  @Metric(about = "Latency for successfully copying an S3 object in " +
      "nanoseconds")
  private MutableRate copyObjectSuccessLatencyNs;

  @Metric(about = "Latency for failing to copy an S3 object in nanoseconds")
  private MutableRate copyObjectFailureLatencyNs;

  @Metric(about = "Latency for successfully creating an S3 object key in " +
      "nanoseconds")
  private MutableRate createKeySuccessLatencyNs;

  @Metric(about = "Latency for failing to create an S3 object key in " +
      "nanoseconds")
  private MutableRate createKeyFailureLatencyNs;

  @Metric(about = "Latency for successfully listing parts of a multipart " +
      "upload in nanoseconds")
  private MutableRate listPartsSuccessLatencyNs;

  @Metric(about = "Latency for failing to list parts of a multipart upload " +
      "in nanoseconds")
  private MutableRate listPartsFailureLatencyNs;

  @Metric(about = "Latency for successfully retrieving an S3 object in " +
      "nanoseconds")
  private MutableRate getKeySuccessLatencyNs;

  @Metric(about = "Latency for failing to retrieve an S3 object in nanoseconds")
  private MutableRate getKeyFailureLatencyNs;

  @Metric(about = "Latency for successfully retrieving metadata for an S3 " +
      "object in nanoseconds")
  private MutableRate headKeySuccessLatencyNs;

  @Metric(about = "Latency for failing to retrieve metadata for an S3 object " +
      "in nanoseconds")
  private MutableRate headKeyFailureLatencyNs;

  @Metric(about = "Latency for successfully initiating a multipart upload in " +
      "nanoseconds")
  private MutableRate initMultipartUploadSuccessLatencyNs;

  @Metric(about = "Latency for failing to initiate a multipart upload in " +
      "nanoseconds")
  private MutableRate initMultipartUploadFailureLatencyNs;

  @Metric(about = "Latency for successfully completing a multipart upload in " +
      "nanoseconds")
  private MutableRate completeMultipartUploadSuccessLatencyNs;

  @Metric(about = "Latency for failing to complete a multipart upload in " +
      "nanoseconds")
  private MutableRate completeMultipartUploadFailureLatencyNs;

  @Metric(about = "Latency for successfully aborting a multipart upload in " +
      "nanoseconds")
  private MutableRate abortMultipartUploadSuccessLatencyNs;

  @Metric(about = "Latency for failing to abort a multipart upload in " +
      "nanoseconds")
  private MutableRate abortMultipartUploadFailureLatencyNs;

  @Metric(about = "Latency for successfully deleting an S3 object in " +
      "nanoseconds")
  private MutableRate deleteKeySuccessLatencyNs;

  @Metric(about = "Latency for failing to delete an S3 object in nanoseconds")
  private MutableRate deleteKeyFailureLatencyNs;

  /**
   * Private constructor.
   */
  private S3GatewayMetrics() {
    this.registry = new MetricsRegistry(SOURCE_NAME);
  }

  /**
   * Create and returns S3 Gateway Metrics instance.
   *
   * @return S3GatewayMetrics
   */
  public static synchronized S3GatewayMetrics create() {
    if (instance != null) {
      return instance;
    }
    MetricsSystem ms = DefaultMetricsSystem.instance();
    instance = ms.register(SOURCE_NAME, "S3 Gateway Metrics",
        new S3GatewayMetrics());
    return instance;
  }

  /**
   * Unregister the metrics instance.
   */
  public static void unRegister() {
    instance = null;
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME);

    // BucketEndpoint
    getBucketSuccess.snapshot(recordBuilder, true);
    getBucketSuccessLatencyNs.snapshot(recordBuilder, true);
    getBucketFailure.snapshot(recordBuilder, true);
    getBucketFailureLatencyNs.snapshot(recordBuilder, true);
    createBucketSuccess.snapshot(recordBuilder, true);
    createBucketSuccessLatencyNs.snapshot(recordBuilder, true);
    createBucketFailure.snapshot(recordBuilder, true);
    createBucketFailureLatencyNs.snapshot(recordBuilder, true);
    headBucketSuccess.snapshot(recordBuilder, true);
    headBucketSuccessLatencyNs.snapshot(recordBuilder, true);
    deleteBucketSuccess.snapshot(recordBuilder, true);
    deleteBucketSuccessLatencyNs.snapshot(recordBuilder, true);
    deleteBucketFailure.snapshot(recordBuilder, true);
    deleteBucketFailureLatencyNs.snapshot(recordBuilder, true);
    getAclSuccess.snapshot(recordBuilder, true);
    getAclSuccessLatencyNs.snapshot(recordBuilder, true);
    getAclFailure.snapshot(recordBuilder, true);
    getAclFailureLatencyNs.snapshot(recordBuilder, true);
    putAclSuccess.snapshot(recordBuilder, true);
    putAclSuccessLatencyNs.snapshot(recordBuilder, true);
    putAclFailure.snapshot(recordBuilder, true);
    putAclFailureLatencyNs.snapshot(recordBuilder, true);
    listMultipartUploadsSuccess.snapshot(recordBuilder, true);
    listMultipartUploadsSuccessLatencyNs.snapshot(recordBuilder, true);
    listMultipartUploadsFailure.snapshot(recordBuilder, true);
    listMultipartUploadsFailureLatencyNs.snapshot(recordBuilder, true);

    // RootEndpoint
    listS3BucketsSuccess.snapshot(recordBuilder, true);
    listS3BucketsSuccessLatencyNs.snapshot(recordBuilder, true);
    listS3BucketsFailure.snapshot(recordBuilder, true);
    listS3BucketsFailureLatencyNs.snapshot(recordBuilder, true);

    // ObjectEndpoint
    createMultipartKeySuccess.snapshot(recordBuilder, true);
    createMultipartKeySuccessLatencyNs.snapshot(recordBuilder, true);
    createMultipartKeyFailure.snapshot(recordBuilder, true);
    createMultipartKeyFailureLatencyNs.snapshot(recordBuilder, true);
    copyObjectSuccess.snapshot(recordBuilder, true);
    copyObjectSuccessLatencyNs.snapshot(recordBuilder, true);
    copyObjectFailure.snapshot(recordBuilder, true);
    copyObjectFailureLatencyNs.snapshot(recordBuilder, true);
    createKeySuccess.snapshot(recordBuilder, true);
    createKeySuccessLatencyNs.snapshot(recordBuilder, true);
    createKeyFailure.snapshot(recordBuilder, true);
    createKeyFailureLatencyNs.snapshot(recordBuilder, true);
    listPartsSuccess.snapshot(recordBuilder, true);
    listPartsSuccessLatencyNs.snapshot(recordBuilder, true);
    listPartsFailure.snapshot(recordBuilder, true);
    listPartsFailureLatencyNs.snapshot(recordBuilder, true);
    getKeySuccess.snapshot(recordBuilder, true);
    getKeySuccessLatencyNs.snapshot(recordBuilder, true);
    getKeyFailure.snapshot(recordBuilder, true);
    getKeyFailureLatencyNs.snapshot(recordBuilder, true);
    headKeySuccess.snapshot(recordBuilder, true);
    headKeySuccessLatencyNs.snapshot(recordBuilder, true);
    headKeyFailure.snapshot(recordBuilder, true);
    headKeyFailureLatencyNs.snapshot(recordBuilder, true);
    initMultipartUploadSuccess.snapshot(recordBuilder, true);
    initMultipartUploadSuccessLatencyNs.snapshot(recordBuilder, true);
    initMultipartUploadFailure.snapshot(recordBuilder, true);
    initMultipartUploadFailureLatencyNs.snapshot(recordBuilder, true);
    completeMultipartUploadSuccess.snapshot(recordBuilder, true);
    completeMultipartUploadSuccessLatencyNs.snapshot(recordBuilder, true);
    completeMultipartUploadFailure.snapshot(recordBuilder, true);
    completeMultipartUploadFailureLatencyNs.snapshot(recordBuilder, true);
    abortMultipartUploadSuccess.snapshot(recordBuilder, true);
    abortMultipartUploadSuccessLatencyNs.snapshot(recordBuilder, true);
    abortMultipartUploadFailure.snapshot(recordBuilder, true);
    abortMultipartUploadFailureLatencyNs.snapshot(recordBuilder, true);
    deleteKeySuccess.snapshot(recordBuilder, true);
    deleteKeySuccessLatencyNs.snapshot(recordBuilder, true);
    deleteKeyFailure.snapshot(recordBuilder, true);
    deleteKeyFailureLatencyNs.snapshot(recordBuilder, true);
  }

  // INC and UPDATE
  // BucketEndpoint

  public void updateGetBucketSuccessStats(long startNanos) {
    getBucketSuccess.incr();
    getBucketSuccessLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateGetBucketFailureStats(long startNanos) {
    getBucketFailure.incr();
    getBucketFailureLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateCreateBucketSuccessStats(long startNanos) {
    createBucketSuccess.incr();
    createBucketSuccessLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateCreateBucketFailureStats(long startNanos) {
    createBucketFailure.incr();
    createBucketFailureLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateHeadBucketSuccessStats(long startNanos) {
    headBucketSuccess.incr();
    headBucketSuccessLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateDeleteBucketSuccessStats(long startNanos) {
    deleteBucketSuccess.incr();
    deleteBucketSuccessLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateDeleteBucketFailureStats(long startNanos) {
    deleteBucketFailure.incr();
    deleteBucketFailureLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateGetAclSuccessStats(long startNanos) {
    getAclSuccess.incr();
    getAclSuccessLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateGetAclFailureStats(long startNanos) {
    getAclFailure.incr();
    getAclFailureLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updatePutAclSuccessStats(long startNanos) {
    putAclSuccess.incr();
    putAclSuccessLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updatePutAclFailureStats(long startNanos) {
    putAclFailure.incr();
    putAclFailureLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateListMultipartUploadsSuccessStats(long startNanos) {
    listMultipartUploadsSuccess.incr();
    listMultipartUploadsSuccessLatencyNs.add(
        Time.monotonicNowNanos() - startNanos);
  }

  public void updateListMultipartUploadsFailureStats(long startNanos) {
    listMultipartUploadsFailure.incr();
    listMultipartUploadsFailureLatencyNs.add(
        Time.monotonicNowNanos() - startNanos);
  }

  // RootEndpoint

  public void updateListS3BucketsSuccessStats(long startNanos) {
    listS3BucketsSuccess.incr();
    listS3BucketsSuccessLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateListS3BucketsFailureStats(long startNanos) {
    listS3BucketsFailure.incr();
    listS3BucketsFailureLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  // ObjectEndpoint

  public void updateCreateMultipartKeySuccessStats(long startNanos) {
    createMultipartKeySuccess.incr();
    createMultipartKeySuccessLatencyNs.add(
        Time.monotonicNowNanos() - startNanos);
  }

  public void updateCreateMultipartKeyFailureStats(long startNanos) {
    createMultipartKeyFailure.incr();
    createMultipartKeyFailureLatencyNs.add(
        Time.monotonicNowNanos() - startNanos);
  }

  public void updateCopyObjectSuccessStats(long startNanos) {
    copyObjectSuccess.incr();
    copyObjectSuccessLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateCopyObjectFailureStats(long startNanos) {
    copyObjectFailure.incr();
    copyObjectFailureLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateCreateKeySuccessStats(long startNanos) {
    createKeySuccess.incr();
    createKeySuccessLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateCreateKeyFailureStats(long startNanos) {
    createKeyFailure.incr();
    createKeyFailureLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateListPartsSuccessStats(long startNanos) {
    listPartsSuccess.incr();
    listPartsSuccessLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateListPartsFailureStats(long startNanos) {
    listPartsFailure.incr();
    listPartsFailureLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateGetKeySuccessStats(long startNanos) {
    getKeySuccess.incr();
    getKeySuccessLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateGetKeyFailureStats(long startNanos) {
    getKeyFailure.incr();
    getKeyFailureLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateHeadKeySuccessStats(long startNanos) {
    headKeySuccess.incr();
    headKeySuccessLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateHeadKeyFailureStats(long startNanos) {
    headKeyFailure.incr();
    headKeyFailureLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateInitMultipartUploadSuccessStats(long startNanos) {
    initMultipartUploadSuccess.incr();
    initMultipartUploadSuccessLatencyNs.add(
        Time.monotonicNowNanos() - startNanos);
  }

  public void updateInitMultipartUploadFailureStats(long startNanos) {
    initMultipartUploadFailure.incr();
    initMultipartUploadFailureLatencyNs.add(
        Time.monotonicNowNanos() - startNanos);
  }

  public void updateCompleteMultipartUploadSuccessStats(long startNanos) {
    completeMultipartUploadSuccess.incr();
    completeMultipartUploadSuccessLatencyNs.add(
        Time.monotonicNowNanos() - startNanos);
  }

  public void updateCompleteMultipartUploadFailureStats(long startNanos) {
    completeMultipartUploadFailure.incr();
    completeMultipartUploadFailureLatencyNs.add(
        Time.monotonicNowNanos() - startNanos);
  }

  public void updateAbortMultipartUploadSuccessStats(long startNanos) {
    abortMultipartUploadSuccess.incr();
    abortMultipartUploadSuccessLatencyNs.add(
        Time.monotonicNowNanos() - startNanos);
  }

  public void updateAbortMultipartUploadFailureStats(long startNanos) {
    abortMultipartUploadFailure.incr();
    abortMultipartUploadFailureLatencyNs.add(
        Time.monotonicNowNanos() - startNanos);
  }

  public void updateDeleteKeySuccessStats(long startNanos) {
    deleteKeySuccess.incr();
    deleteKeySuccessLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateDeleteKeyFailureStats(long startNanos) {
    deleteKeyFailure.incr();
    deleteKeyFailureLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  // GET
  public long getListS3BucketsSuccess() {
    return listS3BucketsSuccess.value();
  }

  public long getHeadBucketSuccess() {
    return headBucketSuccess.value();
  }

  public long getHeadKeySuccess() {
    return headKeySuccess.value();
  }

  public long getGetBucketSuccess() {
    return getBucketSuccess.value();
  }

  public long getGetBucketFailure() {
    return getBucketFailure.value();
  }

  public long getCreateBucketSuccess() {
    return createBucketSuccess.value();
  }

  public long getCreateBucketFailure() {
    return createBucketFailure.value();
  }

  public long getDeleteBucketSuccess() {
    return deleteBucketSuccess.value();
  }

  public long getDeleteBucketFailure() {
    return deleteBucketFailure.value();
  }

  public long getGetAclSuccess() {
    return getAclSuccess.value();
  }

  public long getGetAclFailure() {
    return getAclFailure.value();
  }

  public long getPutAclSuccess() {
    return putAclSuccess.value();
  }

  public long getPutAclFailure() {
    return putAclFailure.value();
  }

  public long getListMultipartUploadsSuccess() {
    return listMultipartUploadsSuccess.value();
  }

  public long getListMultipartUploadsFailure() {
    return listMultipartUploadsFailure.value();
  }

  public long getCreateMultipartKeySuccess() {
    return createMultipartKeySuccess.value();
  }

  public long getCreateMultipartKeyFailure() {
    return createMultipartKeyFailure.value();
  }

  public long getCompleteMultiPartUploadSuccess() {
    return completeMultipartUploadSuccess.value();
  }

  public long getCompleteMultiPartUploadFailure() {
    return completeMultipartUploadFailure.value();
  }

  public long getListPartsSuccess() {
    return listPartsSuccess.value();
  }

  public long getListPartsFailure() {
    return listPartsFailure.value();
  }

  public long getCopyObjectSuccess() {
    return copyObjectSuccess.value();
  }

  public long getCopyObjectFailure() {
    return copyObjectFailure.value();
  }

  public long getCreateKeyFailure() {
    return createKeyFailure.value();
  }

  public long getCreateKeySuccess() {
    return createKeySuccess.value();
  }

  public long getInitMultiPartUploadSuccess() {
    return initMultipartUploadSuccess.value();
  }

  public long getInitMultiPartUploadFailure() {
    return initMultipartUploadFailure.value();
  }

  public long getDeleteKeySuccess() {
    return deleteKeySuccess.value();
  }

  public long getDeleteKeyFailure() {
    return deleteKeyFailure.value();
  }

  public long getGetKeyFailure() {
    return getKeyFailure.value();
  }

  public long getGetKeySuccess() {
    return getKeySuccess.value();
  }

  public long getAbortMultiPartUploadSuccess() {
    return abortMultipartUploadSuccess.value();
  }

  public long getAbortMultiPartUploadFailure() {
    return abortMultipartUploadFailure.value();
  }

  public long getHeadKeyFailure() {
    return headKeyFailure.value();
  }

  public long getListS3BucketsFailure() {
    return listS3BucketsFailure.value();
  }
}
