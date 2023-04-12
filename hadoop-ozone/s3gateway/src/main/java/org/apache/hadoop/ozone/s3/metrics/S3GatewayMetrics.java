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
  private @Metric MutableCounterLong initMultiPartUploadSuccess;
  private @Metric MutableCounterLong initMultiPartUploadFailure;
  private @Metric MutableCounterLong completeMultiPartUploadSuccess;
  private @Metric MutableCounterLong completeMultiPartUploadFailure;
  private @Metric MutableCounterLong abortMultiPartUploadSuccess;
  private @Metric MutableCounterLong abortMultiPartUploadFailure;
  private @Metric MutableCounterLong deleteKeySuccess;
  private @Metric MutableCounterLong deleteKeyFailure;

  // S3 Gateway Latency Metrics
  // ObjectEndpoint

  @Metric(about = "Latency for successfully initiating a multipart upload " +
      "for an S3 object in nanoseconds")
  private MutableRate initMultipartUploadSuccessLatencyNs;

  @Metric(about = "Latency for failing to initiate a multipart upload for an " +
      "S3 object in nanoseconds")
  private MutableRate initMultipartUploadFailureLatencyNs;

  @Metric(about = "Latency for successfully creating a multipart key for an " +
      "S3 object in nanoseconds")
  private MutableRate createMultipartKeySuccessLatencyNs;

  @Metric(about = "Latency for failing to create a multipart key for an S3 " +
      "object in nanoseconds")
  private MutableRate createMultipartKeyFailureLatencyNs;

  @Metric(about = "Latency for successfully completing a multipart upload " +
      "for an S3 object in nanoseconds")
  private MutableRate completeMultipartUploadSuccessLatencyNs;

  @Metric(about = "Latency for failing to complete a multipart upload for an " +
      "S3 object in nanoseconds")
  private MutableRate completeMultipartUploadFailureLatencyNs;

  @Metric(about = "Latency for aborting a multipart upload for an S3 object " +
      "in nanoseconds")
  private MutableRate abortMultipartUploadLatencyNs;

  @Metric(about = "Latency for successfully copying an S3 object in " +
      "nanoseconds")
  private MutableRate copyObjectSuccessLatencyNs;

  @Metric(about = "Latency for failing to copy an S3 object in nanoseconds")
  private MutableRate copyObjectFailureLatencyNs;

  @Metric(about = "Latency for successfully listing parts for a multipart " +
      "upload of an S3 object in nanoseconds")
  private MutableRate listPartsSuccessLatencyNs;

  @Metric(about = "Latency for failing to list parts for a multipart upload " +
      "of an S3 object in nanoseconds")
  private MutableRate listPartsFailureLatencyNs;

  @Metric(about = "Latency for creating an S3 key in nanoseconds")
  private MutableRate createKeyLatencyNs;

  @Metric(about = "Latency for successfully retrieving an S3 key in " +
      "nanoseconds")
  private MutableRate getKeySuccessLatencyNs;

  @Metric(about = "Latency for failing to retrieve an S3 key in nanoseconds")
  private MutableRate getKeyFailureLatencyNs;

  @Metric(about = "Latency for retrieving the metadata for an S3 key in " +
      "nanoseconds")
  private MutableRate headKeyLatencyNs;

  @Metric(about = "Latency for deleting an S3 key in nanoseconds")
  private MutableRate deleteKeyLatencyNs;

  // BucketEndpoint

  @Metric(about = "Latency for retrieving the access control list (ACL) of " +
      "an S3 bucket in nanoseconds")
  private MutableRate getAclLatencyNs;

  @Metric(about = "Latency for successfully setting the access control list " +
      "(ACL) of an S3 bucket in nanoseconds")
  private MutableRate putAclSuccessLatencyNs;

  @Metric(about = "Latency for failing to set the access control list (ACL) " +
      "of an S3 bucket in nanoseconds")
  private MutableRate putAclFailureLatencyNs;

  @Metric(about = "Latency for creating an S3 bucket in nanoseconds")
  private MutableRate createBucketLatencyNs;

  @Metric(about = "Latency for retrieving an S3 bucket in nanoseconds")
  private MutableRate getBucketLatencyNs;

  @Metric(about = "Latency for retrieving the metadata of an S3 bucket in " +
      "nanoseconds")
  private MutableRate headBucketLatencyNs;

  @Metric(about = "Latency for deleting an S3 bucket in nanoseconds")
  private MutableRate deleteBucketLatencyNs;

  @Metric(about = "Latency for successfully listing multipart uploads for an " +
      "S3 bucket in nanoseconds")
  private MutableRate listMultipartUploadsSuccessLatencyNs;

  @Metric(about = "Latency for failing to list multipart uploads for an S3 " +
      "bucket in nanoseconds")
  private MutableRate listMultipartUploadsFailureLatencyNs;

  // RootEndpoint

  @Metric(about = "Latency for successfully listing S3 buckets in nanoseconds")
  private MutableRate listS3BucketsSuccessLatencyNs;

  @Metric(about = "Latency for failing to list S3 buckets in nanoseconds")
  private MutableRate listS3BucketsFailureLatencyNs;

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
    getBucketFailure.snapshot(recordBuilder, true);
    createBucketSuccess.snapshot(recordBuilder, true);
    createBucketFailure.snapshot(recordBuilder, true);
    headBucketSuccess.snapshot(recordBuilder, true);
    deleteBucketSuccess.snapshot(recordBuilder, true);
    deleteBucketFailure.snapshot(recordBuilder, true);
    getAclSuccess.snapshot(recordBuilder, true);
    getAclFailure.snapshot(recordBuilder, true);
    putAclSuccess.snapshot(recordBuilder, true);
    putAclFailure.snapshot(recordBuilder, true);
    listMultipartUploadsSuccess.snapshot(recordBuilder, true);
    listMultipartUploadsFailure.snapshot(recordBuilder, true);

    // RootEndpoint
    listS3BucketsSuccess.snapshot(recordBuilder, true);
    listS3BucketsFailure.snapshot(recordBuilder, true);

    // ObjectEndpoint
    createMultipartKeySuccess.snapshot(recordBuilder, true);
    createMultipartKeyFailure.snapshot(recordBuilder, true);
    copyObjectSuccess.snapshot(recordBuilder, true);
    copyObjectFailure.snapshot(recordBuilder, true);
    createKeySuccess.snapshot(recordBuilder, true);
    createKeyFailure.snapshot(recordBuilder, true);
    listPartsSuccess.snapshot(recordBuilder, true);
    listPartsFailure.snapshot(recordBuilder, true);
    getKeySuccess.snapshot(recordBuilder, true);
    getKeyFailure.snapshot(recordBuilder, true);
    headKeySuccess.snapshot(recordBuilder, true);
    headKeyFailure.snapshot(recordBuilder, true);
    initMultiPartUploadSuccess.snapshot(recordBuilder, true);
    initMultiPartUploadFailure.snapshot(recordBuilder, true);
    completeMultiPartUploadSuccess.snapshot(recordBuilder, true);
    completeMultiPartUploadFailure.snapshot(recordBuilder, true);
    abortMultiPartUploadSuccess.snapshot(recordBuilder, true);
    abortMultiPartUploadFailure.snapshot(recordBuilder, true);
    deleteKeySuccess.snapshot(recordBuilder, true);
    deleteKeyFailure.snapshot(recordBuilder, true);

    // S3 Gateway Latency Metrics
    // ObjectEndpoint
    initMultipartUploadSuccessLatencyNs.snapshot(recordBuilder, true);
    initMultipartUploadFailureLatencyNs.snapshot(recordBuilder, true);
    createMultipartKeySuccessLatencyNs.snapshot(recordBuilder, true);
    createMultipartKeyFailureLatencyNs.snapshot(recordBuilder, true);
    completeMultipartUploadSuccessLatencyNs.snapshot(recordBuilder, true);
    completeMultipartUploadFailureLatencyNs.snapshot(recordBuilder, true);
    abortMultipartUploadLatencyNs.snapshot(recordBuilder, true);
    copyObjectSuccessLatencyNs.snapshot(recordBuilder, true);
    copyObjectFailureLatencyNs.snapshot(recordBuilder, true);
    listPartsSuccessLatencyNs.snapshot(recordBuilder, true);
    listPartsFailureLatencyNs.snapshot(recordBuilder, true);
    createKeyLatencyNs.snapshot(recordBuilder, true);
    getKeySuccessLatencyNs.snapshot(recordBuilder, true);
    getKeyFailureLatencyNs.snapshot(recordBuilder, true);
    headKeyLatencyNs.snapshot(recordBuilder, true);
    deleteKeyLatencyNs.snapshot(recordBuilder, true);

    // BucketEndpoint
    getAclLatencyNs.snapshot(recordBuilder, true);
    putAclSuccessLatencyNs.snapshot(recordBuilder, true);
    putAclFailureLatencyNs.snapshot(recordBuilder, true);
    createBucketLatencyNs.snapshot(recordBuilder, true);
    getBucketLatencyNs.snapshot(recordBuilder, true);
    headBucketLatencyNs.snapshot(recordBuilder, true);
    deleteBucketLatencyNs.snapshot(recordBuilder, true);
    listMultipartUploadsSuccessLatencyNs.snapshot(recordBuilder, true);
    listMultipartUploadsFailureLatencyNs.snapshot(recordBuilder, true);

    // RootEndpoint
    listS3BucketsSuccessLatencyNs.snapshot(recordBuilder, true);
    listS3BucketsFailureLatencyNs.snapshot(recordBuilder, true);
  }

  // INC
  public void incGetBucketSuccess() {
    getBucketSuccess.incr();
  }

  public void incGetBucketFailure() {
    getBucketFailure.incr();
  }

  public void incListS3BucketsSuccess() {
    listS3BucketsSuccess.incr();
  }


  public void incListS3BucketsFailure() {
    listS3BucketsFailure.incr();
  }


  public void incCreateBucketSuccess() {
    createBucketSuccess.incr();
  }

  public void incCreateBucketFailure() {
    createBucketFailure.incr();
  }

  public void incPutAclSuccess() {
    putAclSuccess.incr();
  }

  public void incPutAclFailure() {
    putAclFailure.incr();
  }

  public void incGetAclSuccess() {
    getAclSuccess.incr();
  }

  public void incGetAclFailure() {
    getAclFailure.incr();
  }

  public void incListMultipartUploadsSuccess() {
    listMultipartUploadsSuccess.incr();
  }

  public void incListMultipartUploadsFailure() {
    listMultipartUploadsFailure.incr();
  }

  public void incHeadBucketSuccess() {
    headBucketSuccess.incr();
  }


  public void incDeleteBucketSuccess() {
    deleteBucketSuccess.incr();
  }

  public void incDeleteBucketFailure() {
    deleteBucketFailure.incr();
  }

  public void incCreateMultipartKeySuccess() {
    createMultipartKeySuccess.incr();
  }

  public void incCreateMultipartKeyFailure() {
    createMultipartKeyFailure.incr();
  }

  public void incCopyObjectSuccess() {
    copyObjectSuccess.incr();
  }

  public void incCopyObjectFailure() {
    copyObjectFailure.incr();
  }

  public void incCreateKeySuccess() {
    createKeySuccess.incr();
  }

  public void incCreateKeyFailure() {
    createKeyFailure.incr();
  }

  public void incListPartsSuccess() {
    listPartsSuccess.incr();
  }

  public void incListPartsFailure() {
    listPartsFailure.incr();
  }

  public void incGetKeySuccess() {
    getKeySuccess.incr();
  }

  public void incGetKeyFailure() {
    getKeyFailure.incr();
  }

  public void incHeadKeySuccess() {
    headKeySuccess.incr();
  }

  public void incHeadKeyFailure() {
    headKeyFailure.incr();
  }

  public void incAbortMultiPartUploadSuccess() {
    abortMultiPartUploadSuccess.incr();
  }

  public void incAbortMultiPartUploadFailure() {
    abortMultiPartUploadFailure.incr();
  }

  public void incDeleteKeySuccess() {
    deleteKeySuccess.incr();
  }

  public void incDeleteKeyFailure() {
    deleteKeyFailure.incr();
  }

  public void incInitMultiPartUploadSuccess() {
    initMultiPartUploadSuccess.incr();
  }

  public void incInitMultiPartUploadFailure() {
    initMultiPartUploadFailure.incr();
  }

  public void incCompleteMultiPartUploadSuccess() {
    completeMultiPartUploadSuccess.incr();
  }

  public void incCompleteMultiPartUploadFailure() {
    completeMultiPartUploadFailure.incr();
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
    return completeMultiPartUploadSuccess.value();
  }

  public long getCompleteMultiPartUploadFailure() {
    return completeMultiPartUploadFailure.value();
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
    return initMultiPartUploadSuccess.value();
  }

  public long getInitMultiPartUploadFailure() {
    return initMultiPartUploadFailure.value();
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
    return abortMultiPartUploadSuccess.value();
  }

  public long getAbortMultiPartUploadFailure() {
    return abortMultiPartUploadFailure.value();
  }

  public long getHeadKeyFailure() {
    return headKeyFailure.value();
  }

  public long getListS3BucketsFailure() {
    return listS3BucketsFailure.value();
  }

  public MutableRate getAbortMultipartUploadLatencyNs() {
    return abortMultipartUploadLatencyNs;
  }

  public MutableRate getCreateKeyLatencyNs() {
    return createKeyLatencyNs;
  }

  public MutableRate getHeadKeyLatencyNs() {
    return headKeyLatencyNs;
  }

  public MutableRate getDeleteKeyLatencyNs() {
    return deleteKeyLatencyNs;
  }

  public MutableRate getGetAclLatencyNs() {
    return getAclLatencyNs;
  }

  public MutableRate getCreateBucketLatencyNs() {
    return createBucketLatencyNs;
  }

  public MutableRate getGetBucketLatencyNs() {
    return getBucketLatencyNs;
  }

  public MutableRate getHeadBucketLatencyNs() {
    return headBucketLatencyNs;
  }

  public MutableRate getDeleteBucketLatencyNs() {
    return deleteBucketLatencyNs;
  }

  public void addInitMultipartUploadLatencyNs(long value, boolean success) {
    (success ? initMultipartUploadSuccessLatencyNs :
        initMultipartUploadFailureLatencyNs).add(value);
  }

  public void addCreateMultipartKeyLatencyNs(long value, boolean success) {
    (success ? createMultipartKeySuccessLatencyNs :
        createMultipartKeyFailureLatencyNs).add(value);
  }

  public void addCompleteMultipartUploadLatencyNs(long value, boolean success) {
    (success ? completeMultipartUploadSuccessLatencyNs :
        completeMultipartUploadFailureLatencyNs).add(value);
  }

  public void addCopyObjectLatencyNs(long value, boolean success) {
    (success ? copyObjectSuccessLatencyNs : copyObjectFailureLatencyNs).add(
        value);
  }

  public void addListPartsLatencyNs(long value, boolean success) {
    (success ? listPartsSuccessLatencyNs : listPartsFailureLatencyNs).add(
        value);
  }

  public void addGetKeyLatencyNs(long value, boolean success) {
    (success ? getKeySuccessLatencyNs : getKeyFailureLatencyNs).add(value);
  }

  public void addPutAclLatencyNs(long value, boolean success) {
    (success ? putAclSuccessLatencyNs : putAclFailureLatencyNs).add(value);
  }

  public void addListMultipartUploadsLatencyNs(long value, boolean success) {
    (success ? listMultipartUploadsSuccessLatencyNs :
        listMultipartUploadsFailureLatencyNs).add(value);
  }

  public void addListS3BucketsLatencyNs(long value, boolean success) {
    (success ? listS3BucketsSuccessLatencyNs :
        listS3BucketsFailureLatencyNs).add(value);
  }
}
