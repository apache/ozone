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
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * This class maintains S3 Latency Gateway related metrics.
 */
@InterfaceAudience.Private
@Metrics(about = "S3 Gateway Latency Metrics", context = OzoneConsts.OZONE)
public final class S3GatewayLatencyMetrics implements MetricsSource {

  public static final String SOURCE_NAME =
      S3GatewayLatencyMetrics.class.getSimpleName();

  private MetricsRegistry registry;
  private static S3GatewayLatencyMetrics instance;

  // ObjectEndpoint

  @Metric(about = "Latency for initiating a multipart upload for an S3 object in nanoseconds")
  private MutableRate initMultipartUploadLatencyNs;

  @Metric(about = "Latency for creating a multipart key for an S3 object in nanoseconds")
  private MutableRate createMultipartKeyLatencyNs;

  @Metric(about = "Latency for completing a multipart upload for an S3 object in nanoseconds")
  private MutableRate completeMultipartUploadLatencyNs;

  @Metric(about = "Latency for aborting a multipart upload for an S3 object in nanoseconds")
  private MutableRate abortMultipartUploadLatencyNs;

  @Metric(about = "Latency for copying an S3 object in nanoseconds")
  private MutableRate copyObjectLatencyNs;

  @Metric(about = "Latency for listing parts of a multipart upload for an S3 object in nanoseconds")
  private MutableRate listPartsLatencyNs;

  @Metric(about = "Latency for creating a key for an S3 object in nanoseconds")
  private MutableRate createKeyLatencyNs;

  @Metric(about = "Latency for getting an S3 object in nanoseconds")
  private MutableRate getKeyLatencyNs;

  @Metric(about = "Latency for checking the existence of an S3 object in nanoseconds")
  private MutableRate headKeyLatencyNs;

  @Metric(about = "Latency for deleting an S3 object in nanoseconds")
  private MutableRate deleteKeyLatencyNs;

  // BucketEndpoint

  @Metric(about = "Latency for getting an access control list (ACL) for an S3 bucket in nanoseconds")
  private MutableRate getAclLatencyNs;

  @Metric(about = "Latency for putting an access control list (ACL) for an S3 bucket in nanoseconds")
  private MutableRate putAclLatencyNs;

  @Metric(about = "Latency for creating an S3 bucket in nanoseconds")
  private MutableRate createBucketLatencyNs;

  @Metric(about = "Latency for getting an S3 bucket in nanoseconds")
  private MutableRate getBucketLatencyNs;

  @Metric(about = "Latency for checking the existence of an S3 bucket in nanoseconds")
  private MutableRate headBucketLatencyNs;

  @Metric(about = "Latency for deleting an S3 bucket in nanoseconds")
  private MutableRate deleteBucketLatencyNs;

  @Metric(about = "Latency for listing multipart uploads for an S3 object in nanoseconds")
  private MutableRate listMultipartUploadsLatencyNs;

  // RootEndpoint

  @Metric(about = "Latency for listing S3 buckets in nanoseconds")
  private MutableRate listS3BucketsLatencyNs;

  /**
   * Private constructor.
   */
  private S3GatewayLatencyMetrics() {
    this.registry = new MetricsRegistry(SOURCE_NAME);
  }

  /**
   * Create and returns S3 Gateway Latency Metrics instance.
   *
   * @return S3GatewayLatencyMetrics
   */
  public static synchronized S3GatewayLatencyMetrics create() {
    if (instance != null) {
      return instance;
    }
    MetricsSystem ms = DefaultMetricsSystem.instance();
    instance = ms.register(SOURCE_NAME, "S3 Gateway Latency Metrics",
        new S3GatewayLatencyMetrics());
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

    initMultipartUploadLatencyNs.snapshot(recordBuilder, true);
    createMultipartKeyLatencyNs.snapshot(recordBuilder, true);
    completeMultipartUploadLatencyNs.snapshot(recordBuilder, true);
    abortMultipartUploadLatencyNs.snapshot(recordBuilder, true);
    copyObjectLatencyNs.snapshot(recordBuilder, true);
    listPartsLatencyNs.snapshot(recordBuilder, true);
    createKeyLatencyNs.snapshot(recordBuilder, true);
    getKeyLatencyNs.snapshot(recordBuilder, true);
    headKeyLatencyNs.snapshot(recordBuilder, true);
    deleteKeyLatencyNs.snapshot(recordBuilder, true);

    getAclLatencyNs.snapshot(recordBuilder, true);
    putAclLatencyNs.snapshot(recordBuilder, true);
    createBucketLatencyNs.snapshot(recordBuilder, true);
    getBucketLatencyNs.snapshot(recordBuilder, true);
    headBucketLatencyNs.snapshot(recordBuilder, true);
    deleteBucketLatencyNs.snapshot(recordBuilder, true);
    listMultipartUploadsLatencyNs.snapshot(recordBuilder, true);

    listS3BucketsLatencyNs.snapshot(recordBuilder, true);

  }

  public void addInitMultipartUploadLatencyNs(long value) {
    initMultipartUploadLatencyNs.add(value);
  }

  public void addCreateMultipartKeyLatencyNs(long value) {
    createMultipartKeyLatencyNs.add(value);
  }

  public void addCompleteMultipartUploadLatencyNs(long value) {
    completeMultipartUploadLatencyNs.add(value);
  }

  public MutableRate getAbortMultipartUploadLatencyNs() {
    return abortMultipartUploadLatencyNs;
  }

  public void addCopyObjectLatencyNs(long value) {
    copyObjectLatencyNs.add(value);
  }

  public void addListPartsLatencyNs(long value) {
    listPartsLatencyNs.add(value);
  }

  public MutableRate getCreateKeyLatencyNs() {
    return createKeyLatencyNs;
  }

  public void addGetKeyLatencyNs(long value) {
    getKeyLatencyNs.add(value);
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

  public void addPutAclLatencyNs(long value) {
    putAclLatencyNs.add(value);
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

  public void addListMultipartUploadsLatencyNs(long value) {
    listMultipartUploadsLatencyNs.add(value);
  }

  public void addListS3BucketsLatencyNs(long value) {
    listS3BucketsLatencyNs.add(value);
  }
}