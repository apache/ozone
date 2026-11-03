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

package org.apache.hadoop.ozone.s3;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PROMETHEUS_ENABLED;
import static org.apache.hadoop.hdds.utils.IOUtils.closeQuietly;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3G_DEFAULT_BUCKET_LAYOUT_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTP_ADDRESS_KEY;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientMetrics;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.OmClientMetrics;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ozone.test.ClusterForTests;
import org.apache.ratis.util.function.CheckedSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/** Verifies backend client metrics through real S3 HTTP requests. */
class TestS3ClientMetrics extends ClusterForTests<MiniOzoneCluster> {
  private static final String BUCKET = "client-metrics";
  private static final String CONTENT = "OM client latency metrics";

  private final S3GatewayService s3g =
      new S3GatewayService(conf -> conf.setBoolean(HDDS_PROMETHEUS_ENABLED, true), null);
  private S3Client s3;
  private OmClientMetrics.Handle metricsHandle;

  @Override
  protected OzoneConfiguration createOzoneConfig() {
    OzoneConfiguration conf = createBaseConfiguration();
    // All services share one metrics system; let S3G own its single Prometheus sink.
    conf.setBoolean(HDDS_PROMETHEUS_ENABLED, false);
    conf.set(OZONE_S3G_DEFAULT_BUCKET_LAYOUT_KEY, BucketLayout.OBJECT_STORE.name());
    return conf;
  }

  @Override
  protected MiniOzoneCluster createCluster() throws Exception {
    return newClusterBuilder().addService(s3g).build();
  }

  @Override
  protected void onClusterReady() throws Exception {
    s3 = new S3ClientFactory(s3g.getConf()).createS3ClientV2();
    s3.createBucket(request -> request.bucket(BUCKET));
    metricsHandle = OmClientMetrics.acquireHandle(s3g.getConf());
  }

  @AfterAll
  void closeClients() {
    closeQuietly(s3, metricsHandle);
  }

  @Test
  void putGetHeadAndListRecordBackendCalls() throws Exception {
    // MiniOzoneCluster shares the metrics system with S3G. Only the S3 client operates in these measurement windows.
    XceiverClientMetrics dnMetrics = XceiverClientManager.getXceiverClientMetrics();
    long writes = count(dnMetrics, "WriteChunkLatencyNumOps");
    long commits = count(Type.CommitKey);
    s3.putObject(request -> request.bucket(BUCKET).key("object"), RequestBody.fromString(CONTENT));
    assertIncreased(Type.CommitKey, commits);
    assertThat(count(dnMetrics, "WriteChunkLatencyNumOps")).isGreaterThan(writes);

    long reads = count(dnMetrics, "ReadChunkLatencyNumOps");
    long lookups = count(Type.GetKeyInfo);
    assertThat(s3.getObjectAsBytes(request -> request.bucket(BUCKET).key("object")).asUtf8String()).isEqualTo(CONTENT);
    assertIncreased(Type.GetKeyInfo, lookups);
    // The gRPC callback can complete the response before recording its latency.
    waitFor(() -> count(dnMetrics, "ReadChunkLatencyNumOps") > reads, 10, 5_000);

    lookups = count(Type.GetKeyInfo);
    s3.headObject(request -> request.bucket(BUCKET).key("object"));
    assertIncreased(Type.GetKeyInfo, lookups);

    long lists = count(Type.ListKeysLight);
    assertThat(s3.listObjectsV2(request -> request.bucket(BUCKET)).contents()).isNotEmpty();
    assertIncreased(Type.ListKeysLight, lists);

    DefaultMetricsSystem.instance().publishMetricsNow();
    assertThat(adminResponse("/prom")).contains("om_client_metrics_commit_key_latency_ns_num_ops",
        "xceiver_client_metrics_write_chunk_latency_num_ops", "xceiver_client_metrics_read_chunk_latency_num_ops");
    // Hadoop caches JMX attributes, so newly observed command types need a cache refresh.
    waitFor((CheckedSupplier<Boolean, Exception>) () -> {
      String jmx = adminResponse("/jmx?qry=Hadoop:service=*,name=OmClientMetrics");
      return jmx.contains("CommitKeyLatencyNsNumOps") && jmx.contains("GetKeyInfoLatencyNsNumOps");
    }, 100, 30_000);
  }

  @Test
  void multipartRecordsPartAndUploadCommit() {
    String key = "multipart";
    String uploadId = s3.createMultipartUpload(request -> request.bucket(BUCKET).key(key)).uploadId();
    long partCommits = count(Type.CommitMultiPartUpload);
    UploadPartResponse part = s3.uploadPart(request -> request.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(1),
        RequestBody.fromString(CONTENT));
    assertIncreased(Type.CommitMultiPartUpload, partCommits);

    long completions = count(Type.CompleteMultiPartUpload);
    s3.completeMultipartUpload(request -> request.bucket(BUCKET).key(key).uploadId(uploadId)
        .multipartUpload(upload -> upload.parts(CompletedPart.builder().partNumber(1).eTag(part.eTag()).build())));
    assertIncreased(Type.CompleteMultiPartUpload, completions);
    assertThat(s3.getObjectAsBytes(request -> request.bucket(BUCKET).key(key)).asUtf8String()).isEqualTo(CONTENT);
  }

  @Test
  void missingKeyStillRecordsOmCall() {
    long lookups = count(Type.GetKeyInfo);
    assertThatExceptionOfType(S3Exception.class)
        .isThrownBy(() -> s3.headObject(request -> request.bucket(BUCKET).key("missing")))
        .satisfies(error -> assertThat(error.statusCode()).isEqualTo(404));
    assertIncreased(Type.GetKeyInfo, lookups);
  }

  private void assertIncreased(Type type, long before) {
    assertThat(count(type)).as("%s completed calls", type).isGreaterThan(before);
  }

  private long count(Type type) {
    return count(metricsHandle.metrics(), type + "LatencyNsNumOps");
  }

  private static long count(MetricsSource source, String name) {
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    source.getMetrics(collector, true);
    for (MetricsRecord record : collector.getRecords()) {
      for (AbstractMetric metric : record.metrics()) {
        if (metric.name().equals(name)) {
          return metric.value().longValue();
        }
      }
    }
    return 0;
  }

  private String adminResponse(String path) throws Exception {
    URL url = new URL("http://" + s3g.getConf().get(OZONE_S3G_WEBADMIN_HTTP_ADDRESS_KEY) + path);
    URLConnection connection = url.openConnection();
    connection.setConnectTimeout(5_000);
    connection.setReadTimeout(5_000);
    try (InputStream response = connection.getInputStream()) {
      return IOUtils.toString(response, StandardCharsets.UTF_8);
    }
  }
}
