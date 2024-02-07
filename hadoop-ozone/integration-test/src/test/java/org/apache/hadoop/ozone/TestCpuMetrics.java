/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test /prom http endpoint to test availability of the next metrics.
 *  <p>jvm_metrics_cpu_available_processors</p>
 *  <p>jvm_metrics_cpu_system_load</p>
 *  <p>jvm_metrics_cpu_jvm_load</p>
 */
public class TestCpuMetrics {

  private static MiniOzoneCluster cluster;
  private final OkHttpClient httpClient = new OkHttpClient();

  @BeforeAll
  public static void setup() throws InterruptedException, TimeoutException,
      IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();
  }

  @Test
  public void testCpuMetrics() throws IOException {
    // given
    String scmHttpServerUrl = "http://localhost:" +
        HddsUtils.getPortNumberFromConfigKeys(cluster.getConf(),
                OZONE_SCM_HTTP_ADDRESS_KEY).getAsInt();
    Request prometheusMetricsRequest = new Request.Builder()
        .url(scmHttpServerUrl + "/prom")
        .build();

    // when
    Response metricsResponse = httpClient.newCall(prometheusMetricsRequest)
        .execute();
    String metricsResponseBodyContent = metricsResponse.body().string();

    // then
    assertThat(metricsResponseBodyContent)
        .contains("jvm_metrics_cpu_available_processors");
    assertThat(metricsResponseBodyContent)
        .contains("jvm_metrics_cpu_system_load");
    assertThat(metricsResponseBodyContent)
        .contains("jvm_metrics_cpu_jvm_load");
  }

}
