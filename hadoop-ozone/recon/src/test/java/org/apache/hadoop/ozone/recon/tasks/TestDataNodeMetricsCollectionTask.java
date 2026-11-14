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

package org.apache.hadoop.ozone.recon.tasks;

import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.defaultVersionProto;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.ozone.container.common.helpers.BlockDeletingServiceMetrics;
import org.apache.hadoop.ozone.recon.MetricsServiceProviderFactory;
import org.apache.hadoop.ozone.recon.api.types.DatanodePendingDeletionMetrics;
import org.apache.hadoop.ozone.recon.spi.MetricsServiceProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests for {@link DataNodeMetricsCollectionTask}.
 */
class TestDataNodeMetricsCollectionTask {

  private static final String BEAN_PREFIX = "Hadoop:service=HddsDatanode,name=";
  private static final String LEGACY_BEAN = BEAN_PREFIX + "BlockDeletingService";

  private final DatanodeInfo nodeDetails = new DatanodeInfo(
      MockDatanodeDetails.randomDatanodeDetails(),
      NodeStatus.inServiceHealthy(), defaultVersionProto(), 5 * 60 * 1000);

  @ParameterizedTest
  @CsvSource({
      "dn1.example.com, false, http://dn1.example.com:9882/jmx",
      "2001:db8::10, false, http://[2001:db8::10]:9882/jmx",
      "2001:db8::10, true, https://[2001:db8::10]:9883/jmx",
  })
  void testJmxMetricsUrl(String hostName, boolean httpsEnabled, String expectedUrl) {
    DatanodeInfo datanode = mock(DatanodeInfo.class);
    when(datanode.getHostName()).thenReturn(hostName);
    when(datanode.getPort(Name.HTTP)).thenReturn(DatanodeDetails.newPort(Name.HTTP, 9882));
    when(datanode.getPort(Name.HTTPS)).thenReturn(DatanodeDetails.newPort(Name.HTTPS, 9883));
    MetricsServiceProviderFactory factory = mock(MetricsServiceProviderFactory.class);

    new DataNodeMetricsCollectionTask(datanode, httpsEnabled, factory);

    verify(factory).getJmxMetricsServiceProvider(expectedUrl);
  }

  private DatanodePendingDeletionMetrics collect(List<Map<String, Object>> beans) throws Exception {
    MetricsServiceProvider provider = mock(MetricsServiceProvider.class);
    when(provider.getMetrics(LEGACY_BEAN + "*")).thenReturn(beans);
    MetricsServiceProviderFactory factory = mock(MetricsServiceProviderFactory.class);
    when(factory.getJmxMetricsServiceProvider(anyString())).thenReturn(provider);
    return new DataNodeMetricsCollectionTask(nodeDetails, false, factory).call();
  }

  private static Map<String, Object> bean(String name, long pendingBlockBytes) {
    Map<String, Object> bean = new HashMap<>();
    bean.put("name", name);
    bean.put("TotalPendingBlockBytes", pendingBlockBytes);
    return bean;
  }

  @Test
  void picksDatanodeQualifiedBeanWhenPresent() throws Exception {
    String ownBean = BEAN_PREFIX + BlockDeletingServiceMetrics.getSourceName(nodeDetails.getUuidString());
    String otherBean = BEAN_PREFIX + BlockDeletingServiceMetrics.getSourceName("other-datanode");
    List<Map<String, Object>> beans = Arrays.asList(bean(LEGACY_BEAN, 7L), bean(otherBean, 11L), bean(ownBean, 42L));

    assertEquals(42L, collect(beans).getPendingBlockSize());
  }

  @Test
  void fallsBackToLegacyBeanName() throws Exception {
    assertEquals(7L, collect(Collections.singletonList(bean(LEGACY_BEAN, 7L))).getPendingBlockSize());
  }

  @Test
  void reportsMinusOneWhenNoBeansFound() throws Exception {
    assertEquals(-1L, collect(Collections.emptyList()).getPendingBlockSize());
  }
}
