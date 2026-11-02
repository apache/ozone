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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.ozone.recon.MetricsServiceProviderFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests for {@link DataNodeMetricsCollectionTask}.
 */
class TestDataNodeMetricsCollectionTask {

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
}
