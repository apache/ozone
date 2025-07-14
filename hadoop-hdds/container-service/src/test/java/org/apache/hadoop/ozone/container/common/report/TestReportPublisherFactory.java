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

package org.apache.hadoop.ozone.container.common.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.junit.jupiter.api.Test;

/**
 * Test cases to test ReportPublisherFactory.
 */
public class TestReportPublisherFactory {

  @Test
  public void testGetContainerReportPublisher() {
    OzoneConfiguration conf = new OzoneConfiguration();
    ReportPublisherFactory factory = new ReportPublisherFactory(conf);
    ReportPublisher publisher = factory
        .getPublisherFor(ContainerReportsProto.class);
    assertEquals(
        ContainerReportPublisher.class, publisher.getClass());
    assertEquals(conf, publisher.getConf());
  }

  @Test
  public void testGetNodeReportPublisher() {
    OzoneConfiguration conf = new OzoneConfiguration();
    ReportPublisherFactory factory = new ReportPublisherFactory(conf);
    ReportPublisher publisher = factory
        .getPublisherFor(NodeReportProto.class);
    assertEquals(NodeReportPublisher.class, publisher.getClass());
    assertEquals(conf, publisher.getConf());
  }

  @Test
  public void testInvalidReportPublisher() {
    OzoneConfiguration conf = new OzoneConfiguration();
    ReportPublisherFactory factory = new ReportPublisherFactory(conf);
    RuntimeException runtimeException = assertThrows(
        RuntimeException.class,
        () -> factory.getPublisherFor(HddsProtos.DatanodeDetailsProto.class)
    );
    assertThat(runtimeException.getMessage())
        .contains("No publisher found for report");
  }
}
