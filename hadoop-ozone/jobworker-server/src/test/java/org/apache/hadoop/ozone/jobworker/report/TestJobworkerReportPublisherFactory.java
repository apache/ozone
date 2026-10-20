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

package org.apache.hadoop.ozone.jobworker.report;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerNodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test cases to test {@link JobworkerReportPublisherFactory}.
 */
public class TestJobworkerReportPublisherFactory {

  @Test
  public void testGetNodeReportPublisher() {
    OzoneConfiguration conf = new OzoneConfiguration();
    JobworkerReportPublisherFactory factory = new JobworkerReportPublisherFactory(conf);
    JobworkerReportPublisher publisher = factory
        .getPublisherFor(JobworkerNodeReportProto.class);

    Assertions.assertEquals(JobworkerNodeReportPublisher.class, publisher.getClass());
    Assertions.assertEquals(conf, publisher.getConf());
  }

  @Test
  public void testInvalidReportPublisher() {
    OzoneConfiguration conf = new OzoneConfiguration();
    JobworkerReportPublisherFactory factory = new JobworkerReportPublisherFactory(conf);

    RuntimeException exception = Assertions.assertThrows(
        RuntimeException.class,
        () -> factory.getPublisherFor(HddsProtos.DatanodeDetailsProto.class)
    );

    Assertions.assertTrue(exception.getMessage().contains("No publisher found for report"));
  }
}
