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

import com.google.protobuf.Message;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerNodeReportProto;

/**
 * Factory class to construct {@link JobworkerReportPublisher} for a report.
 */
public class JobworkerReportPublisherFactory {

  private final ConfigurationSource conf;
  private final Map<Class<? extends Message>,
      Class<? extends JobworkerReportPublisher>> report2publisher;

  /**
   * Constructs {@link JobworkerReportPublisherFactory} instance.
   * @param conf Configuration to be passed to the {@link JobworkerReportPublisher}
   */
  public JobworkerReportPublisherFactory(ConfigurationSource conf) {
    this.conf = conf;
    this.report2publisher = new HashMap<>();

    // Register all report types and their publishers
    report2publisher.put(JobworkerNodeReportProto.class, JobworkerNodeReportPublisher.class);
    report2publisher.put(CommandStatusReportsProto.class, JobworkerCommandStatusReportPublisher.class);
    // Add more report types at here
  }

  /**
   * Returns the JobworkerReportPublisher for the corresponding report.
   * @param report report
   * @return report publisher
   */
  public JobworkerReportPublisher getPublisherFor(
      Class<? extends Message> report) {
    Class<? extends JobworkerReportPublisher> publisherClass =
        report2publisher.get(report);
    if (publisherClass == null) {
      throw new RuntimeException("No publisher found for report " + report);
    }
    try {
      JobworkerReportPublisher reportPublisher = publisherClass.newInstance();
      reportPublisher.setConf(conf);
      return reportPublisher;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
