/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.insight.datanode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.insight.BaseInsightPoint;
import org.apache.hadoop.ozone.insight.Component.Type;
import org.apache.hadoop.ozone.insight.InsightPoint;
import org.apache.hadoop.ozone.insight.LoggerSource;
import org.apache.hadoop.ozone.insight.MetricGroupDisplay;

import static org.apache.hadoop.ozone.insight.datanode.PipelineComponentUtil.getPipelineIdFromFilters;
import static org.apache.hadoop.ozone.insight.datanode.PipelineComponentUtil.withDatanodesFromPipeline;

/**
 * Insight definition for datanode/pipline metrics.
 */
public class DatanodeClientInsight extends BaseInsightPoint
    implements InsightPoint {

  private OzoneConfiguration conf;

  public DatanodeClientInsight(
      OzoneConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public List<LoggerSource> getRelatedLoggers(boolean verbose,
      Map<String, String> filters) {

    List<LoggerSource> result = new ArrayList<>();

    try (ScmClient scmClient = createScmClient(conf)) {
      withDatanodesFromPipeline(scmClient,
          getPipelineIdFromFilters(filters),
          dn -> {
            result
                .add(new LoggerSource(dn,
                    HddsDispatcher.class.getCanonicalName(),
                    defaultLevel(verbose),
                    false));
            return null;
          });
    } catch (IOException e) {
      throw new UncheckedIOException("Can't enumerate required logs", e);
    }
    return result;
  }

  @Override
  public List<MetricGroupDisplay> getMetrics() {
    List<MetricGroupDisplay> metrics = new ArrayList<>();

    addProtocolMessageMetrics(metrics, "hdds_dispatcher",
        Type.SCM, ScmBlockLocationProtocolProtos.Type.values());

    return metrics;
  }

  @Override
  public String getDescription() {
    return "Datanode client protocol";
  }
}
